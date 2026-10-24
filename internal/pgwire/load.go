package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// COPY INTO loads files from object storage into a table of the Iceberg
// catalog, as Snowflake's does:
//
//	COPY INTO table [(column, ...)] FROM 'location' [(option [value], ...)]
//
// The location is a prefix (ending in /), a glob or one file, in the
// warehouse or IGLOO_ALLOWED_LOCATIONS. Igloo lists the files there, leaves
// out those the load history says are in the table already, and inserts
// the rest in one transaction, one INSERT per file, each prepared and
// checked as the user's own statement: so writing needs INSERT on the table
// (the writer role or a grant) and reading files the reader role, exactly
// as INSERT ... SELECT FROM read_csv(...) would.

// Load options. Values are words, numbers or 'strings'; a flag alone means
// true.
const (
	formatAuto     = ""
	onErrorAbort   = "abort"
	onErrorSkip    = "skip_file"
	onErrorRows    = "continue"
	matchByName    = "name"
	matchByPos     = "position"
	compressAuto   = "auto"
	maxErrorLength = 2000
)

var loadFormats = []string{"csv", "tsv", "parquet", "json", "ndjson"}

// copyCommand is a parsed COPY INTO.
type copyCommand struct {
	target      []string
	columns     []string
	location    string
	format      string
	header      bool
	delimiter   string
	compression string
	pattern     string
	matchBy     string
	onError     string
	force       bool
	createTable bool
}

// parseCopyInto recognizes COPY INTO. ok is false for any other statement;
// err reports a malformed one.
func parseCopyInto(query string) (c copyCommand, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("COPY", "INTO") {
		return c, false, nil
	}
	syntax := func(format string, args ...any) (copyCommand, bool, error) {
		return c, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	if c.target, ok = sc.name(); !ok {
		return syntax("COPY INTO needs a table name")
	}
	if sc.punct('(') {
		for {
			col, ok := sc.name()
			if !ok || len(col) != 1 {
				return syntax("expected a column name in COPY INTO's column list")
			}
			c.columns = append(c.columns, col[0])
			if sc.punct(')') {
				break
			}
			if !sc.punct(',') {
				return syntax("expected , or ) in COPY INTO's column list")
			}
		}
	}
	if !sc.keywords("FROM") {
		return syntax("expected FROM 'location' after COPY INTO %s", strings.Join(c.target, "."))
	}
	loc, ok := sc.literal()
	if !ok || loc == nil || *loc == "" {
		return syntax("COPY INTO takes its location as a string, such as FROM 's3://bucket/path/'")
	}
	c.location = *loc
	c.header, c.compression = true, compressAuto
	if sc.punct('(') && !sc.punct(')') {
		for {
			key := strings.ToLower(sc.word())
			if key == "" {
				return syntax("expected a COPY INTO option")
			}
			sc.i += len(key)
			sc.punct('=')
			value, err := optionValue(sc)
			if err != nil {
				return c, true, err
			}
			if err := c.setOption(key, value); err != nil {
				return c, true, err
			}
			if sc.punct(')') {
				break
			}
			if !sc.punct(',') {
				return syntax("expected , or ) after COPY INTO option %s", strings.ToUpper(key))
			}
		}
	}
	if !sc.done() {
		return syntax("unexpected text after COPY INTO's options: %q", strings.TrimSpace(sc.rest()))
	}
	if c.pattern != "" {
		if _, err := regexp.Compile(c.pattern); err != nil {
			return c, true, pgerror.WithCode(fmt.Errorf("PATTERN: %w", err), codes.InvalidParameterValue)
		}
	}
	return c, true, nil
}

// optionValue reads an option's value: a 'string', a word or number, or
// nothing (the option is a flag, true).
func optionValue(sc *scanner) (string, error) {
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '\'' {
		v, ok := sc.literal()
		if !ok || v == nil {
			return "", pgerror.WithCode(errors.New("malformed string in COPY INTO's options"), codes.Syntax)
		}
		return *v, nil
	}
	if w := sc.word(); w != "" {
		sc.i += len(w)
		return strings.ToLower(w), nil
	}
	return "true", nil
}

func (c *copyCommand) setOption(key, value string) error {
	invalid := func(allowed string) error {
		return pgerror.WithCode(fmt.Errorf("invalid %s %q; use %s", strings.ToUpper(key), value, allowed), codes.InvalidParameterValue)
	}
	boolean := func(dst *bool) error {
		switch value {
		case "true", "on", "1", "yes":
			*dst = true
		case "false", "off", "0", "no":
			*dst = false
		default:
			return invalid("true or false")
		}
		return nil
	}
	switch key {
	case "format", "type":
		if c.format = strings.ToLower(value); !slices.Contains(loadFormats, c.format) {
			return invalid(strings.Join(loadFormats, ", "))
		}
	case "header":
		return boolean(&c.header)
	case "delimiter", "delim", "sep":
		if len(value) != 1 && value != `\t` {
			return invalid("one character")
		}
		c.delimiter = value
	case "compression":
		switch value {
		case "auto", "gzip", "zstd", "none":
			c.compression = value
		default:
			return invalid("auto, gzip, zstd or none")
		}
	case "pattern":
		c.pattern = value
	case "match_by":
		if value != matchByName && value != matchByPos {
			return invalid("name or position")
		}
		c.matchBy = value
	case "on_error":
		switch value {
		case onErrorAbort, "abort_statement":
			c.onError = onErrorAbort
		case onErrorSkip, onErrorRows:
			c.onError = value
		default:
			return invalid("abort, skip_file or continue")
		}
	case "force":
		return boolean(&c.force)
	case "create_table":
		return boolean(&c.createTable)
	default:
		return pgerror.WithCode(fmt.Errorf("unknown COPY INTO option %s; options are FORMAT, HEADER, DELIMITER, "+
			"COMPRESSION, PATTERN, MATCH_BY, ON_ERROR, FORCE and CREATE_TABLE", strings.ToUpper(key)), codes.Syntax)
	}
	return nil
}

// String writes c back as SQL, with its target as given: pipes keep their
// COPY INTO this way, with the target resolved.
func (c copyCommand) String() string {
	var b strings.Builder
	b.WriteString("COPY INTO " + quoteName(c.target))
	if len(c.columns) > 0 {
		cols := make([]string, len(c.columns))
		for i, col := range c.columns {
			cols[i] = quoteIdent(col)
		}
		b.WriteString(" (" + strings.Join(cols, ", ") + ")")
	}
	b.WriteString(" FROM " + quoteLiteral(c.location))
	var opts []string
	if c.format != formatAuto {
		opts = append(opts, "FORMAT "+c.format)
	}
	if !c.header {
		opts = append(opts, "HEADER false")
	}
	if c.delimiter != "" {
		opts = append(opts, "DELIMITER "+quoteLiteral(c.delimiter))
	}
	if c.compression != compressAuto {
		opts = append(opts, "COMPRESSION "+c.compression)
	}
	if c.pattern != "" {
		opts = append(opts, "PATTERN "+quoteLiteral(c.pattern))
	}
	if c.matchBy != "" {
		opts = append(opts, "MATCH_BY "+c.matchBy)
	}
	if c.onError != "" {
		opts = append(opts, "ON_ERROR "+c.onError)
	}
	if c.force {
		opts = append(opts, "FORCE true")
	}
	if c.createTable {
		opts = append(opts, "CREATE_TABLE true")
	}
	if len(opts) > 0 {
		b.WriteString(" (" + strings.Join(opts, ", ") + ")")
	}
	return b.String()
}

// punct consumes c if the statement continues with it.
func (sc *scanner) punct(c byte) bool {
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == c {
		sc.i++
		return true
	}
	return false
}

// copyColumns describe what COPY INTO returns: one row per file it tried.
var copyColumns = wire.Columns{
	{Name: "file", Oid: pgtype.TextOID, Width: -1},
	{Name: "status", Oid: pgtype.TextOID, Width: -1},
	{Name: "rows_loaded", Oid: pgtype.Int8OID, Width: 8},
	{Name: "errors", Oid: pgtype.Int8OID, Width: 8},
	{Name: "first_error", Oid: pgtype.TextOID, Width: -1},
}

// copyIntoStatement carries out COPY INTO for a client.
func (s *Server) copyIntoStatement(sess *session, query string, c copyCommand) wire.PreparedStatements {
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		user := sess.currentUser()
		var (
			loads  []tasks.Load
			target auth.Object
		)
		err := s.inNoTransaction(sess, "COPY INTO")
		if err == nil {
			// COPY INTO creates its target table when missing, resolves it
			// and commits new snapshots, all of which reach the catalog's
			// in-memory set; hold the exclusive side so nothing else reaches
			// the set on this instance while it does (see holdCatalog).
			releaseCatalog := s.holdCatalog(sess)
			err = sess.runCancelable(ctx, query, func(ctx context.Context) error {
				var err error
				loads, target, err = s.copyInto(ctx, sess, user, c, "")
				return err
			})
			releaseCatalog()
		}
		var total int64
		if err == nil {
			for _, l := range loads {
				total += l.Rows
				var first any
				if l.FirstError != "" {
					first = l.FirstError
				}
				if err = w.Row([]any{l.File, l.Status, l.Rows, l.Errors, first}); err != nil {
					break
				}
			}
		}
		if err == nil {
			err = w.Complete(fmt.Sprintf("COPY %d", total))
		}
		s.metrics.query(start, err)
		rec := queryRecord{text: query, start: start, err: err, rows: total}
		if target.Table != "" {
			rec.written, rec.locations = []auth.Object{target}, []string{c.location}
		}
		s.recordQuery(sess, rec)
		return pgError(err)
	}, wire.WithColumns(copyColumns)))
}

// inNoTransaction refuses a command that Igloo commits itself while the
// session has a transaction open.
func (s *Server) inNoTransaction(sess *session, command string) error {
	if sess.inTx {
		return pgerror.WithCode(fmt.Errorf("%s cannot run inside a transaction block", command), codes.ActiveSQLTransaction)
	}
	return nil
}

// loadFile is a file COPY INTO loads, and how.
type loadFile struct {
	tasks.Load
	format      string
	compression string
	insert      string
	// count, for ON_ERROR continue, counts the rows that will be skipped,
	// and rejects names the table where the CSV reader puts lines it
	// cannot parse.
	count   string
	rejects string
}

func (f *loadFile) fail(err error) {
	f.Status, f.Rows, f.FirstError = tasks.LoadFailed, 0, errorText(err)
}

// copyInto carries out c as user on sess's connection, for pipe if it is
// a pipe's. It returns what happened to each file it tried to load, and
// the table it loaded into.
func (s *Server) copyInto(ctx context.Context, sess *session, user auth.User, c copyCommand, pipe string) ([]tasks.Load, auth.Object, error) {
	if user.Role == auth.RoleNone {
		return nil, auth.Object{}, denied("COPY INTO reads files, which needs the reader role; users with the none role may read only the tables granted to them")
	}
	if err := s.checkLocation(c.location); err != nil {
		return nil, auth.Object{}, err
	}
	if err := s.checkLoadLocation(ctx, user, c.location); err != nil {
		return nil, auth.Object{}, err
	}
	target, exists, err := s.resolveLoadTarget(ctx, sess, c.target)
	if err != nil {
		return nil, target, err
	}
	if !exists && !c.createTable {
		return nil, target, pgerror.WithCode(fmt.Errorf("table %s does not exist; create it first, or add CREATE_TABLE true to create it from the files",
			strings.Join(c.target, ".")), codes.UndefinedTable)
	}
	table := s.catalogName() + "." + target.String()
	qualified := quoteName([]string{s.catalogName(), target.Schema, target.Table})
	if err := s.authorizeLoad(ctx, sess, user, qualified, exists); err != nil {
		return nil, target, err
	}
	var columns []column
	if exists {
		if columns, err = targetColumns(ctx, sess, c, qualified); err != nil {
			return nil, target, err
		}
	}

	files, err := listFiles(ctx, sess.conn, c)
	if err != nil {
		return nil, target, err
	}
	if len(files) == 0 {
		return nil, target, nil
	}

	unlock, err := s.tasks.Store().Lock(ctx, tasks.TableKey(table))
	if err != nil {
		return nil, target, fmt.Errorf("wait for other loads into %s: %w", table, err)
	}
	defer unlock()
	todo, err := s.newFiles(ctx, table, files, c.force, pipe != "")
	if err != nil || len(todo) == 0 {
		return nil, target, err
	}

	onError := c.onError
	if onError == "" {
		// As in Snowflake: one bad file stops a COPY INTO, but not a pipe.
		onError = onErrorAbort
		if pipe != "" {
			onError = onErrorSkip
		}
	}
	now := time.Now()
	for _, f := range todo {
		f.ID, f.Table, f.Start, f.Status, f.User, f.Pipe = audit.NewID(now), table, now, tasks.LoadLoading, user.Name, pipe
	}

	if !exists {
		if err := s.createLoadTable(ctx, sess, user, c, qualified, todo); err != nil {
			return nil, target, err
		}
		if columns, err = targetColumns(ctx, sess, c, qualified); err != nil {
			return nil, target, err
		}
	}

	store := s.tasks.Store()
	loadErr := s.planLoads(ctx, sess, user, c, onError, qualified, columns, todo)
	if loadErr == nil {
		// Record the files as loading first: if Igloo stops before their
		// outcome is recorded, the next load finds them and leaves them
		// alone rather than perhaps loading them twice.
		var claims []tasks.Load
		for _, f := range todo {
			if f.Status == tasks.LoadLoading {
				claims = append(claims, f.Load)
			}
		}
		if len(claims) > 0 {
			if err := store.WriteLoads(ctx, claims); err != nil {
				return nil, target, fmt.Errorf("record the load history: %w", err)
			}
		}
		loadErr = s.loadFiles(ctx, sess, user, todo, onError)
		if loadErr == nil {
			// Then the ids of the values that had none, in a member table.
			var rows int64
			for _, f := range todo {
				rows += f.Rows
			}
			s.fillIDs(ctx, sess, s.loadIDs(ctx, target), rows)
		}
	}
	finish, unloaded := finished(todo, loadErr)
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if len(unloaded) > 0 {
		if err := store.DeleteLoads(wctx, unloaded); err != nil {
			s.log.Warn("could not update the load history", "table", table, "err", err)
		}
	}
	if len(finish) == 0 {
		return nil, target, loadErr
	}
	if err := store.WriteLoads(wctx, finish); err != nil {
		s.log.Warn("could not update the load history", "table", table, "err", err)
		if loadErr == nil {
			loadErr = fmt.Errorf("the files were loaded, but the load history could not be updated; "+
				"they stay marked as loading and will not be loaded again: %w", err)
		}
	}
	s.notifyLoadErrors(table, user.Name, pipe, finish)
	return finish, target, loadErr
}

// finished settles the outcome of each file once the load has ended with
// err. With ON_ERROR abort a failure leaves nothing loaded: the file that
// failed is recorded, and the others are forgotten (unloaded).
func finished(todo []*loadFile, err error) (done []tasks.Load, unloaded []string) {
	end := time.Now()
	for _, f := range todo {
		f.End = end
		switch {
		case f.Status == tasks.LoadFailed:
		case err != nil:
			unloaded = append(unloaded, f.ID)
			continue
		case f.Errors > 0:
			f.Status = tasks.LoadPartial
		default:
			f.Status = tasks.LoadLoaded
		}
		done = append(done, f.Load)
	}
	return done, unloaded
}

// checkLocation refuses locations outside the warehouse and
// IGLOO_ALLOWED_LOCATIONS. DuckDB's sandbox would refuse them too; this
// says why, and keeps COPY INTO off the server's own directories, which the
// sandbox allows for spill files and the read cache.
func (s *Server) checkLocation(loc string) error {
	allowed := s.locations()
	if !slices.Contains(strings.Split(loc, "/"), "..") {
		for _, prefix := range allowed {
			if strings.HasPrefix(loc, prefix) {
				return nil
			}
		}
	}
	msg := "permission denied: COPY INTO loads only from the warehouse and IGLOO_ALLOWED_LOCATIONS"
	if len(allowed) > 0 {
		msg += " (" + strings.Join(allowed, ", ") + ")"
	}
	return pgerror.WithCode(fmt.Errorf("%s, not %s", msg, loc), codes.InsufficientPrivilege)
}

// authorizeLoad checks, before anything is listed or read, that user may
// load into the table qualified: INSERT on it, or, if COPY INTO is to
// create it, CREATE TABLE AS. It prepares the statements to have them
// checked, and never runs them. The INSERT of each file is checked again.
func (s *Server) authorizeLoad(ctx context.Context, sess *session, user auth.User, qualified string, exists bool) error {
	query := "INSERT INTO " + qualified + " SELECT * FROM " + qualified + " LIMIT 0"
	if !exists {
		query = "CREATE TABLE " + qualified + " AS SELECT 1 AS loaded"
	}
	st, err := s.prepareChecked(ctx, sess, user, query, nil)
	if err != nil {
		return err
	}
	return st.close()
}

// locations are the URL prefixes COPY INTO may load from.
func (s *Server) locations() []string {
	if l, ok := s.engine.(interface{ Locations() []string }); ok {
		return l.Locations()
	}
	return nil
}

// resolveLoadTarget works out which table of the catalog name means, as
// DuckDB would for an INSERT. exists is false when there is none yet; the
// object is then where CREATE TABLE would put it.
func (s *Server) resolveLoadTarget(ctx context.Context, sess *session, name []string) (auth.Object, bool, error) {
	notInCatalog := pgerror.WithCode(fmt.Errorf("COPY INTO loads into tables of the %s catalog; %s is not one",
		s.catalogName(), strings.Join(name, ".")), codes.FeatureNotSupported)
	if s.catalogName() == "" {
		return auth.Object{}, false, pgerror.WithCode(errors.New("COPY INTO needs an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return auth.Object{}, false, err
	}
	res, err := r.resolve(tableRef{parts: name}, nil)
	if err != nil {
		return auth.Object{}, false, err
	}
	switch {
	case len(res.first) > 1:
		return auth.Object{}, false, pgerror.WithCode(fmt.Errorf("%s is ambiguous; qualify it", strings.Join(name, ".")), codes.AmbiguousAlias)
	case len(res.first) == 1:
		o := res.first[0]
		if !strings.EqualFold(o.db, s.catalogName()) || o.view {
			return auth.Object{}, false, notInCatalog
		}
		return auth.Object{Schema: o.schema, Table: o.name}, true, nil
	}
	o, err := s.newTableTarget(ctx, sess, name)
	if errors.Is(err, errNoGrantPath) {
		err = notInCatalog
	}
	return o, false, err
}

// listFiles lists the files at c's location that match its pattern, with
// their sizes and times, which tell versions of a file apart.
func listFiles(ctx context.Context, conn *duckdb.Conn, c copyCommand) ([]*loadFile, error) {
	glob, dir := c.location, c.location[:strings.LastIndexByte(c.location, '/')+1]
	switch i := strings.IndexAny(c.location, "*?[{"); {
	case i >= 0:
		dir = c.location[:strings.LastIndexByte(c.location[:i], '/')+1]
	case strings.HasSuffix(c.location, "/"):
		glob = c.location + "**"
	}
	var pattern *regexp.Regexp
	if c.pattern != "" {
		pattern = regexp.MustCompile("^(?:" + c.pattern + ")$")
	}
	rows, err := queryRows(ctx, conn, "SELECT filename, size, last_modified FROM read_blob("+quoteLiteral(glob)+") ORDER BY filename")
	if err != nil {
		if strings.Contains(err.Error(), "No files found") {
			return nil, nil
		}
		return nil, pgError(err)
	}
	var files []*loadFile
	for _, row := range rows {
		name, _ := row[0].(string)
		if name == "" || strings.HasSuffix(name, "/") {
			continue
		}
		if pattern != nil && !pattern.MatchString(strings.TrimPrefix(name, dir)) {
			continue
		}
		f := &loadFile{}
		f.File = name
		f.Size, _ = row[1].(int64)
		if t, ok := row[2].(time.Time); ok {
			f.Modified = tasks.Truncate(t)
		}
		files = append(files, f)
	}
	return files, nil
}

// newFiles leaves out the files the load history says need no loading:
// those loaded already, and those a load was cut short on, unless force.
// A file that changed since (another size or time) is new. Pipes also
// leave out files that failed, until they change.
func (s *Server) newFiles(ctx context.Context, table string, files []*loadFile, force, pipe bool) ([]*loadFile, error) {
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = f.File
	}
	store := s.tasks.Store()
	previous, err := store.PreviousLoads(ctx, table, names)
	if err != nil {
		return nil, fmt.Errorf("read the load history: %w", err)
	}
	var todo []*loadFile
	for _, f := range files {
		prev, seen := previous[f.File]
		if force || !seen || !prev.SameVersion(f.Size, f.Modified) {
			todo = append(todo, f)
			continue
		}
		switch prev.Status {
		case tasks.LoadLoading:
			// Nobody else loads this table now, so this load was cut
			// short: the file may or may not be in the table.
			prev.Status, prev.End = tasks.LoadUnknown, time.Now()
			prev.FirstError = "Igloo stopped while loading this file, so it may or may not be in the table; " +
				"check, and load it with FORCE true if it is not"
			if err := store.WriteLoads(ctx, []tasks.Load{prev}); err != nil {
				return nil, err
			}
		case tasks.LoadFailed:
			if !pipe {
				todo = append(todo, f)
			}
		}
	}
	return todo, nil
}

// createLoadTable creates the target from the first file's columns, as
// CREATE TABLE AS would, so it needs the same privileges.
func (s *Server) createLoadTable(ctx context.Context, sess *session, user auth.User, c copyCommand, qualified string, files []*loadFile) error {
	f := files[0]
	format, compression, _, err := detectFormat(ctx, sess.conn, c, f.File)
	if err != nil {
		return pgerror.WithCode(fmt.Errorf("cannot create the table from %s: %w", f.File, err), codes.InvalidParameterValue)
	}
	return s.execDDL(ctx, sess, user, "CREATE TABLE IF NOT EXISTS "+qualified+" AS SELECT * FROM "+
		reader(c, f.File, format, compression, "", false)+" LIMIT 0")
}

// column is a column of a table or file.
type column struct {
	name, typ string
	notNull   bool
}

// targetColumns are the columns of the table, qualified, that COPY INTO
// loads: those in c's column list, or all of them.
func targetColumns(ctx context.Context, sess *session, c copyCommand, qualified string) ([]column, error) {
	all, err := describeColumns(ctx, sess.conn, "DESCRIBE "+qualified)
	if err != nil || len(c.columns) == 0 {
		return all, pgError(err)
	}
	var picked []column
	for _, name := range c.columns {
		i := slices.IndexFunc(all, func(t column) bool { return strings.EqualFold(t.name, name) })
		if i < 0 {
			return nil, pgerror.WithCode(fmt.Errorf("column %q of %s does not exist", name, strings.Join(c.target, ".")), codes.UndefinedColumn)
		}
		picked = append(picked, all[i])
	}
	return picked, nil
}

// describeColumns runs a DESCRIBE and returns the columns it lists.
func describeColumns(ctx context.Context, conn *duckdb.Conn, query string) ([]column, error) {
	rows, err := queryRows(ctx, conn, query)
	if err != nil {
		return nil, err
	}
	cols := make([]column, len(rows))
	for i, row := range rows {
		cols[i].name, _ = row[0].(string)
		cols[i].typ, _ = row[1].(string)
		if len(row) > 2 {
			null, _ := row[2].(string)
			cols[i].notNull = null == "NO"
		}
	}
	return cols, nil
}

// planLoads works out how to load each file into the table: its format,
// which of its columns go where, and the INSERT that loads it. A file
// that cannot be read at all fails here (or stops the load, with ON_ERROR
// abort).
func (s *Server) planLoads(ctx context.Context, sess *session, user auth.User, c copyCommand, onError, qualified string,
	target []column, files []*loadFile) error {
	for n, f := range files {
		err := s.planLoad(ctx, sess, user, c, onError, qualified, target, f, n)
		if err == nil {
			continue
		}
		if fatalLoadError(ctx, err) {
			return pgError(err)
		}
		f.fail(err)
		if onError == onErrorAbort {
			return fmt.Errorf("COPY INTO stopped at %s, and loaded nothing: %w", f.File, pgError(err))
		}
	}
	return nil
}

func (s *Server) planLoad(ctx context.Context, sess *session, user auth.User, c copyCommand, onError, qualified string,
	target []column, f *loadFile, n int) error {
	var (
		fileCols []column
		err      error
	)
	if f.format, f.compression, fileCols, err = detectFormat(ctx, sess.conn, c, f.File); err != nil {
		return err
	}
	matchBy := c.matchBy
	if matchBy == "" {
		matchBy = matchByName
		if (f.format == "csv" || f.format == "tsv") && !c.header {
			matchBy = matchByPos
		}
	}
	continueOnRows := onError == onErrorRows
	// bad are the conditions under which a row is skipped, and why the
	// messages for each.
	var names, exprs, bad, why []string
	add := func(t, src column) {
		ref := "src." + quoteIdent(src.name)
		cast := "CAST(" + ref + " AS " + t.typ + ")"
		if continueOnRows {
			cast = "TRY_" + cast
			cond := "(" + ref + " IS NOT NULL AND " + cast + " IS NULL)"
			bad = append(bad, cond)
			why = append(why, "CASE WHEN "+cond+" THEN "+quoteLiteral("could not convert ")+" || CAST("+ref+" AS VARCHAR) || "+
				quoteLiteral(" to "+t.typ+" for column "+t.name)+" END")
			if t.notNull {
				bad = append(bad, cast+" IS NULL")
				why = append(why, "CASE WHEN "+cast+" IS NULL THEN "+quoteLiteral("column "+t.name+" is NOT NULL")+" END")
			}
		}
		names, exprs = append(names, quoteIdent(t.name)), append(exprs, cast)
	}
	if matchBy == matchByPos {
		if len(fileCols) != len(target) {
			return fmt.Errorf("the file has %d columns and the table %d; with MATCH_BY position they must agree", len(fileCols), len(target))
		}
		for i, t := range target {
			add(t, fileCols[i])
		}
	} else {
		for _, t := range target {
			if i := slices.IndexFunc(fileCols, func(fc column) bool { return strings.EqualFold(fc.name, t.name) }); i >= 0 {
				add(t, fileCols[i])
			}
		}
		if len(names) == 0 {
			fileNames := make([]string, len(fileCols))
			for i, fc := range fileCols {
				fileNames[i] = fc.name
			}
			return fmt.Errorf("none of the file's columns (%s) is a column of the table; check HEADER, or use MATCH_BY position",
				strings.Join(fileNames, ", "))
		}
	}

	from := " FROM " + reader(c, f.File, f.format, f.compression, "", continueOnRows) + " AS src"
	f.insert = "INSERT INTO " + qualified + " (" + strings.Join(names, ", ") + ") SELECT " + strings.Join(exprs, ", ") + from
	if !continueOnRows {
		return nil
	}
	condition := "false"
	if len(bad) > 0 {
		condition = strings.Join(bad, " OR ")
		f.insert += " WHERE NOT (" + condition + ")"
	}
	if f.format == "csv" || f.format == "tsv" {
		f.rejects = fmt.Sprintf("igloo_rejects_%d_%d", rand.Uint32(), n)
	}
	reason := "NULL"
	if len(why) > 0 {
		reason = "any_value(concat_ws('; ', " + strings.Join(why, ", ") + ")) FILTER (WHERE " + condition + ")"
	}
	f.count = "SELECT count(*) FILTER (WHERE " + condition + "), " + reason + " FROM " +
		reader(c, f.File, f.format, f.compression, f.rejects, true) + " AS src"
	return s.countSkipped(ctx, sess, user, f)
}

// countSkipped counts the rows of f that ON_ERROR continue skips: those
// the CSV reader cannot parse, and those whose values do not convert to
// the table's types.
func (s *Server) countSkipped(ctx context.Context, sess *session, user auth.User, f *loadFile) error {
	st, err := s.prepareChecked(ctx, sess, user, f.count, nil)
	if err != nil {
		return err
	}
	rows, err := st.stmt.QueryContext(ctx, nil)
	if err != nil {
		_ = st.close()
		return pgError(err)
	}
	dest := make([]driver.Value, 2)
	err = rows.Next(dest)
	_ = rows.Close()
	_ = st.close()
	if err != nil {
		return pgError(err)
	}
	f.Errors, _ = dest[0].(int64)
	if msg, _ := dest[1].(string); msg != "" {
		f.FirstError = errorText(errors.New(msg))
	}
	if f.rejects == "" {
		return nil
	}
	defer func() {
		for _, t := range []string{f.rejects, f.rejects + "_scan"} {
			_, _ = sess.conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS temp."+quoteIdent(t), nil)
		}
	}()
	rejected, err := queryRows(ctx, sess.conn, "SELECT count(*), arg_min(error_message, line) FROM (SELECT DISTINCT ON (file_id, line) "+
		"file_id, line, error_message FROM temp."+quoteIdent(f.rejects)+" ORDER BY file_id, line, column_idx)")
	if err != nil {
		return pgError(err)
	}
	if len(rejected) == 1 {
		n, _ := rejected[0][0].(int64)
		f.Errors += n
		if msg, _ := rejected[0][1].(string); msg != "" {
			f.FirstError = errorText(errors.New(msg))
		}
	}
	return nil
}

// detectFormat returns the format and compression to read file with, and
// the file's columns. They are as c says, or else from the file's name. A
// CSV or JSON file whose name does not say it is compressed is tried as
// gzip and zstd too, if it does not read as text: drops are often
// compressed without saying so.
func detectFormat(ctx context.Context, conn *duckdb.Conn, c copyCommand, file string) (format, compression string, cols []column, err error) {
	format, compression = c.format, c.compression
	name := strings.ToLower(file)
	for ext, comp := range map[string]string{".gz": "gzip", ".gzip": "gzip", ".zst": "zstd", ".zstd": "zstd"} {
		if strings.HasSuffix(name, ext) {
			name = strings.TrimSuffix(name, ext)
			if compression == compressAuto {
				compression = comp
			}
		}
	}
	if format == formatAuto {
		switch {
		case strings.HasSuffix(name, ".csv"):
			format = "csv"
		case strings.HasSuffix(name, ".tsv"), strings.HasSuffix(name, ".tab"):
			format = "tsv"
		case strings.HasSuffix(name, ".parquet"), strings.HasSuffix(name, ".pq"):
			format = "parquet"
		case strings.HasSuffix(name, ".json"):
			format = "json"
		case strings.HasSuffix(name, ".ndjson"), strings.HasSuffix(name, ".jsonl"):
			format = "ndjson"
		default:
			return "", "", nil, fmt.Errorf("cannot tell the format of %s from its name; give COPY INTO the FORMAT option", file)
		}
	}
	tries := []string{compression}
	if format != "parquet" && compression == compressAuto {
		// Text first, then compressed.
		tries = []string{"none", "gzip", "zstd"}
	}
	var first error
	for _, comp := range tries {
		cols, err := describeColumns(ctx, conn, "DESCRIBE SELECT * FROM "+reader(c, file, format, comp, "", false))
		if err == nil {
			return format, comp, cols, nil
		}
		if first == nil {
			first = err
		}
		if ctx.Err() != nil {
			break
		}
	}
	return "", "", nil, pgError(first)
}

// reader is the table function that reads file. ignoreErrors skips what
// cannot be read; rejects, for CSV, names the table that counts it.
func reader(c copyCommand, file, format, compression, rejects string, ignoreErrors bool) string {
	args := []string{quoteLiteral(file)}
	switch format {
	case "parquet":
		return "read_parquet(" + args[0] + ")"
	case "json", "ndjson":
		jsonFormat := "auto"
		if format == "ndjson" {
			jsonFormat = "newline_delimited"
		}
		comp := map[string]string{compressAuto: "auto_detect", "none": "uncompressed"}[compression]
		if comp == "" {
			comp = compression
		}
		args = append(args, "format = "+quoteLiteral(jsonFormat), "compression = "+quoteLiteral(comp))
		if ignoreErrors && format == "ndjson" {
			args = append(args, "ignore_errors = true")
		}
		return "read_json(" + strings.Join(args, ", ") + ")"
	}
	delimiter := c.delimiter
	if format == "tsv" && delimiter == "" {
		delimiter = `\t`
	}
	args = append(args, fmt.Sprintf("header = %t", c.header), "compression = "+quoteLiteral(compression))
	if delimiter != "" {
		args = append(args, "delim = "+quoteLiteral(delimiter))
	}
	if ignoreErrors {
		args = append(args, "ignore_errors = true")
	}
	if rejects != "" {
		args = append(args, "store_rejects = true", "rejects_table = "+quoteLiteral(rejects),
			"rejects_scan = "+quoteLiteral(rejects+"_scan"))
	}
	return "read_csv(" + strings.Join(args, ", ") + ")"
}

// loadFiles inserts the files that have an INSERT in one transaction,
// which commits them together. A file that fails ends the load with
// ON_ERROR abort; otherwise it is left out and the rest are loaded again.
// A commit that loses a race with another writer is tried again.
func (s *Server) loadFiles(ctx context.Context, sess *session, user auth.User, files []*loadFile, onError string) error {
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	rollback := func() { _ = exec(context.WithoutCancel(ctx), "ROLLBACK") }
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		var pending []*loadFile
		for _, f := range files {
			if f.Status != tasks.LoadFailed && f.insert != "" {
				pending = append(pending, f)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		if err := exec(ctx, "BEGIN"); err != nil {
			return pgError(err)
		}
		var (
			failed *loadFile
			ferr   error
		)
		for _, f := range pending {
			n, err := s.execChecked(ctx, sess, user, f.insert)
			if err != nil {
				failed, ferr = f, err
				break
			}
			f.Rows = n
		}
		if failed != nil {
			rollback()
			if fatalLoadError(ctx, ferr) {
				return pgError(ferr)
			}
			failed.fail(ferr)
			if onError == onErrorAbort {
				return fmt.Errorf("COPY INTO stopped at %s, and loaded nothing: %w", failed.File, pgError(ferr))
			}
			continue
		}
		err := exec(ctx, "COMMIT")
		if err == nil {
			return nil
		}
		rollback()
		if !commitConflict(err) || attempt == maxCommitRetries {
			return pgError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(rand.Int64N(int64(backoff)))):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// fatalLoadError reports whether err stops a whole load rather than one
// file: a refusal, a cancellation, or trouble with the table or catalog.
// It leaves no file marked as failed.
func fatalLoadError(ctx context.Context, err error) bool {
	if ctx.Err() != nil || pgerror.GetCode(err) == codes.InsufficientPrivilege {
		return true
	}
	var de *duckdb.Error
	if errors.As(err, &de) {
		switch de.Type {
		case duckdb.ErrorTypeCatalog, duckdb.ErrorTypeTransaction, duckdb.ErrorTypePermission,
			duckdb.ErrorTypeInterrupt, duckdb.ErrorTypeFatal, duckdb.ErrorTypeOutOfMemory:
			return true
		}
	}
	return false
}

// errorText is err's message for the load history, at most
// maxErrorLength bytes.
func errorText(err error) string {
	msg := strings.TrimSpace(pgerror.Flatten(pgError(err)).Message)
	if len(msg) > maxErrorLength {
		msg = msg[:maxErrorLength] + "…"
	}
	return msg
}

// execChecked prepares query as user's own statement, checking it against
// their role and grants, and runs it. It returns the rows it changed.
func (s *Server) execChecked(ctx context.Context, sess *session, user auth.User, query string) (int64, error) {
	st, err := s.prepareChecked(ctx, sess, user, query, nil)
	if err != nil {
		return 0, err
	}
	defer st.close() //nolint:errcheck
	// A load into a member table writes the ids it can (see idassign.go).
	if n, done, err := s.execWithIDs(ctx, sess, st, nil); done {
		return n, err
	}
	res, err := st.stmt.ExecContext(ctx, nil)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// execDDL is execChecked for DDL, which also does what a client's DDL
// does: CREATE OR REPLACE TABLE replaces Iceberg tables, and grants follow
// dropped and renamed tables.
func (s *Server) execDDL(ctx context.Context, sess *session, user auth.User, query string) error {
	st, err := s.prepareChecked(ctx, sess, user, query, nil)
	if err != nil {
		return err
	}
	defer st.close() //nolint:errcheck
	err = runWithCommitRetry(ctx, sess, st, func() error {
		_, err := st.stmt.ExecContext(ctx, nil)
		return err
	})
	if isIcebergNoReplace(err) {
		if r, ok := s.replaceable(sess, st); ok {
			_, err = s.replaceTable(ctx, sess, r)
		}
	}
	if err != nil {
		return pgError(err)
	}
	if changesCatalog(st.stmtType) {
		s.catalogChanged()
	}
	if st.effect != nil {
		s.applyEffect(ctx, sess, st.effect)
	}
	for _, c := range st.created {
		s.recordOwner(ctx, user, c)
	}
	return nil
}

// queryRows runs query on conn and returns every row.
func queryRows(ctx context.Context, conn *duckdb.Conn, query string) ([][]driver.Value, error) {
	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out [][]driver.Value
	for {
		dest := make([]driver.Value, len(rows.Columns()))
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		out = append(out, dest)
	}
}
