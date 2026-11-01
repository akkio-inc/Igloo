package pgwire

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/incremental"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Refreshing materialized views and dynamic tables. A refresh brings the
// view's table up to date with its query. When the query's shape allows
// it (see package incremental), it reads only what changed in the
// query's tables since the last refresh, as their Iceberg snapshots tell,
// and writes only the rows that change: a refresh after a load of a day's
// rows reads that day's rows, not the table. Otherwise, or when what
// changed cannot be read so (rows deleted from a table a projection reads,
// a table whose history was expired, a change to most of a table), it runs
// the query again over every row, and says why, in SHOW and in the run's
// detail in igloo.task_history.
//
// Every table a refresh reads is read at the snapshot it records, so the
// next refresh starts exactly where it ended, and the result equals a full
// refresh's. An aggregating view keeps each group's partial aggregates in
// a state table of Igloo's own schema igloo_refresh, which no user's
// statement reads or writes (see refreshSchema); a refresh merges into it
// the partial aggregates of what changed, and writes the groups that
// changed to the view's table, in one transaction.

// refreshSchema is the catalog schema that keeps aggregating views' state
// tables, one a view. It is Igloo's own.
const refreshSchema = "igloo_refresh"

// refreshPolicy is the row access policy, Igloo's own, on every state
// table: it is false, so no user's statement reads one.
const refreshPolicy = "igloo_refresh_state"

// Refresh modes, as Snowflake's dynamic tables name them.
const (
	refreshAuto        = "AUTO"
	refreshFull        = "FULL"
	refreshIncremental = "INCREMENTAL"
)

// Refresh actions, as SHOW and the run's detail give them.
const (
	actionIncremental = "INCREMENTAL"
	actionFull        = "FULL"
	actionNoData      = "NO_DATA"
)

// defaultFullFraction is the share of a full refresh's cost that reading
// what changed may reach before a refresh reads the tables whole rather
// than the change (see cheaperInFull).
const defaultFullFraction = 0.8

// refreshState is what a materialized view or dynamic table keeps between
// refreshes, in its job's Refresh, as JSON.
type refreshState struct {
	// Mode is the refresh mode asked for: AUTO (the default),
	// INCREMENTAL or FULL.
	Mode string `json:"mode,omitempty"`
	// Lag and Warehouse are a dynamic table's TARGET_LAG and WAREHOUSE,
	// as written.
	Lag       string `json:"target_lag,omitempty"`
	Warehouse string `json:"warehouse,omitempty"`

	// Query is a digest of the query as the last refresh ran it, policies
	// applied; a change to it, or to the policies, refreshes in full.
	Query string `json:"query,omitempty"`
	// Sources are the snapshots of the tables the last refresh read, by
	// schema.table; -1 for a table without one.
	Sources map[string]int64 `json:"sources,omitempty"`
	// Table is the snapshot of the view's table the last refresh left,
	// and State and StateSnapshot its state table's name and snapshot.
	Table         int64  `json:"table,omitempty"`
	State         string `json:"state,omitempty"`
	StateSnapshot int64  `json:"state_snapshot,omitempty"`
	// Unkeepable is the digest of a query whose values the state cannot
	// hold (see keepState), which refreshes in full until it changes, and
	// UnkeepableReason why.
	Unkeepable       string `json:"unkeepable,omitempty"`
	UnkeepableReason string `json:"unkeepable_reason,omitempty"`

	// Incremental and Reason say whether the query can be refreshed
	// incrementally, and why not, as of its last refresh or creation.
	Incremental bool   `json:"incremental,omitempty"`
	Reason      string `json:"reason,omitempty"`
	// Action and Detail are what the last refresh did, and At when it
	// ended; DataTimestamp is when the data it read was current.
	Action        string    `json:"action,omitempty"`
	Detail        string    `json:"detail,omitempty"`
	DataTimestamp time.Time `json:"data_timestamp,omitzero"`
}

func decodeRefresh(s string) refreshState {
	var st refreshState
	if s != "" {
		_ = json.Unmarshal([]byte(s), &st)
	}
	if st.Mode == "" {
		st.Mode = refreshAuto
	}
	return st
}

func (st refreshState) encode() string {
	b, _ := json.Marshal(st)
	return string(b)
}

// refreshResult is what a refresh did.
type refreshResult struct {
	rows     int64
	policies []string
	action   string
	detail   string
	// used is what DuckDB measured of the statements it ran.
	used audit.Resources
}

// refreshView refreshes j, a materialized view or dynamic table, as user
// in sess, an embedded session of the owner's, and records what it read.
// It is what CREATE OR REPLACE TABLE ... AS does, and needs the same
// privileges, which is how it is checked.
func (s *Server) refreshView(ctx context.Context, sess *session, user auth.User, j tasks.Job) (refreshResult, error) {
	schema, table, _ := strings.Cut(j.Name, ".")
	qualified := quoteName([]string{s.catalogName(), schema, table})
	if n, err := s.engine.CountStatements(j.Statement); err != nil || n != 1 || !selectLike(j.Statement) {
		return refreshResult{}, fmt.Errorf("the definition of %s %s is not one query", j.Kind, j.Name)
	}
	st, err := s.prepareChecked(ctx, sess, user, "CREATE OR REPLACE TABLE "+qualified+" AS "+j.Statement, nil)
	if err != nil {
		return refreshResult{}, err
	}
	_ = st.close()
	checked := st.access
	// What runs is the query as policies rewrite it for user, so the view
	// holds what its owner may see of protected tables.
	query, g, _, err := s.policyRewrite(ctx, sess, user, duckdb.STATEMENT_TYPE_SELECT, s.rewrite(ctx, sess, user, j.Statement), nil, nil)
	if err != nil {
		return refreshResult{}, pgError(err)
	}
	state := decodeRefresh(j.Refresh)
	r := &refresher{s: s, sess: sess, job: j, schema: schema, table: table, qualified: qualified, query: query, st: state,
		digest: digest(query), access: checked, guard: g}
	res, err := r.run(ctx)
	res.used = r.used
	if err != nil {
		return res, err
	}
	res.policies = g.applied
	return res, nil
}

// refresher carries out one refresh.
type refresher struct {
	s                *Server
	sess             *session
	job              tasks.Job
	schema, table    string
	qualified, query string
	digest           string
	st               refreshState
	plan             *incremental.Plan
	// snaps are the current snapshots of the plan's tables, by
	// schema.table; tables their loaded metadata.
	snaps map[string]int64
	// temps are the temporary views and tables the refresh made.
	temps []string
	// used adds up what DuckDB measured of the statements it ran.
	used audit.Resources
	// access is what grants let the refresh read, and guard how policies
	// rewrote its query, for a run of its query on workers (see rebuild).
	access *access
	guard  *guard
	// placement is where its query ran, when it ran on workers.
	placement string
}

// rebuild replaces the view's rows with its query's, the query on workers
// where auto mode would run it there as a read by the view's owner (see
// fillOn), the view written from their results.
func (r *refresher) rebuild(ctx context.Context) (int64, error) {
	write := func(ctx context.Context, source string) (int64, error) {
		return r.replaceRows(ctx, "SELECT * FROM ("+source+") AS refresh")
	}
	start := time.Now()
	f := &stagedFill{s: r.s, sess: r.sess, ctx: ctx}
	defer f.close()
	src := r.s.fillSource(ctx, r.sess, &statement{access: r.access, guard: r.guard}, r.query)
	if src != nil {
		src.writer = r.rebuildWriter(ctx, src)
	}
	rows, parts, _, err := r.s.fillOn(f, src, func(ctx context.Context) (int64, error) { return write(ctx, r.query) }, write)
	if err == nil && parts.key != "" {
		r.s.dist.ObserveOn(parts.key, parts.workers, parts.large, time.Since(cmp.Or(parts.since, start)))
	}
	if err == nil && parts.workers > 0 {
		r.placement = parts.placement
	}
	return rows, err
}

// rebuildWriter returns how workers would write the view's rows, in place
// of the table's files, or nil: where the refresh could not overwrite them
// itself (see overwritePlan). Workers write a partitioned view's as a table's,
// where the refresh's own COPY could not.
func (r *refresher) rebuildWriter(ctx context.Context, src *statement) *tableWrite {
	o, err := r.overwritePlan(ctx)
	if err != nil {
		return nil
	}
	w := r.s.tableWriter(ctx, r.sess, r.schema, r.table, src, nil)
	if w != nil {
		w.overwrite = o
	}
	return w
}

// onWorkers says, for a refresh's detail, where its query ran when it ran
// on workers (see rebuild).
func (r *refresher) onWorkers() string {
	if r.placement == "" {
		return ""
	}
	return "; its query ran on " + r.placement
}

// exec runs q on the refresh's connection, and adds what DuckDB measured
// of it to what the refresh used.
func (r *refresher) exec(ctx context.Context, q string) (int64, error) {
	res, err := r.sess.conn.ExecContext(ctx, q, nil)
	r.measured(q)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// measured adds what DuckDB measured of q, the statement the connection
// last ran, to what the refresh used.
func (r *refresher) measured(q string) {
	if m := r.sess.wl.meter.Last(q); m != nil {
		r.used.CPUTime += m.CPUTime
		r.used.BytesRead += m.BytesRead
		r.used.MemoryAllocated += m.MemoryAllocated
	}
}

func digest(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:16])
}

func (r *refresher) run(ctx context.Context) (refreshResult, error) {
	// Planning resolves the query's tables and lists their columns, which
	// walk the catalog's in-memory set: it holds the catalog lock
	// exclusively. The rest holds it only to change the catalog (temporary
	// tables, the state table) and while DuckDB binds what it runs, as a
	// user's statements do (see catalogRunLock): a full refresh runs a
	// query as long as any read, and held it for its whole run.
	defer r.dropTemps()
	r.st.Incremental, r.st.Reason = false, ""
	release := r.s.holdCatalog(r.sess)
	reason := r.planQuery(ctx)
	release()
	if reason == "" {
		r.st.Incremental = true
	} else {
		r.st.Reason = reason
	}
	if r.st.Mode == refreshIncremental && reason != "" {
		return refreshResult{}, fmt.Errorf("%s %s cannot be refreshed incrementally: %s (REFRESH_MODE = INCREMENTAL)", r.job.Kind, r.job.Name, reason)
	}
	why := reason
	if r.st.Mode == refreshFull {
		why = "its REFRESH_MODE is FULL"
	}
	if why == "" {
		why = r.baseline(ctx)
	}
	if why == "" {
		res, reason, err := r.incremental(ctx)
		if err != nil || reason == "" {
			return res, err
		}
		why = reason
	}
	return r.full(ctx, why)
}

// planQuery plans the query, and loads its tables' current snapshots. It
// returns why the query cannot be refreshed incrementally, or "".
func (r *refresher) planQuery(ctx context.Context) string {
	s := r.s
	if s.ddl == nil {
		return "Igloo has no Iceberg catalog, whose snapshots tell what changed"
	}
	fs, err := s.functionSets(ctx, r.sess)
	if err != nil {
		return "Igloo could not list DuckDB's functions: " + err.Error()
	}
	res, err := s.newResolver(ctx, r.sess)
	if err != nil {
		return "Igloo could not resolve its tables: " + err.Error()
	}
	conn := r.sess.conn
	env := incremental.Env{
		Parser: connParser{conn},
		Table: func(_ context.Context, parts []string) (string, string, bool, error) {
			rs, err := res.resolve(tableRef{parts: parts}, nil)
			if err != nil {
				return "", "", false, err
			}
			if len(rs.first) != 1 || len(rs.all) != 1 {
				return "", "", false, nil
			}
			o := rs.first[0]
			if o.view || o.internal || o.catalogView || o.external || !strings.EqualFold(o.db, s.catalogName()) || isIglooSchema(o.schema) ||
				strings.EqualFold(o.schema, refreshSchema) {
				return "", "", false, nil
			}
			return o.schema, o.name, true, nil
		},
		Columns: func(ctx context.Context, schema, name string) ([]string, error) {
			return queryStrings(ctx, r.sess, `SELECT column_name FROM information_schema.columns
				WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3 ORDER BY ordinal_position`, s.catalogName(), schema, name)
		},
		Describe: func(ctx context.Context, query string) ([]incremental.Column, error) {
			rows, err := queryRows(ctx, conn, "DESCRIBE "+query)
			if err != nil {
				return nil, err
			}
			out := make([]incremental.Column, len(rows))
			for i, row := range rows {
				name, _ := row[0].(string)
				typ, _ := row[1].(string)
				out[i] = incremental.Column{Name: name, Type: typ}
			}
			return out, nil
		},
		Aggregate: func(name string) bool { return fs.aggregates[name] || fs.aggregateMacros[name] },
		Unstable:  func(name string) bool { return fs.unstable[name] },
		Bitmaps:   s.engine.BitmapsLoaded(),
	}
	plan, err := incremental.Analyze(ctx, env, r.query)
	if err != nil {
		return "Igloo could not read the query: " + err.Error()
	}
	if plan.Reason == "" {
		if err := plan.Types(ctx); err != nil {
			return "Igloo could not read the query's types: " + err.Error()
		}
	}
	r.plan = plan
	if plan.Reason != "" {
		switch {
		case strings.HasPrefix(plan.Reason, "it "):
			return "the query " + strings.TrimPrefix(plan.Reason, "it ")
		case strings.HasPrefix(plan.Reason, "its "):
			return "the query's " + strings.TrimPrefix(plan.Reason, "its ")
		}
		return plan.Reason
	}
	if r.st.Unkeepable == r.digest {
		return r.st.UnkeepableReason
	}
	r.snaps = map[string]int64{}
	for _, src := range plan.Sources {
		if _, ok := r.snaps[src.Table()]; ok {
			continue
		}
		tbl, err := s.ddl.LoadTable(ctx, src.Schema, src.Name)
		if err != nil {
			return fmt.Sprintf("Igloo could not load %s: %v", src.Table(), err)
		}
		r.snaps[src.Table()] = -1
		if snap := tbl.CurrentSnapshot(); snap != nil {
			r.snaps[src.Table()] = snap.SnapshotID
		}
	}
	return ""
}

// baseline checks that what the last refresh recorded is where this one
// can go on from: the same query, its tables read, and the view's table
// and state as it left them. It returns why not, or "".
func (r *refresher) baseline(ctx context.Context) string {
	st := r.st
	switch {
	case st.Query == "":
		return "it has not been refreshed since it was created or replaced"
	case st.Query != r.digest:
		return "its query, or the policies on its tables, changed since the last refresh"
	}
	for t := range r.snaps {
		if _, ok := st.Sources[t]; !ok {
			return fmt.Sprintf("the last refresh did not read %s", t)
		}
	}
	snap, err := r.snapshot(ctx, r.schema, r.table)
	if err != nil {
		return "Igloo could not load its table: " + err.Error()
	}
	if snap != st.Table {
		return "its table was changed since the last refresh, other than by a refresh"
	}
	if r.plan.Grouped {
		if st.State == "" {
			return "it has no state yet"
		}
		snap, err := r.snapshot(ctx, refreshSchema, st.State)
		if err != nil {
			return "its state is missing"
		}
		if snap != st.StateSnapshot {
			return "its state was changed since the last refresh"
		}
	}
	return ""
}

// snapshot returns a catalog table's current snapshot, -1 for none.
func (r *refresher) snapshot(ctx context.Context, schema, name string) (int64, error) {
	tbl, err := r.s.ddl.LoadTable(ctx, schema, name)
	if err != nil {
		return 0, err
	}
	if snap := tbl.CurrentSnapshot(); snap != nil {
		return snap.SnapshotID, nil
	}
	return -1, nil
}

// changes works out what changed in each table the plan reads since the
// last refresh. It returns why the change cannot be read incrementally, or
// "".
func (r *refresher) changes(ctx context.Context) (map[string]incremental.Change, string, error) {
	out := map[string]incremental.Change{}
	fraction := r.s.refreshFraction
	if fraction <= 0 {
		fraction = defaultFullFraction
	}
	for t, to := range r.snaps {
		from := r.st.Sources[t]
		schema, name, _ := strings.Cut(t, ".")
		if from == to {
			out[t] = incremental.Change{From: from, To: to}
			continue
		}
		if to < 0 {
			return nil, fmt.Sprintf("%s has no snapshot any more", t), nil
		}
		tbl, err := r.s.ddl.LoadTable(ctx, schema, name)
		if err != nil {
			return nil, "", err
		}
		// A refresh picks a change's rows by DuckDB's columns of each
		// row's data file and position, which a column of the same name
		// would hide.
		for _, v := range []string{"filename", "file_row_number"} {
			if _, ok := tbl.Schema().FindFieldByNameCaseInsensitive(v); ok {
				return nil, fmt.Sprintf("%s has a column named %s, by which DuckDB names each row's data file or position, and a refresh picks what changed", t, v), nil
			}
		}
		c, err := incremental.Diff(ctx, tbl, t, from, to, r.s.deleteTargets)
		if err != nil {
			return nil, "", err
		}
		if c.Reason != "" {
			return nil, c.Reason, nil
		}
		out[t] = c
	}
	for _, src := range r.plan.Sources {
		c := out[src.Table()]
		if c.Empty() {
			continue
		}
		if src.Nullable {
			return nil, fmt.Sprintf("%s, which an outer join fills with NULLs, changed", src.Table()), nil
		}
		if !c.AppendOnly() && !r.plan.Invertible() {
			what := "a projection"
			if r.plan.Grouped {
				what = "min, max and count(DISTINCT)"
			}
			return nil, fmt.Sprintf("rows were deleted from or updated in %s, which %s cannot take incrementally", src.Table(), what), nil
		}
	}
	why, err := r.cheaperInFull(ctx, out, fraction)
	if err != nil || why != "" {
		return nil, why, err
	}
	return out, "", nil
}

// deletedRowCost is what DuckDB takes to apply a row's deletion by a
// position delete file as it scans a table, in the bytes of files it reads
// in that time: 1.3 to 3 KiB on local disk, and more on S3, where a count
// and sum over a billion rows went from 2.7 s to 17 s once an update had
// deleted 20 million of them.
const deletedRowCost = 2048

// cheaperInFull returns why reading the query's tables whole would cost
// about as much as reading what changed, or "". Each read costs the bytes
// of the files it reads and deletedRowCost for each row it applies a
// position delete file's deletion of: a full refresh reads every
// reference's table at its new snapshot, its delete files applied; an
// incremental one each changed reference's change (the files added, the
// files removed and those with rows deleted since, as the last refresh
// read them, and the new delete files, joined by position) and, with it,
// the other references' tables whole, as the delta rule joins them.
// Reading what changed costs more for each byte, in the merge into the
// view and its state, so it is chosen only while it costs less than
// fraction of a full refresh. After an update of a few percent of a
// table's rows, which touches every file, the change reads the files once
// as they were, without the new deletions, which a full refresh applies.
func (r *refresher) cheaperInFull(ctx context.Context, changes map[string]incremental.Change, fraction float64) (string, error) {
	whole, changed := map[string]int64{}, map[string]int64{}
	for _, src := range r.plan.Sources {
		t := src.Table()
		if _, ok := whole[t]; ok {
			continue
		}
		schema, name, _ := strings.Cut(t, ".")
		tbl, err := r.s.ddl.LoadTable(ctx, schema, name)
		if err != nil {
			return "", err
		}
		cost := func(snap int64) (bytes, deleted int64, err error) {
			if _, bytes, err = incremental.SizeAt(ctx, tbl, snap); err != nil {
				return 0, 0, err
			}
			deleted, err = incremental.Deleted(ctx, tbl, snap)
			return bytes, deleted, err
		}
		b, d, err := cost(r.snaps[t])
		if err != nil {
			return "", err
		}
		whole[t] = b + deletedRowCost*d
		c := changes[t]
		if c.Empty() {
			continue
		}
		changed[t] = c.Bytes
		if c.FromBytes > 0 {
			// The rows the last refresh's snapshot deleted of the files the
			// change reads as it has them, as their share of its bytes.
			fb, fd, err := cost(c.From)
			if err != nil {
				return "", err
			}
			if fb > 0 {
				changed[t] += int64(float64(deletedRowCost*fd) * min(1, float64(c.FromBytes)/float64(fb)))
			}
		}
	}
	var full, delta int64
	for i, src := range r.plan.Sources {
		full += whole[src.Table()]
		c := changes[src.Table()]
		if c.Empty() {
			continue
		}
		delta += changed[src.Table()]
		for k, other := range r.plan.Sources {
			if k != i {
				delta += whole[other.Table()]
			}
		}
	}
	if full > 0 && float64(delta) > fraction*float64(full) {
		return fmt.Sprintf("reading what changed costs about as much as reading its tables whole: %d against %d bytes of files, "+
			"each deleted row to apply counted as %d", delta, full, deletedRowCost), nil
	}
	return "", nil
}

// incremental refreshes from what changed. It returns why it could not,
// with nothing written, for a full refresh instead.
func (r *refresher) incremental(ctx context.Context) (refreshResult, string, error) {
	changes, why, err := r.changes(ctx)
	if err != nil || why != "" {
		return refreshResult{}, why, err
	}
	var added, removed, bytes int64
	var changedTables []string
	for t, c := range changes {
		if !c.Empty() {
			changedTables = append(changedTables, t)
			added += c.AddedRows
			removed += c.RemovedRows + c.TouchedRows
			bytes += c.Bytes
		}
	}
	slices.Sort(changedTables)
	if len(changedTables) == 0 {
		return r.done(ctx, refreshResult{action: actionNoData, detail: "nothing changed in its tables since the last refresh"})
	}
	// Each changed table's rows added (ins), deleted (del), and both, with
	// their signs (chg).
	views := map[string][3]string{}
	for i, t := range changedTables {
		c := changes[t]
		schema, name, _ := strings.Cut(t, ".")
		src := quoteName([]string{r.s.catalogName(), schema, name})
		at := func(v int64) string { return src + " AT (VERSION => " + strconv.FormatInt(v, 10) + ")" }
		in := func(files []string) string {
			q := make([]string, len(files))
			for i, f := range files {
				q[i] = quoteLiteral(f)
			}
			return "filename IN (" + strings.Join(q, ", ") + ")"
		}
		v := [3]string{fmt.Sprintf("__igloo_ins_%d", i), fmt.Sprintf("__igloo_del_%d", i), fmt.Sprintf("__igloo_chg_%d", i)}
		ins := "SELECT * FROM " + at(c.To) + " WHERE false"
		if len(c.Added) > 0 {
			ins = "SELECT * FROM " + at(c.To) + " WHERE " + in(c.Added)
		}
		if err := r.temp(ctx, "VIEW", v[0], ins); err != nil {
			return refreshResult{}, "", err
		}
		if !c.AppendOnly() {
			var parts []string
			if len(c.Removed) > 0 {
				parts = append(parts, "SELECT * FROM "+at(c.From)+" WHERE "+in(c.Removed))
			}
			if len(c.Touched) > 0 {
				// The rows the new delete files delete: those of the files
				// they name, read as From has them, at their positions.
				// Matched by the files' names, which writers spell the same
				// in delete files and manifests however they spell the rest
				// of the path. Taking From's rows of the files EXCEPT ALL
				// To's read them twice and hashed every row: after an
				// update of 2% of a table's rows spread over all its
				// files, slower than reading the table whole.
				deletes := make([]string, len(c.Deletes))
				for i, f := range c.Deletes {
					deletes[i] = quoteLiteral(f)
				}
				parts = append(parts, "(SELECT r.* EXCLUDE (__igloo_file, __igloo_pos) FROM (SELECT *, regexp_extract(filename, '[^/]+$') AS __igloo_file, "+
					"file_row_number AS __igloo_pos FROM "+at(c.From)+" WHERE "+in(c.Touched)+") AS r SEMI JOIN "+
					"(SELECT regexp_extract(file_path, '[^/]+$') AS f, pos FROM read_parquet(["+strings.Join(deletes, ", ")+"])) AS d "+
					"ON r.__igloo_file = d.f AND r.__igloo_pos = d.pos)")
			}
			if err := r.temp(ctx, "VIEW", v[1], strings.Join(parts, " UNION ALL ")); err != nil {
				return refreshResult{}, "", err
			}
			if err := r.temp(ctx, "VIEW", v[2], "SELECT *, 1 AS __igloo_sign FROM "+v[0]+" UNION ALL SELECT *, -1 AS __igloo_sign FROM "+v[1]); err != nil {
				return refreshResult{}, "", err
			}
		}
		views[t] = v
	}
	// The delta rule: one term for each reference to a changed table, which
	// reads that reference's changes, the references before it as they
	// are now, and those after it as they were.
	srcs := r.plan.Sources
	var terms []string
	for i, src := range srcs {
		c := changes[src.Table()]
		if c.Empty() {
			continue
		}
		pins := make([]incremental.Pin, len(srcs))
		sign := ""
		for k, other := range srcs {
			oc := changes[other.Table()]
			switch {
			case k < i:
				pins[k], err = r.pinAt(ctx, other.Table(), oc.To)
			case k > i:
				pins[k], err = r.pinAt(ctx, other.Table(), oc.From)
			case oc.AppendOnly():
				pins[k].View, pins[k].Changed = views[other.Table()][0], true
			default:
				pins[k].View, pins[k].Changed, pins[k].Signed, sign = views[other.Table()][2], true, true, other.Outer
			}
			if err != nil {
				return refreshResult{}, "", err
			}
		}
		var term string
		if r.plan.Grouped {
			term, err = r.plan.Input(ctx, pins, sign)
		} else {
			term, err = r.plan.Query(ctx, pins)
		}
		if err != nil {
			return refreshResult{}, "", err
		}
		terms = append(terms, "("+term+")")
	}
	detail := fmt.Sprintf("read %d rows added", added)
	if removed > 0 {
		detail += fmt.Sprintf(" and %d rows deleted or in files with rows deleted", removed)
	}
	detail += fmt.Sprintf(" in %s (%d bytes of files)", strings.Join(changedTables, ", "), bytes)
	union := strings.Join(terms, " UNION ALL ")
	if !r.plan.Grouped {
		rows, err := r.write(ctx, "INSERT INTO "+r.qualified+" SELECT * FROM ("+union+") AS __igloo_delta")
		if err != nil {
			return refreshResult{}, "", err
		}
		return r.done(ctx, refreshResult{rows: rows, action: actionIncremental, detail: detail + fmt.Sprintf("; added %d rows", rows)})
	}
	return r.mergeGroups(ctx, union, detail)
}

// mergeGroups merges the partial aggregates of the changed rows, input,
// into the state, and writes the groups that changed.
func (r *refresher) mergeGroups(ctx context.Context, input, detail string) (refreshResult, string, error) {
	p := r.plan
	state := quoteName([]string{r.s.catalogName(), refreshSchema, r.st.State})
	if err := r.temp(ctx, "TABLE", "__igloo_delta", p.Partials(input, true)); err != nil {
		return refreshResult{}, "", err
	}
	if q := p.Overflow("__igloo_delta"); q != "" {
		if n, err := r.count(ctx, q); err != nil || n > 0 {
			return refreshResult{}, "count(DISTINCT x) met values outside 0..4294967295, which its bitmaps cannot hold", err
		}
	}
	groups, err := r.count(ctx, "SELECT count(*) FROM __igloo_delta")
	if err != nil {
		return refreshResult{}, "", err
	}
	if groups == 0 {
		return r.done(ctx, refreshResult{action: actionIncremental, detail: detail + "; no group changed"})
	}
	if err := r.temp(ctx, "TABLE", "__igloo_merged", p.Merge(state, "__igloo_delta")); err != nil {
		return refreshResult{}, "", err
	}
	if q := p.AverageLimit("__igloo_merged"); q != "" {
		if n, err := r.count(ctx, q); err != nil || n > 0 {
			return refreshResult{}, "an average's sum outgrew what a double holds exactly", err
		}
	}
	stateCols := r.stateColumns()
	alive := "SELECT " + stateCols + " FROM __igloo_merged AS m WHERE " + p.Alive("m")
	// whole is the view's rows when it is written whole, by overwrite (see
	// replaceRows), and stmts otherwise what changes them.
	var whole string
	var stmts []string
	cols, keyed := p.KeyColumns()
	switch {
	case p.Global:
		if whole, err = p.Final(ctx, alive); err != nil {
			return refreshResult{}, "", err
		}
	case keyed:
		names, err := r.viewColumns(ctx)
		if err != nil {
			return refreshResult{}, "", err
		}
		var conds []string
		for i, k := range p.Keys() {
			conds = append(conds, "m."+quoteIdent(names[cols[i]])+" IS NOT DISTINCT FROM d."+k)
		}
		final, err := p.Final(ctx, alive)
		if err != nil {
			return refreshResult{}, "", err
		}
		stmts = append(stmts, "DELETE FROM "+r.qualified+" AS m WHERE EXISTS (SELECT 1 FROM __igloo_delta AS d WHERE "+strings.Join(conds, " AND ")+")",
			"INSERT INTO "+r.qualified+" "+final)
	default:
		// The view's rows do not show their groups: it is written whole,
		// from the state, which the delta's groups replace.
		whole, err = p.Final(ctx, "SELECT "+stateCols+" FROM "+state+" AS s WHERE NOT EXISTS (SELECT 1 FROM __igloo_delta AS d WHERE "+
			p.Match("s", "d")+") UNION ALL "+alive)
		if err != nil {
			return refreshResult{}, "", err
		}
	}
	var stateStmts []string
	if p.Global {
		stateStmts = append(stateStmts, "DELETE FROM "+state)
	} else {
		stateStmts = append(stateStmts, "DELETE FROM "+state+" AS s WHERE EXISTS (SELECT 1 FROM __igloo_delta AS d WHERE "+p.Match("s", "d")+")")
	}
	stateStmts = append(stateStmts, "INSERT INTO "+state+" "+alive)
	var rows int64
	if whole != "" {
		rows, err = r.replaceRows(ctx, whole, stateStmts...)
	} else {
		rows, err = r.write(ctx, append(stmts, stateStmts...)...)
	}
	if err != nil {
		return refreshResult{}, "", err
	}
	return r.done(ctx, refreshResult{rows: rows, action: actionIncremental, detail: detail + fmt.Sprintf("; %d groups changed", groups)})
}

// full runs the query again over every row of its tables, read at the
// snapshots it records when it can go on incrementally from them, and
// rebuilds an aggregating view's state.
func (r *refresher) full(ctx context.Context, why string) (refreshResult, error) {
	detail := "ran the query over every row: " + why
	if rows, bytes, ok := r.size(ctx); ok {
		detail = fmt.Sprintf("ran the query over every row (%d rows, %d bytes of files): %s", rows, bytes, why)
	}
	p := r.plan
	if p == nil || p.Reason != "" || r.snaps == nil || r.st.Mode == refreshFull {
		// Nothing to go on from: the query as it is, and no state.
		rows, err := r.rebuild(ctx)
		if err != nil {
			return refreshResult{}, err
		}
		r.dropState(ctx)
		res, _, err := r.done(ctx, refreshResult{rows: rows, action: actionFull, detail: detail + r.onWorkers()})
		return res, err
	}
	pins := make([]incremental.Pin, len(p.Sources))
	for i, src := range p.Sources {
		var err error
		if pins[i], err = r.pinAt(ctx, src.Table(), r.snaps[src.Table()]); err != nil {
			return refreshResult{}, err
		}
	}
	if !p.Grouped {
		q, err := p.Query(ctx, pins)
		if err != nil {
			return refreshResult{}, err
		}
		rows, err := r.replaceRows(ctx, "SELECT * FROM ("+q+") AS refresh")
		if err != nil {
			return refreshResult{}, err
		}
		res, _, err := r.done(ctx, refreshResult{rows: rows, action: actionFull, detail: detail})
		return res, err
	}
	input, err := p.Input(ctx, pins, "")
	if err != nil {
		return refreshResult{}, err
	}
	if err := r.temp(ctx, "TABLE", "__igloo_full", p.Partials(input, false)); err != nil {
		return refreshResult{}, err
	}
	if reason, err := r.keepState(ctx); err != nil {
		return refreshResult{}, err
	} else if reason != "" {
		// The state cannot hold these values: the query as it is, until it
		// changes.
		r.st.Unkeepable, r.st.UnkeepableReason, r.st.Incremental, r.st.Reason = r.digest, reason, false, reason
		rows, err := r.rebuild(ctx)
		if err != nil {
			return refreshResult{}, err
		}
		r.dropState(ctx)
		res, _, err := r.done(ctx, refreshResult{rows: rows, action: actionFull, detail: "ran the query over every row: " + reason + r.onWorkers()})
		return res, err
	}
	name, err := r.makeState(ctx)
	if err != nil {
		return refreshResult{}, err
	}
	state := quoteName([]string{r.s.catalogName(), refreshSchema, name})
	rows := "SELECT " + r.stateColumns() + " FROM (" + p.StateRows("__igloo_full") + ") AS f"
	final, err := p.Final(ctx, rows)
	if err != nil {
		return refreshResult{}, err
	}
	n, err := r.replaceRows(ctx, final, "INSERT INTO "+state+" "+rows)
	if err != nil {
		return refreshResult{}, err
	}
	res, _, err := r.done(ctx, refreshResult{rows: n, action: actionFull, detail: detail})
	return res, err
}

// size adds up the rows and data files' bytes of the plan's tables, as
// they are now; ok is false when it cannot tell.
func (r *refresher) size(ctx context.Context) (rows, bytes int64, ok bool) {
	if r.snaps == nil {
		return 0, 0, false
	}
	for t := range r.snaps {
		schema, name, _ := strings.Cut(t, ".")
		tbl, err := r.s.ddl.LoadTable(ctx, schema, name)
		if err != nil {
			return 0, 0, false
		}
		n, b, err := incremental.Size(tbl)
		if err != nil {
			return 0, 0, false
		}
		rows, bytes = rows+n, bytes+b
	}
	return rows, bytes, true
}

// keepState checks that the state can hold the full refresh's partial
// aggregates, in __igloo_full, exactly; it returns why not, or "".
func (r *refresher) keepState(ctx context.Context) (string, error) {
	p := r.plan
	if q := p.Overflow("__igloo_full"); q != "" {
		n, err := r.count(ctx, q)
		if err != nil {
			return "", err
		}
		if n > 0 {
			return "count(DISTINCT x) has values outside 0..4294967295, which its bitmaps cannot hold", nil
		}
	}
	if q := p.AverageLimit("(" + p.StateRows("__igloo_full") + ")"); q != "" {
		n, err := r.count(ctx, q)
		if err != nil {
			return "", err
		}
		if n > 0 {
			return "an average's sum is too large for a double to hold exactly", nil
		}
	}
	return "", nil
}

// stateName is the name of the state table of a view, schema.table: its
// name and a digest that tells apart views whose names would otherwise
// make the same one.
func stateName(view string) string {
	_, table, _ := strings.Cut(view, ".")
	h := sha256.Sum256([]byte(strings.ToLower(view)))
	return strings.ToLower(table) + "__" + hex.EncodeToString(h[:4])
}

// makeState makes the view's state table afresh, empty, with the columns
// the plan keeps, guarded by refreshPolicy, and returns its name.
func (r *refresher) makeState(ctx context.Context) (string, error) {
	name := stateName(r.job.Name)
	if r.st.State != "" && r.st.State != name {
		// Its state under the name it had before it was renamed.
		r.dropState(ctx)
	}
	if err := r.s.ensureRefreshPolicy(ctx, name); err != nil {
		return "", err
	}
	var defs []string
	for _, c := range r.plan.StateColumns() {
		defs = append(defs, quoteIdent(c.Name)+" "+c.Type)
	}
	qualified := quoteName([]string{r.s.catalogName(), refreshSchema, name})
	err := r.s.execHeld(ctx, r.sess,
		"CREATE SCHEMA IF NOT EXISTS "+quoteName([]string{r.s.catalogName(), refreshSchema}),
		"DROP TABLE IF EXISTS "+qualified,
		"CREATE TABLE "+qualified+" ("+strings.Join(defs, ", ")+")")
	if err != nil {
		return "", fmt.Errorf("make the state table %s.%s: %w", refreshSchema, name, err)
	}
	r.st.State = name
	if r.s.ddl != nil {
		r.s.ddl.TableChanged(refreshSchema, name)
	}
	return name, nil
}

// dropState drops the view's state table, if it has one.
func (r *refresher) dropState(ctx context.Context) {
	if r.st.State == "" {
		return
	}
	name := r.st.State
	r.st.State, r.st.StateSnapshot = "", 0
	r.s.dropState(ctx, r.sess, name)
}

// dropStateTable drops the state table name.
func (s *Server) dropStateTable(ctx context.Context, sess *session, name string) error {
	if err := s.execHeld(ctx, sess, "DROP TABLE IF EXISTS "+quoteName([]string{s.catalogName(), refreshSchema, name})); err != nil {
		return err
	}
	if s.ddl != nil {
		s.ddl.TableChanged(refreshSchema, name)
	}
	return nil
}

// stateColumns lists the state's columns, quoted.
func (r *refresher) stateColumns() string {
	var names []string
	for _, c := range r.plan.StateColumns() {
		names = append(names, quoteIdent(c.Name))
	}
	return strings.Join(names, ", ")
}

// viewColumns lists the columns of the view's table, holding the catalog
// lock to list them.
func (r *refresher) viewColumns(ctx context.Context) ([]string, error) {
	defer r.s.holdCatalog(r.sess)()
	return queryStrings(ctx, r.sess, `SELECT column_name FROM information_schema.columns
		WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3 ORDER BY ordinal_position`, r.s.catalogName(), r.schema, r.table)
}

// pinAt returns how to read table at its snapshot v: AT (VERSION => v),
// or, for v -1, a table without a snapshot, as an empty view of it.
func (r *refresher) pinAt(ctx context.Context, table string, v int64) (incremental.Pin, error) {
	if v >= 0 {
		return incremental.Pin{Version: v}, nil
	}
	schema, name, _ := strings.Cut(table, ".")
	view := "__igloo_empty_" + digest(table)[:8]
	err := r.temp(ctx, "VIEW", view, "SELECT * FROM "+quoteName([]string{r.s.catalogName(), schema, name})+" WHERE false")
	return incremental.Pin{View: view}, err
}

// temp makes a temporary view or table, which the refresh drops when it
// ends: a table empty, holding the catalog lock, and then filled, which
// holds it only while DuckDB binds the query (see runTempCreate).
func (r *refresher) temp(ctx context.Context, kind, name, query string) error {
	create := "CREATE OR REPLACE TEMP " + kind + " " + quoteIdent(name) + " AS " + query
	if kind == "TABLE" {
		create += "\nWITH NO DATA"
	}
	release := r.s.holdCatalog(r.sess)
	_, err := r.exec(ctx, create)
	release()
	if err != nil {
		return err
	}
	if !slices.Contains(r.temps, kind+" "+name) {
		r.temps = append(r.temps, kind+" "+name)
	}
	if kind == "TABLE" {
		_, err = r.exec(ctx, "INSERT INTO temp.main."+quoteIdent(name)+"\n"+query)
	}
	return err
}

func (r *refresher) dropTemps() {
	ctx := context.Background()
	defer r.s.holdCatalog(r.sess)()
	for _, t := range slices.Backward(r.temps) {
		kind, name, _ := strings.Cut(t, " ")
		_, _ = r.sess.conn.ExecContext(ctx, "DROP "+kind+" IF EXISTS temp.main."+quoteIdent(name), nil)
	}
}

// count runs a query of one integer.
func (r *refresher) count(ctx context.Context, q string) (int64, error) {
	rows, err := queryRows(ctx, r.sess.conn, q)
	r.measured(q)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return 0, fmt.Errorf("%s: no result", q)
	}
	switch v := rows[0][0].(type) {
	case int64:
		return v, nil
	case nil:
		return 0, nil
	}
	return strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
}

// write runs stmts in one transaction, again on a conflict with another
// writer's commit, and returns the rows the first INSERT into the view's
// table wrote.
func (r *refresher) write(ctx context.Context, stmts ...string) (int64, error) {
	exec := r.exec
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		var rows int64
		counted := false
		_, err := exec(ctx, "BEGIN")
		for _, q := range stmts {
			if err != nil {
				break
			}
			var n int64
			n, err = exec(ctx, q)
			if !counted && strings.HasPrefix(q, "INSERT INTO "+r.qualified+" ") {
				rows, counted = n, true
			}
		}
		if err == nil {
			if _, err = exec(ctx, "COMMIT"); err == nil {
				return rows, nil
			}
		}
		_, _ = exec(context.WithoutCancel(ctx), "ROLLBACK")
		if !commitConflict(err) || attempt == maxCommitRetries {
			return -1, pgError(err)
		}
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// done records what the refresh read and left, so the next one goes on
// from there, and what it did. withState says it wrote the state.
func (r *refresher) done(ctx context.Context, res refreshResult) (refreshResult, string, error) {
	st := &r.st
	st.Action, st.Detail, st.DataTimestamp = res.action, res.detail, time.Now().UTC()
	if r.snaps != nil && r.plan != nil && r.plan.Reason == "" && st.Unkeepable != r.digest && st.Mode != refreshFull {
		st.Query, st.Sources = r.digest, r.snaps
		if snap, err := r.snapshot(ctx, r.schema, r.table); err == nil {
			st.Table = snap
		} else {
			st.Query = ""
		}
		if r.plan.Grouped && st.State != "" {
			if snap, err := r.snapshot(ctx, refreshSchema, st.State); err == nil {
				st.StateSnapshot = snap
			} else {
				st.Query = ""
			}
		}
	} else {
		st.Query, st.Sources, st.Table = "", nil, 0
	}
	if err := r.s.tasks.Store().SetRefresh(ctx, r.job.Kind, r.job.Name, st.encode()); err != nil {
		// The next refresh finds nothing to go on from, and is full.
		r.s.log.Warn("could not record what a refresh read; the next one is full", "kind", r.job.Kind, "name", r.job.Name, "err", err)
	}
	tasks.SetDetail(ctx, fmt.Sprintf("%s: %s; %v of CPU", strings.ToLower(res.action), res.detail, r.used.CPUTime.Round(time.Millisecond)))
	return res, "", nil
}

// connParser parses and prints SQL with DuckDB on a connection.
type connParser struct{ conn *duckdb.Conn }

func (p connParser) Parse(ctx context.Context, query string) (string, error) {
	return queryOne(ctx, p.conn, "SELECT system.main.json_serialize_sql($1::VARCHAR)::VARCHAR", query)
}

func (p connParser) Print(ctx context.Context, doc string) (string, error) {
	return queryOne(ctx, p.conn, "SELECT system.main.json_deserialize_sql($1::VARCHAR::JSON)", doc)
}

// ensureRefreshPolicy puts refreshPolicy, which keeps every user's
// statements from reading it, on the state table name.
func (s *Server) ensureRefreshPolicy(ctx context.Context, name string) error {
	if s.gov == nil || s.gov.store == nil {
		return nil
	}
	return s.guardIglooTable(ctx, refreshSchema, name, refreshPolicy,
		"Igloo's own: keeps materialized views' and dynamic tables' state (igloo_refresh) from every user's statements")
}

// guardIglooTable puts the row access policy name, Igloo's own, whose body
// is false, on the table schema.table of one of Igloo's schemas, by its
// column __c0, so that no user's statement reads it: Igloo reads it on its
// own connection.
//
// The policies' generation, which every result the result cache keeps is
// tied to, stays as it is (see governance.reloadOwn): no user's statement
// reads Igloo's schemas, so none changes, and a refresh of a materialized
// view emptied the cache of every dashboard.
func (s *Server) guardIglooTable(ctx context.Context, schema, table, name, comment string) error {
	if snap := s.gov.snapshot(ctx); snap != nil {
		if _, ok := snap.Catalog.Policy(name); ok && slices.ContainsFunc(snap.Catalog.RowFilters, func(f policy.RowFilter) bool {
			return f.Policy == name && strings.EqualFold(f.Schema, schema) && strings.EqualFold(f.Table, table)
		}) {
			return nil
		}
	}
	now := time.Now().UTC()
	err := s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if _, ok := cat.Policy(name); !ok {
			p := policy.Policy{Kind: policy.RowAccess, Name: name, Args: []policy.Arg{{Name: "__c0", Type: "BIGINT"}},
				Returns: "BOOLEAN", Body: "false", CreatedBy: "igloo", Comment: comment}
			if err := cat.PutPolicy(p, false, now); err != nil {
				return err
			}
		}
		err := cat.AddRowFilter(policy.RowFilter{Schema: schema, Table: table, Policy: name, Columns: []string{"__c0"}, SetBy: "igloo"}, now)
		if err != nil && !errors.Is(err, policy.ErrExists) {
			return err
		}
		return nil
	})
	if err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reloadOwn(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	return nil
}

// refreshReason works out, for CREATE, whether query, as user would
// refresh it, can be refreshed incrementally: it returns why not, or "".
// An error means the query could not be read, which creating it reports.
func (s *Server) refreshReason(ctx context.Context, sess *session, user auth.User, query string) (string, error) {
	rewritten, _, _, err := s.policyRewrite(ctx, sess, user, duckdb.STATEMENT_TYPE_SELECT, s.rewrite(ctx, sess, user, query), nil, nil)
	if err != nil {
		return "", err
	}
	r := &refresher{s: s, sess: sess, query: rewritten, digest: digest(rewritten), st: refreshState{Mode: refreshAuto}}
	defer s.holdCatalog(sess)()
	return r.planQuery(ctx), nil
}

// dropViewState drops the state table of j, a materialized view or
// dynamic table, if it has one, and the policy that guards it.
func (s *Server) dropViewState(ctx context.Context, sess *session, j tasks.Job) {
	if st := decodeRefresh(j.Refresh); st.State != "" && sess != nil {
		s.dropState(ctx, sess, st.State)
	}
}

// dropState drops the state table name, and the policy that guards it.
func (s *Server) dropState(ctx context.Context, sess *session, name string) {
	if err := s.dropStateTable(ctx, sess, name); err != nil {
		s.log.Warn("could not drop a state table", "table", refreshSchema+"."+name, "err", err)
		return
	}
	err := s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if err := cat.DropRowFilter("", refreshSchema, name, refreshPolicy); err != nil && !errors.Is(err, policy.ErrNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		s.log.Warn("could not drop a state table's policy", "table", refreshSchema+"."+name, "err", err)
		return
	}
	if err := s.gov.reloadOwn(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
}

// upstreamKey carries how deep refreshUpstream is.
type upstreamKey struct{}

// maxUpstream bounds dynamic tables refreshing those they read.
const maxUpstream = 16

// refreshUpstream refreshes, before j, a dynamic table, the dynamic tables
// it reads whose TARGET_LAG is DOWNSTREAM and that are not suspended: they
// refresh only for those that read them. A failure fails j's refresh.
func (s *Server) refreshUpstream(ctx context.Context, sess *session, j tasks.Job) error {
	depth, _ := ctx.Value(upstreamKey{}).(int)
	if depth >= maxUpstream {
		return fmt.Errorf("dynamic tables read each other more than %d deep", maxUpstream)
	}
	jobs, err := s.tasks.Store().Jobs(ctx, tasks.KindDynamicTable)
	if err != nil {
		return err
	}
	downstream := map[string]tasks.Job{}
	for _, d := range jobs {
		if d.State == tasks.StateStarted && strings.EqualFold(decodeRefresh(d.Refresh).Lag, "DOWNSTREAM") && !strings.EqualFold(d.Name, j.Name) {
			downstream[strings.ToLower(d.Name)] = d
		}
	}
	if len(downstream) == 0 {
		return nil
	}
	release := s.holdCatalog(sess)
	r, err := s.newResolver(ctx, sess)
	var reads readSet
	if err == nil {
		r.lenient = true
		reads, err = r.analyze(j.Statement, nil, 0)
	}
	release()
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, upstreamKey{}, depth+1)
	for _, o := range reads.tables {
		d, ok := downstream[strings.ToLower(o.String())]
		if !ok {
			continue
		}
		if _, err := s.tasks.RunNow(ctx, s, d); err != nil && !errors.Is(err, tasks.ErrRunning) {
			return fmt.Errorf("refresh %s, which it reads: %w", d.Name, err)
		}
	}
	return nil
}
