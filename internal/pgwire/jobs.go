package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/schedule"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Tasks, pipes, materialized views, dynamic tables and alerts. DuckDB has
// none of them, so Igloo carries out these statements itself and keeps the
// definitions in the tasks store (package tasks):
//
//	CREATE [OR REPLACE] TASK [IF NOT EXISTS] name SCHEDULE 'spec' [TIMEZONE 'tz'] AS statement
//	CREATE [OR REPLACE] PIPE [IF NOT EXISTS] name [SCHEDULE 'spec' [TIMEZONE 'tz']] AS COPY INTO ...
//	CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] name [SCHEDULE 'spec' [TIMEZONE 'tz']] AS query
//	CREATE [OR REPLACE] DYNAMIC TABLE [IF NOT EXISTS] name TARGET_LAG = 'lag' | DOWNSTREAM [WAREHOUSE = w]
//		[REFRESH_MODE = AUTO | FULL | INCREMENTAL] [INITIALIZE = ON_CREATE | ON_SCHEDULE] AS query
//	CREATE [OR REPLACE] ALERT [IF NOT EXISTS] name SCHEDULE 'spec' [TIMEZONE 'tz'] IF (EXISTS (query)) THEN NOTIFY ...
//	ALTER TASK | PIPE | MATERIALIZED VIEW | ALERT [IF EXISTS] name RESUME | SUSPEND | SET SCHEDULE 'spec' [TIMEZONE 'tz']
//	ALTER DYNAMIC TABLE [IF EXISTS] name RESUME | SUSPEND | REFRESH | SET TARGET_LAG = 'lag' | DOWNSTREAM [WAREHOUSE = w]
//	DROP TASK | PIPE | MATERIALIZED VIEW | DYNAMIC TABLE | ALERT [IF EXISTS] name
//	EXECUTE TASK | ALERT name
//	REFRESH MATERIALIZED VIEW name
//	SHOW TASKS | PIPES | MATERIALIZED VIEWS | DYNAMIC TABLES [LIKE 'pattern'] [IN ...] | ALERTS
//
// What an alert does, IF ... THEN NOTIFY ..., is in alerts.go.
//
// Work always runs as its owner, the user who created it, with the
// privileges the owner has when it runs: a run is a new session of the
// owner's, through the same checks as their own statements. Scheduling
// work needs the writer role. Only owners and admins may change, run or
// drop a job, and users other than admins see only their own.

// pipeSchedule is how often a pipe without a schedule checks for files.
const pipeSchedule = "1 minute"

// jobCommand is a parsed statement on a task, pipe or materialized view.
type jobCommand struct {
	verb string // CREATE, ALTER, DROP, EXECUTE, REFRESH or SHOW
	kind string
	name []string
	// replace, ifExists and ifNotExists are the statement's clauses.
	replace, ifExists, ifNotExists bool
	// schedule and timezone, when set, are the SCHEDULE and TIMEZONE.
	schedule, timezone *string
	// action is ALTER's RESUME, SUSPEND, SCHEDULE or LAG.
	action string
	// body is what follows CREATE's AS.
	body string
	// lag, warehouse, refreshMode and initialize are a dynamic table's
	// TARGET_LAG (DOWNSTREAM, or the lag as written), WAREHOUSE,
	// REFRESH_MODE and INITIALIZE.
	lag, warehouse          *string
	refreshMode, initialize string
	// like and schema are SHOW's LIKE pattern and IN SCHEMA.
	like   *string
	schema string
	// notices are said to the client: options accepted and ignored.
	notices []string
}

// parseJobCommand recognizes the statements above. ok is false for any
// other; err reports a malformed one.
func parseJobCommand(query string) (c jobCommand, ok bool, err error) {
	sc := &scanner{s: query}
	kind := func() bool {
		switch {
		case sc.keywords("TASK"):
			c.kind = tasks.KindTask
		case sc.keywords("PIPE"):
			c.kind = tasks.KindPipe
		case sc.keywords("MATERIALIZED", "VIEW"):
			c.kind = tasks.KindMaterializedView
		case sc.keywords("DYNAMIC", "TABLE"):
			c.kind = tasks.KindDynamicTable
		case sc.keywords("ALERT"):
			c.kind = tasks.KindAlert
		default:
			return false
		}
		return true
	}
	plural := func() bool {
		switch {
		case sc.keywords("TASKS"):
			c.kind = tasks.KindTask
		case sc.keywords("PIPES"):
			c.kind = tasks.KindPipe
		case sc.keywords("MATERIALIZED", "VIEWS"):
			c.kind = tasks.KindMaterializedView
		case sc.keywords("DYNAMIC", "TABLES"):
			c.kind = tasks.KindDynamicTable
		case sc.keywords("ALERTS"):
			c.kind = tasks.KindAlert
		default:
			return false
		}
		return true
	}
	switch {
	case sc.keywords("CREATE"):
		c.replace = sc.keywords("OR", "REPLACE")
		if !kind() {
			return c, false, nil
		}
		c.verb = "CREATE"
	case sc.keywords("ALTER"):
		if !kind() {
			return c, false, nil
		}
		c.verb = "ALTER"
	case sc.keywords("DROP"):
		if !kind() {
			return c, false, nil
		}
		c.verb = "DROP"
	case sc.keywords("EXECUTE", "TASK"):
		c.verb, c.kind = "EXECUTE", tasks.KindTask
	case sc.keywords("EXECUTE", "ALERT"):
		c.verb, c.kind = "EXECUTE", tasks.KindAlert
	case sc.keywords("REFRESH", "MATERIALIZED", "VIEW"):
		c.verb, c.kind = "REFRESH", tasks.KindMaterializedView
	case sc.keywords("SHOW"):
		if !plural() {
			return c, false, nil
		}
		c.verb = "SHOW"
		if tasks.Refreshed(c.kind) {
			if sc.keywords("LIKE") {
				if c.like, ok = sc.literal(); !ok || c.like == nil {
					return c, true, jobSyntax("LIKE takes a string, such as LIKE 'daily%%'")
				}
			}
			if sc.keywords("IN") {
				switch {
				case sc.keywords("ACCOUNT"):
				case sc.keywords("DATABASE"):
					sc.name()
				case sc.keywords("SCHEMA"):
					name, ok := sc.name()
					if !ok {
						return c, true, jobSyntax("IN SCHEMA needs a schema")
					}
					c.schema = name[len(name)-1]
				default:
					return c, true, jobSyntax("expected IN ACCOUNT, IN DATABASE or IN SCHEMA after SHOW %s", c.plural())
				}
			}
		}
		if !sc.done() {
			return c, true, jobSyntax("unexpected text after SHOW %s", c.plural())
		}
		return c, true, nil
	default:
		return c, false, nil
	}

	what := c.verb + " " + strings.ToUpper(c.kind)
	switch c.verb {
	case "CREATE":
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
	case "ALTER", "DROP":
		c.ifExists = sc.keywords("IF", "EXISTS")
	}
	if c.name, ok = sc.name(); !ok {
		return c, true, jobSyntax("%s needs a name", what)
	}
	if !tasks.Refreshed(c.kind) && len(c.name) != 1 {
		return c, true, jobSyntax("%s names are not qualified: %s", c.kind, strings.Join(c.name, "."))
	}

	scheduleClause := func() error {
		sc.punct('=')
		spec, ok := sc.literal()
		if !ok || spec == nil {
			return jobSyntax("SCHEDULE takes a string, such as SCHEDULE '5 minutes' or SCHEDULE '0 6 * * *'")
		}
		c.schedule = spec
		if sc.keywords("TIMEZONE") {
			sc.punct('=')
			tz, ok := sc.literal()
			if !ok || tz == nil {
				return jobSyntax("TIMEZONE takes a string, such as TIMEZONE 'America/New_York'")
			}
			c.timezone = tz
		}
		return nil
	}
	switch c.verb {
	case "CREATE":
		if c.kind == tasks.KindDynamicTable {
			if err := c.dynamicOptions(sc, false); err != nil {
				return c, true, err
			}
			if c.lag == nil {
				return c, true, jobSyntax("CREATE DYNAMIC TABLE needs a TARGET_LAG, such as TARGET_LAG = '5 minutes' or TARGET_LAG = DOWNSTREAM")
			}
		} else if sc.keywords("SCHEDULE") {
			if err := scheduleClause(); err != nil {
				return c, true, err
			}
		}
		if c.kind == tasks.KindAlert {
			// As in Snowflake, an alert's IF follows its schedule.
			if c.schedule == nil {
				return c, true, jobSyntax("CREATE ALERT needs a SCHEDULE, such as SCHEDULE '1 hour' or SCHEDULE '0 9 * * *'")
			}
			if sc.word() != "IF" {
				return c, true, jobSyntax("expected SCHEDULE '...' [TIMEZONE '...'] IF (EXISTS (query)) THEN NOTIFY ... after CREATE ALERT %s",
					strings.Join(c.name, "."))
			}
			c.body = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
			return c, true, nil
		}
		if !sc.keywords("AS") {
			if c.kind == tasks.KindDynamicTable {
				return c, true, jobSyntax("expected TARGET_LAG = '...' [WAREHOUSE = ...] [REFRESH_MODE = ...] [INITIALIZE = ...] AS ... after %s %s",
					what, strings.Join(c.name, "."))
			}
			return c, true, jobSyntax("expected [SCHEDULE '...' [TIMEZONE '...']] AS ... after %s %s", what, strings.Join(c.name, "."))
		}
		c.body = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
		if c.body == "" {
			return c, true, jobSyntax("%s needs a statement after AS", what)
		}
		if c.kind == tasks.KindTask && c.schedule == nil {
			return c, true, jobSyntax("CREATE TASK needs a SCHEDULE, such as SCHEDULE '1 hour' or SCHEDULE '0 6 * * *'")
		}
		return c, true, nil
	case "ALTER":
		switch {
		case sc.keywords("RESUME"):
			c.action = "RESUME"
		case sc.keywords("SUSPEND"):
			c.action = "SUSPEND"
		case c.kind == tasks.KindDynamicTable && sc.keywords("REFRESH"):
			c.verb = "REFRESH"
		case c.kind == tasks.KindDynamicTable && sc.keywords("SET"):
			c.action = "LAG"
			if err := c.dynamicOptions(sc, true); err != nil {
				return c, true, err
			}
			if c.lag == nil && c.warehouse == nil {
				return c, true, jobSyntax("expected TARGET_LAG = '...' or WAREHOUSE = ... after ALTER DYNAMIC TABLE %s SET", strings.Join(c.name, "."))
			}
		case c.kind == tasks.KindDynamicTable:
			return c, true, jobSyntax("expected RESUME, SUSPEND, REFRESH or SET TARGET_LAG after %s %s", what, strings.Join(c.name, "."))
		case sc.keywords("SET", "SCHEDULE"):
			c.action = "SCHEDULE"
			if err := scheduleClause(); err != nil {
				return c, true, err
			}
		default:
			return c, true, jobSyntax("expected RESUME, SUSPEND or SET SCHEDULE after %s %s", what, strings.Join(c.name, "."))
		}
	}
	if !sc.done() {
		return c, true, jobSyntax("unexpected text after %s %s: %q", what, strings.Join(c.name, "."), strings.TrimSpace(sc.rest()))
	}
	return c, true, nil
}

func jobSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// dynamicOptions reads a dynamic table's options, in any order: TARGET_LAG,
// WAREHOUSE, REFRESH_MODE and INITIALIZE, and Snowflake's options Igloo
// accepts and ignores, with a notice. alter says they follow ALTER ...
// SET, which takes TARGET_LAG and WAREHOUSE.
func (c *jobCommand) dynamicOptions(sc *scanner, alter bool) error {
	for {
		switch w := sc.word(); {
		case w == "TARGET_LAG":
			sc.keywords(w)
			sc.punct('=')
			if sc.keywords("DOWNSTREAM") {
				v := "DOWNSTREAM"
				c.lag = &v
				continue
			}
			lag, ok := sc.literal()
			if !ok || lag == nil {
				return jobSyntax("TARGET_LAG takes a string, such as TARGET_LAG = '5 minutes', or DOWNSTREAM")
			}
			c.lag = lag
		case w == "WAREHOUSE":
			sc.keywords(w)
			sc.punct('=')
			name, ok := sc.name()
			if !ok {
				return jobSyntax("WAREHOUSE takes a name")
			}
			v := strings.Join(name, ".")
			c.warehouse = &v
			c.notices = append(c.notices, "WAREHOUSE is ignored: Igloo has no warehouses, and refreshes run on the replica that holds the job "+
				"(see docs/workload-management.md for compute pools)")
		case !alter && w == "REFRESH_MODE":
			sc.keywords(w)
			sc.punct('=')
			switch m := sc.word(); m {
			case refreshAuto, refreshFull, refreshIncremental:
				sc.keywords(m)
				c.refreshMode = m
			default:
				return jobSyntax("REFRESH_MODE is AUTO, FULL or INCREMENTAL")
			}
		case !alter && w == "INITIALIZE":
			sc.keywords(w)
			sc.punct('=')
			switch m := sc.word(); m {
			case "ON_CREATE", "ON_SCHEDULE":
				sc.keywords(m)
				c.initialize = m
			default:
				return jobSyntax("INITIALIZE is ON_CREATE or ON_SCHEDULE")
			}
		case !alter && w == "COMMENT":
			sc.keywords(w)
			sc.punct('=')
			if _, ok := sc.literal(); !ok {
				return jobSyntax("COMMENT takes a string")
			}
			c.notices = append(c.notices, "COMMENT is ignored: use COMMENT ON TABLE")
		case w == "DATA_RETENTION_TIME_IN_DAYS" || w == "MAX_DATA_EXTENSION_TIME_IN_DAYS":
			sc.keywords(w)
			sc.punct('=')
			sc.skip()
			j := sc.i
			for j < len(sc.s) && sc.s[j] >= '0' && sc.s[j] <= '9' {
				j++
			}
			if j == sc.i {
				return jobSyntax("%s takes a number of days", w)
			}
			sc.i = j
			c.notices = append(c.notices, w+" is ignored: Igloo keeps snapshots as table maintenance says (see docs/operations.md)")
		default:
			return nil
		}
	}
}

func (c jobCommand) plural() string {
	switch c.kind {
	case tasks.KindMaterializedView:
		return "MATERIALIZED VIEWS"
	case tasks.KindDynamicTable:
		return "DYNAMIC TABLES"
	}
	return strings.ToUpper(c.kind) + "S"
}

// tag is the command tag of a successful c.
func (c jobCommand) tag() string {
	switch c.verb {
	case "EXECUTE":
		return "EXECUTE " + strings.ToUpper(c.kind)
	case "REFRESH":
		if c.kind == tasks.KindDynamicTable {
			return "ALTER DYNAMIC TABLE"
		}
		return "REFRESH MATERIALIZED VIEW"
	}
	return c.verb + " " + strings.ToUpper(c.kind)
}

// mvColumns describe SHOW MATERIALIZED VIEWS: jobColumns, and how it is
// refreshed.
var mvColumns = append(slices.Clone(jobColumns),
	wire.Column{Name: "refresh_mode", Oid: pgtype.TextOID, Width: -1},
	wire.Column{Name: "refresh_mode_reason", Oid: pgtype.TextOID, Width: -1},
	wire.Column{Name: "last_refresh_action", Oid: pgtype.TextOID, Width: -1},
	wire.Column{Name: "last_refresh_detail", Oid: pgtype.TextOID, Width: -1},
	wire.Column{Name: "data_timestamp", Oid: pgtype.TimestamptzOID, Width: 8},
)

// dynamicColumns describe SHOW DYNAMIC TABLES, with Snowflake's names
// where Snowflake has the column.
var dynamicColumns = wire.Columns{
	{Name: "created_on", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "database_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "schema_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "owner", Oid: pgtype.TextOID, Width: -1},
	{Name: "target_lag", Oid: pgtype.TextOID, Width: -1},
	{Name: "refresh_mode", Oid: pgtype.TextOID, Width: -1},
	{Name: "refresh_mode_reason", Oid: pgtype.TextOID, Width: -1},
	{Name: "warehouse", Oid: pgtype.TextOID, Width: -1},
	{Name: "text", Oid: pgtype.TextOID, Width: -1},
	{Name: "scheduling_state", Oid: pgtype.TextOID, Width: -1},
	{Name: "data_timestamp", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "next_run", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "last_run", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "last_outcome", Oid: pgtype.TextOID, Width: -1},
	{Name: "last_error", Oid: pgtype.TextOID, Width: -1},
	{Name: "last_refresh_action", Oid: pgtype.TextOID, Width: -1},
	{Name: "last_refresh_detail", Oid: pgtype.TextOID, Width: -1},
}

// jobColumns describe SHOW TASKS, PIPES and ALERTS.
var jobColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "owner", Oid: pgtype.TextOID, Width: -1},
	{Name: "state", Oid: pgtype.TextOID, Width: -1},
	{Name: "schedule", Oid: pgtype.TextOID, Width: -1},
	{Name: "timezone", Oid: pgtype.TextOID, Width: -1},
	{Name: "next_run", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "last_run", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "last_outcome", Oid: pgtype.TextOID, Width: -1},
	{Name: "last_error", Oid: pgtype.TextOID, Width: -1},
	{Name: "definition", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

// errNotAlone refuses the statements loadingCommand carries out when sent
// with others.
var errNotAlone = pgerror.WithCode(errors.New("send COPY INTO, and statements on tasks, pipes and materialized views, on their own"),
	codes.FeatureNotSupported)

// isLoadingCommand reports whether loadingCommand carries out query.
func isLoadingCommand(query string) bool {
	_, isCopy, _ := parseCopyInto(query)
	_, isJob, _ := parseJobCommand(query)
	return isCopy || isJob
}

// loadingCommand carries out COPY INTO and the statements on tasks, pipes
// and materialized views. ok is false for every other statement.
func (s *Server) loadingCommand(sess *session, query string) (wire.PreparedStatements, bool, error) {
	cp, isCopy, err := parseCopyInto(query)
	var job jobCommand
	isJob := false
	if !isCopy {
		job, isJob, err = parseJobCommand(query)
	}
	if !isCopy && !isJob {
		return nil, false, nil
	}
	if len(sqllex.Split(query)) != 1 {
		err = errNotAlone
	}
	if err != nil {
		s.metrics.query(time.Now(), err)
		s.recordQuery(sess, queryRecord{text: query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	if isCopy {
		return s.copyIntoStatement(sess, query, cp), true, nil
	}
	var opts []wire.PreparedOptionFn
	if job.verb == "SHOW" {
		switch job.kind {
		case tasks.KindMaterializedView:
			opts = append(opts, wire.WithColumns(mvColumns))
		case tasks.KindDynamicTable:
			opts = append(opts, wire.WithColumns(dynamicColumns))
		default:
			opts = append(opts, wire.WithColumns(jobColumns))
		}
	}
	return withNotices(wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		tw := &tagWriter{DataWriter: w}
		// What the command read and wrote, for the history.
		var rec queryRecord
		user, err := s.refreshUser(ctx, sess)
		if err == nil {
			err = sess.runCancelable(ctx, query, func(ctx context.Context) error {
				return s.runJobCommand(ctx, sess, user, job, tw, &rec)
			})
		}
		s.metrics.query(start, err)
		if err != nil {
			s.log.Debug("statement failed", "user", user.Name, "err", err)
		}
		s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: tw.rows(),
			written: rec.written, analyze: rec.analyze, stmtType: rec.stmtType})
		return pgError(err)
	}, opts...)), job.notices), true, nil
}

// runJobCommand carries out c. A command that creates a materialized view
// fills in rec what creating it read and wrote.
func (s *Server) runJobCommand(ctx context.Context, sess *session, user auth.User, c jobCommand, w wire.DataWriter, rec *queryRecord) error {
	if c.verb == "SHOW" {
		return s.showJobs(ctx, user, c, w)
	}
	if err := s.inNoTransaction(sess, c.tag()); err != nil {
		return err
	}
	name, err := s.jobName(ctx, sess, c)
	if err != nil {
		return err
	}
	if c.verb == "CREATE" {
		if err := s.createJob(ctx, sess, user, c, name, rec); err != nil {
			return err
		}
		s.tasks.Kick()
		return w.Complete(c.tag())
	}

	store := s.tasks.Store()
	j, err := store.Job(ctx, c.kind, name)
	if errors.Is(err, tasks.ErrNotFound) {
		if c.ifExists {
			return w.Complete(c.tag())
		}
		return jobMissing(c, name)
	}
	if err != nil {
		return err
	}
	if j.Owner != user.Name && user.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s %s belongs to %s; only its owner or an admin may %s it",
			c.kind, name, j.Owner, strings.ToLower(c.verb)), codes.InsufficientPrivilege)
	}

	switch c.verb {
	case "ALTER":
		err = s.alterJob(ctx, user, c, j)
	case "DROP":
		err = s.dropJob(ctx, sess, user, j)
	case "EXECUTE", "REFRESH":
		_, err = s.tasks.RunNow(ctx, s, j)
		switch {
		case errors.Is(err, tasks.ErrRunning):
			err = pgerror.WithCode(err, codes.ObjectInUse)
		case errors.Is(err, tasks.ErrSkipped):
			err = pgerror.WithCode(err, codes.ObjectNotInPrerequisiteState)
		}
	}
	if err != nil {
		return err
	}
	s.tasks.Kick()
	return w.Complete(c.tag())
}

// disownJobs suspends the tasks, pipes, materialized views and alerts of a
// dropped user, and marks them as a dropped user's, "name (dropped)": a
// user created later with the same name does not get them. Admins see
// them, and may drop them, or CREATE OR REPLACE them as their own.
func (s *Server) disownJobs(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.tasks.Store().DisownJobs(ctx, name, name+" (dropped)"); err != nil {
		s.log.Warn("could not suspend a dropped user's scheduled work", "user", name, "err", err)
		return
	}
	s.tasks.Kick()
}

// jobName is the name c's job is stored under: a task's or pipe's name,
// or a materialized view's or dynamic table's schema.table in the catalog.
func (s *Server) jobName(ctx context.Context, sess *session, c jobCommand) (string, error) {
	if !tasks.Refreshed(c.kind) {
		name := c.name[0]
		if !config.ValidName(name) {
			return "", pgerror.WithCode(fmt.Errorf("invalid %s name %q: use lowercase letters, digits and underscores", c.kind, name),
				codes.InvalidName)
		}
		return name, nil
	}
	if c.verb == "CREATE" {
		o, err := s.newTableTarget(ctx, sess, c.name)
		if errors.Is(err, errNoGrantPath) {
			err = pgerror.WithCode(fmt.Errorf("%ss are tables of the %s catalog; %s is not one",
				c.kind, s.catalogName(), strings.Join(c.name, ".")), codes.FeatureNotSupported)
		}
		if err != nil {
			return "", err
		}
		return o.String(), nil
	}
	o, exists, err := s.resolveLoadTarget(ctx, sess, c.name)
	if err != nil || !exists {
		// Perhaps its table is gone: look for it by the name it would have.
		if o, err := s.newTableTarget(ctx, sess, c.name); err == nil {
			return o.String(), nil
		}
		return strings.Join(c.name, "."), nil
	}
	return o.String(), nil
}

// dropJob drops j, and a materialized view's or dynamic table's table and
// state with it.
func (s *Server) dropJob(ctx context.Context, sess *session, user auth.User, j tasks.Job) error {
	var err error
	if tasks.Refreshed(j.Kind) {
		schema, table, _ := strings.Cut(j.Name, ".")
		err = s.execDDL(ctx, sess, user, "DROP TABLE IF EXISTS "+quoteName([]string{s.catalogName(), schema, table}))
		if err == nil {
			s.dropViewState(ctx, sess, j)
		}
	}
	if err == nil {
		if err = s.tasks.Store().DropJob(ctx, j.Kind, j.Name); errors.Is(err, tasks.ErrNotFound) {
			err = nil // dropping the table dropped it
		}
	}
	if err == nil {
		s.log.Info("job changed", "action", "drop", "kind", j.Kind, "name", j.Name, "by", user.Name)
	}
	return err
}

func jobMissing(c jobCommand, name string) error {
	code := codes.UndefinedObject
	if tasks.Refreshed(c.kind) {
		code = codes.UndefinedTable
	}
	return pgerror.WithCode(fmt.Errorf("%s %s does not exist", c.kind, name), code)
}

// needsWriter refuses to schedule work for users without the writer role.
func needsWriter(user auth.User, what string) error {
	if rank[user.Role] >= rank[auth.RoleWriter] {
		return nil
	}
	return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the writer role", what), codes.InsufficientPrivilege)
}

// parseSchedule reads a schedule and works out when it first runs, from
// now.
func parseSchedule(spec, tz string, now time.Time) (schedule.Schedule, time.Time, error) {
	sched, err := schedule.Parse(spec, tz)
	if err != nil {
		return sched, time.Time{}, pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	first := sched.First(now)
	if schedule.Never(first) {
		return sched, first, pgerror.WithCode(fmt.Errorf("the schedule %q never runs", spec), codes.InvalidParameterValue)
	}
	return sched, first, nil
}

func (s *Server) createJob(ctx context.Context, sess *session, user auth.User, c jobCommand, name string, rec *queryRecord) error {
	store := s.tasks.Store()
	j := tasks.Job{Kind: c.kind, Name: name, Owner: user.Name, State: tasks.StateStarted}
	if c.schedule != nil || c.kind != tasks.KindMaterializedView {
		what := "CREATE " + strings.ToUpper(c.kind)
		if c.kind == tasks.KindMaterializedView {
			what += " with a SCHEDULE"
		}
		if err := needsWriter(user, what); err != nil {
			return err
		}
	}
	var state refreshState
	if c.kind == tasks.KindDynamicTable {
		state = refreshState{Mode: refreshAuto, Lag: *c.lag}
		if c.refreshMode != "" {
			state.Mode = c.refreshMode
		}
		if c.warehouse != nil {
			state.Warehouse = *c.warehouse
		}
		spec, err := lagSchedule(*c.lag)
		if err != nil {
			return err
		}
		if spec != "" {
			c.schedule = &spec
		}
	}
	if c.schedule != nil {
		tz := ""
		if c.timezone != nil {
			tz = *c.timezone
		}
		sched, first, err := parseSchedule(*c.schedule, tz, time.Now())
		if err != nil {
			return err
		}
		if c.kind == tasks.KindAlert {
			if err := checkAlertSchedule(sched); err != nil {
				return err
			}
		}
		j.Schedule, j.Timezone, j.NextRun = sched.String(), tz, first
	}

	existing, err := store.Job(ctx, c.kind, name)
	if tasks.Refreshed(c.kind) && errors.Is(err, tasks.ErrNotFound) {
		// A materialized view and a dynamic table are tables: one name, one
		// table.
		other := tasks.KindDynamicTable
		if c.kind == tasks.KindDynamicTable {
			other = tasks.KindMaterializedView
		}
		if o, oerr := store.Job(ctx, other, name); oerr == nil {
			if !c.replace {
				return pgerror.WithCode(fmt.Errorf("%s is a %s", name, other), codes.DuplicateRelation)
			}
			if o.Owner != user.Name && user.Role != auth.RoleAdmin {
				return pgerror.WithCode(fmt.Errorf("permission denied: %s %s belongs to %s; only its owner or an admin may replace it",
					other, name, o.Owner), codes.InsufficientPrivilege)
			}
			defer func() {
				if err := store.DropJob(context.WithoutCancel(ctx), other, name); err != nil && !errors.Is(err, tasks.ErrNotFound) {
					s.log.Warn("could not drop the job a table replaced", "kind", other, "name", name, "err", err)
				}
				s.dropViewState(ctx, sess, o)
			}()
		}
	}
	switch {
	case err == nil && c.ifNotExists:
		return nil
	case err == nil && !c.replace:
		return pgerror.WithCode(fmt.Errorf("%s %s already exists", c.kind, name), codes.DuplicateObject)
	case err == nil && existing.Owner != user.Name && user.Role != auth.RoleAdmin:
		return pgerror.WithCode(fmt.Errorf("permission denied: %s %s belongs to %s; only its owner or an admin may replace it",
			c.kind, name, existing.Owner), codes.InsufficientPrivilege)
	case err != nil && !errors.Is(err, tasks.ErrNotFound):
		return err
	}

	switch c.kind {
	case tasks.KindTask:
		// As in Snowflake, a new task waits for ALTER TASK ... RESUME.
		j.State, j.NextRun = tasks.StateSuspended, time.Time{}
		if n := len(sqllex.Split(c.body)); n != 1 {
			return pgerror.WithCode(errors.New("a task runs one SQL statement"), codes.FeatureNotSupported)
		}
		if inner, isJob, _ := parseJobCommand(c.body); isJob && inner.kind != tasks.KindMaterializedView {
			return pgerror.WithCode(errors.New("a task cannot create, change, run or drop tasks, pipes and alerts"), codes.FeatureNotSupported)
		}
		j.Statement = c.body
	case tasks.KindPipe:
		cp, isCopy, err := parseCopyInto(c.body)
		if err != nil {
			return err
		}
		if !isCopy {
			return pgerror.WithCode(errors.New("a pipe runs COPY INTO: CREATE PIPE name AS COPY INTO table FROM 'location'"), codes.Syntax)
		}
		if err := s.checkLocation(cp.location); err != nil {
			return err
		}
		if err := s.checkLoadLocation(ctx, user, cp.location); err != nil {
			return err
		}
		target, exists, err := s.resolveLoadTarget(ctx, sess, cp.target)
		if err != nil {
			return err
		}
		if !exists && !cp.createTable {
			return pgerror.WithCode(fmt.Errorf("table %s does not exist", strings.Join(cp.target, ".")), codes.UndefinedTable)
		}
		cp.target = []string{s.catalogName(), target.Schema, target.Table}
		j.Statement = cp.String()
		if c.schedule == nil {
			j.Schedule = pipeSchedule
		}
		// A new pipe looks for files at once.
		j.NextRun = time.Now()
	case tasks.KindAlert:
		// As in Snowflake, a new alert waits for ALTER ALERT ... RESUME.
		j.State, j.NextRun = tasks.StateSuspended, time.Time{}
		def, err := parseAlert(c.body)
		if err != nil {
			return err
		}
		if err := s.checkAlert(ctx, sess, user, name, def); err != nil {
			return err
		}
		j.Statement = def.encode()
	case tasks.KindMaterializedView, tasks.KindDynamicTable:
		if n, err := s.engine.CountStatements(c.body); err != nil || n != 1 || !selectLike(c.body) {
			return pgerror.WithCode(fmt.Errorf("a %s is defined by one query: CREATE %s name AS SELECT ...", c.kind, strings.ToUpper(c.kind)),
				codes.Syntax)
		}
		j.Statement = c.body
		// Whether it can be refreshed incrementally, as SHOW says, and as
		// REFRESH_MODE = INCREMENTAL requires. A query that cannot be read
		// fails below, as creating its table does.
		if reason, err := s.refreshReason(ctx, sess, user, c.body); err == nil && reason == "" {
			state.Incremental = true
		} else if err == nil {
			state.Reason = reason
			if state.Mode == refreshIncremental {
				return pgerror.WithCode(fmt.Errorf("%s %s cannot be refreshed incrementally: %s; use REFRESH_MODE = AUTO or FULL", c.kind, name, reason),
					codes.FeatureNotSupported)
			}
		}
		if c.kind == tasks.KindMaterializedView {
			state.Mode = refreshAuto
		}
		j.Refresh = state.encode()
		schema, table, _ := strings.Cut(name, ".")
		create := "CREATE TABLE "
		if c.replace {
			create = "CREATE OR REPLACE TABLE "
		}
		body := c.body
		if c.initialize == "ON_SCHEDULE" {
			// Its first refresh fills it.
			body = "SELECT * FROM (" + c.body + ") AS initialize WHERE false"
		}
		create += quoteName([]string{s.catalogName(), schema, table}) + " AS " + body
		if err := s.execDDL(ctx, sess, user, create); err != nil {
			return err
		}
		rec.written = []auth.Object{{Schema: schema, Table: table}}
		rec.analyze, rec.stmtType = s.rewrite(ctx, sess, user, create), duckdb.STATEMENT_TYPE_CREATE
		if err == nil && existing.Refresh != "" {
			// The state of what it replaced.
			s.dropViewState(ctx, sess, existing)
		}
	}
	// A materialized view's or dynamic table's table is in place by now,
	// so its definition goes with it, whatever was there.
	err = store.CreateJob(ctx, j, c.replace || tasks.Refreshed(c.kind))
	if errors.Is(err, tasks.ErrExists) {
		return pgerror.WithCode(fmt.Errorf("%s %s already exists", c.kind, name), codes.DuplicateObject)
	}
	if err != nil {
		return err
	}
	s.log.Info("job changed", "action", "create", "kind", c.kind, "name", name, "by", user.Name)
	return nil
}

func (s *Server) alterJob(ctx context.Context, user auth.User, c jobCommand, j tasks.Job) error {
	store := s.tasks.Store()
	now := time.Now()
	switch c.action {
	case "SUSPEND":
		if err := store.SetState(ctx, j.Kind, j.Name, tasks.StateSuspended, time.Time{}); err != nil {
			return err
		}
	case "RESUME":
		if err := needsWriter(user, "ALTER "+strings.ToUpper(c.kind)+" ... RESUME"); err != nil {
			return err
		}
		if j.Schedule == "" && j.Kind == tasks.KindDynamicTable {
			// Its TARGET_LAG is DOWNSTREAM: it refreshes before the dynamic
			// tables that read it do, while it is resumed.
			if err := store.SetState(ctx, j.Kind, j.Name, tasks.StateStarted, time.Time{}); err != nil {
				return err
			}
			break
		}
		if j.Schedule == "" {
			return pgerror.WithCode(fmt.Errorf("%s %s has no schedule; give it one with SET SCHEDULE", c.kind, j.Name),
				codes.ObjectNotInPrerequisiteState)
		}
		if j.State == tasks.StateStarted && !j.NextRun.IsZero() {
			return nil
		}
		_, next, err := parseSchedule(j.Schedule, j.Timezone, now)
		if err != nil {
			return err
		}
		if j.Kind == tasks.KindPipe {
			next = now
		}
		if err := store.SetState(ctx, j.Kind, j.Name, tasks.StateStarted, next); err != nil {
			return err
		}
	case "LAG":
		if err := needsWriter(user, "ALTER DYNAMIC TABLE ... SET"); err != nil {
			return err
		}
		st := decodeRefresh(j.Refresh)
		if c.warehouse != nil {
			st.Warehouse = *c.warehouse
		}
		if c.lag != nil {
			spec, err := lagSchedule(*c.lag)
			if err != nil {
				return err
			}
			st.Lag = *c.lag
			next := time.Time{}
			if spec != "" && j.State == tasks.StateStarted {
				if _, next, err = parseSchedule(spec, "", now); err != nil {
					return err
				}
			}
			if err := store.SetSchedule(ctx, j.Kind, j.Name, spec, "", next); err != nil {
				return err
			}
		}
		if err := store.SetRefresh(ctx, j.Kind, j.Name, st.encode()); err != nil {
			return err
		}
	case "SCHEDULE":
		if err := needsWriter(user, "ALTER "+strings.ToUpper(c.kind)+" ... SET SCHEDULE"); err != nil {
			return err
		}
		tz := ""
		if c.timezone != nil {
			tz = *c.timezone
		}
		sched, next, err := parseSchedule(*c.schedule, tz, now)
		if err != nil {
			return err
		}
		if j.Kind == tasks.KindAlert {
			if err := checkAlertSchedule(sched); err != nil {
				return err
			}
		}
		if j.State != tasks.StateStarted {
			next = time.Time{}
		}
		if err := store.SetSchedule(ctx, j.Kind, j.Name, sched.String(), tz, next); err != nil {
			return err
		}
	}
	s.log.Info("job changed", "action", strings.ToLower(c.action), "kind", c.kind, "name", j.Name, "by", user.Name)
	return nil
}

// showJobs lists jobs of c's kind: all of them for admins, a user's own
// otherwise.
func (s *Server) showJobs(ctx context.Context, user auth.User, c jobCommand, w wire.DataWriter) error {
	jobs, err := s.tasks.Store().Jobs(ctx, c.kind)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Owner != user.Name && user.Role != auth.RoleAdmin {
			continue
		}
		if tasks.Refreshed(j.Kind) {
			if err := s.showView(c, j, w); err != nil {
				return err
			}
			continue
		}
		definition := j.Statement
		if j.Kind == tasks.KindAlert {
			definition = alertDefinition(j.Statement)
		}
		row := []any{j.Name, j.Owner, j.State, nullText(j.Schedule), nullText(j.Timezone), nullTime(j.NextRun),
			nil, nil, nil, definition, j.CreatedAt}
		if j.Schedule != "" && j.Timezone == "" {
			row[4] = "UTC"
		}
		if r := j.LastRun; r != nil {
			row[6], row[7], row[8] = r.Start, r.Outcome, nullText(r.Error)
		}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// showView writes the row of SHOW MATERIALIZED VIEWS or SHOW DYNAMIC
// TABLES for j, if it matches c's LIKE and IN SCHEMA.
func (s *Server) showView(c jobCommand, j tasks.Job, w wire.DataWriter) error {
	st := decodeRefresh(j.Refresh)
	schema, table, _ := strings.Cut(j.Name, ".")
	if c.schema != "" && !strings.EqualFold(c.schema, schema) {
		return nil
	}
	if c.like != nil && !likeMatch(strings.ToLower(*c.like), table) {
		return nil
	}
	reason := any(nil)
	if !st.Incremental && st.Reason != "" {
		reason = st.Reason
	}
	mode := st.Mode
	if mode == refreshAuto {
		// What AUTO comes to, as Snowflake shows it.
		mode = refreshFull
		if st.Incremental {
			mode = refreshIncremental
		}
	}
	var lastRun, lastOutcome, lastError any
	if r := j.LastRun; r != nil {
		lastRun, lastOutcome, lastError = r.Start, r.Outcome, nullText(r.Error)
	}
	if j.Kind == tasks.KindDynamicTable {
		scheduling := "RUNNING"
		if j.State != tasks.StateStarted {
			scheduling = "SUSPENDED"
		}
		return w.Row([]any{j.CreatedAt, table, s.catalogName(), schema, j.Owner, st.Lag, mode, reason, nullText(st.Warehouse), j.Statement,
			scheduling, nullTime(st.DataTimestamp), nullTime(j.NextRun), lastRun, lastOutcome, lastError, nullText(st.Action), nullText(st.Detail)})
	}
	row := []any{j.Name, j.Owner, j.State, nullText(j.Schedule), nullText(j.Timezone), nullTime(j.NextRun),
		lastRun, lastOutcome, lastError, j.Statement, j.CreatedAt, mode, reason, nullText(st.Action), nullText(st.Detail), nullTime(st.DataTimestamp)}
	if j.Schedule != "" && j.Timezone == "" {
		row[4] = "UTC"
	}
	return w.Row(row)
}

// lagSchedule turns a dynamic table's TARGET_LAG into the schedule of its
// refreshes: every half of the lag, at least every 30 seconds apart, so
// that its data stays within the lag while a refresh takes less than half
// of it. DOWNSTREAM has none: the table refreshes before those that read
// it.
func lagSchedule(lag string) (string, error) {
	if strings.EqualFold(lag, "DOWNSTREAM") {
		return "", nil
	}
	fields := strings.Fields(strings.ToLower(lag))
	bad := pgerror.WithCode(fmt.Errorf("TARGET_LAG %q is not a lag such as '90 seconds', '5 minutes', '2 hours' or '1 day', or DOWNSTREAM", lag),
		codes.InvalidParameterValue)
	if len(fields) != 2 {
		return "", bad
	}
	n, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || n <= 0 {
		return "", bad
	}
	var unit time.Duration
	switch strings.TrimSuffix(fields[1], "s") {
	case "second":
		unit = time.Second
	case "minute":
		unit = time.Minute
	case "hour":
		unit = time.Hour
	case "day":
		unit = 24 * time.Hour
	default:
		return "", bad
	}
	if n > int64(366*24*time.Hour/unit) {
		return "", bad
	}
	d := time.Duration(n) * unit
	if d < time.Minute {
		return "", pgerror.WithCode(fmt.Errorf("TARGET_LAG %q is under a minute, the shortest lag", lag), codes.InvalidParameterValue)
	}
	return strconv.FormatInt(int64(max(d/2, 30*time.Second)/time.Second), 10) + " seconds", nil
}

func nullText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// RunJob runs j as its owner, now, with the privileges the owner has now,
// in a new session of theirs; it implements tasks.Runner. A task's
// statement runs as if the owner sent it. A pipe's COPY INTO and a
// materialized view's refresh are recorded in the owner's query history
// when they change anything.
func (s *Server) RunJob(ctx context.Context, j tasks.Job) (int64, error) {
	if j.Kind == erasure.JobKind {
		// An erasure request is carried out whatever became of whoever
		// made it: the obligation stays.
		if s.erasure == nil {
			return -1, errors.New("erasure requests need an Iceberg catalog (IGLOO_CATALOG_URI)")
		}
		return -1, s.erasure.Run(ctx, j.Name)
	}
	if j.Kind == tasks.KindAlert {
		return s.runAlert(ctx, j)
	}
	if err := s.stillDue(ctx, j); err != nil {
		return -1, err
	}
	owner, err := s.users.Get(ctx, j.Owner)
	if errors.Is(err, auth.ErrNotFound) {
		return -1, fmt.Errorf("%w: %s", tasks.ErrOwnerGone, j.Owner)
	}
	if err != nil {
		return -1, err
	}
	// A disabled user's jobs fail until they are enabled again, rather
	// than act for someone who may no longer sign in.
	if owner.Disabled {
		return -1, pgerror.WithCode(fmt.Errorf("the owner %s is disabled (ALTER USER %s LOGIN enables them)", owner.Name, owner.Name),
			codes.ObjectNotInPrerequisiteState)
	}
	es, err := s.OpenEmbedded(ctx, owner, j.Kind+" "+j.Name)
	if err != nil {
		return -1, err
	}
	defer es.Close() //nolint:errcheck
	sess := es.sess
	sess.method, sess.job = methodTask, j.Kind+" "+j.Name

	switch j.Kind {
	case tasks.KindTask:
		out := &taskResult{}
		if err := es.Run(ctx, j.Statement, out); err != nil {
			return -1, err
		}
		if out.took {
			return out.wrote, nil
		}
		if n := tagRows(out.tag); n >= 0 {
			return n, nil
		}
		return out.rows, nil

	case tasks.KindPipe:
		cp, _, err := parseCopyInto(j.Statement)
		if err != nil {
			return -1, err
		}
		start := time.Now()
		var loads []tasks.Load
		// COPY INTO reaches the catalog's in-memory set (see
		// copyIntoStatement); hold the exclusive side.
		releaseCatalog := s.holdCatalog(sess)
		err = sess.runCancelable(ctx, j.Statement, func(ctx context.Context) error {
			loads, _, err = s.copyInto(ctx, sess, owner, cp, j.Name)
			return err
		})
		releaseCatalog()
		if err == nil && len(loads) == 0 {
			return 0, tasks.ErrIdle
		}
		var rows int64
		failed := 0
		var first string
		for _, l := range loads {
			rows += l.Rows
			if l.Status == tasks.LoadFailed {
				failed++
				if first == "" {
					first = l.File + ": " + l.FirstError
				}
			}
		}
		if err == nil && failed > 0 {
			err = fmt.Errorf("%d of %d files failed to load (see igloo.load_history); the first: %s", failed, len(loads), first)
		}
		target := cp.target[1:]
		written := []auth.Object{{Schema: target[0], Table: target[1]}}
		if rows > 0 {
			s.metadataChanged(written)
		}
		s.recordQuery(sess, queryRecord{text: j.Statement, start: start, err: err, rows: rows,
			written: written, locations: []string{cp.location}})
		return rows, err

	case tasks.KindMaterializedView, tasks.KindDynamicTable:
		if j.Kind == tasks.KindDynamicTable {
			if err := s.refreshUpstream(ctx, sess, j); err != nil {
				return -1, err
			}
		}
		start := time.Now()
		var res refreshResult
		schema, table, _ := strings.Cut(j.Name, ".")
		text := "REFRESH MATERIALIZED VIEW " + quoteName([]string{schema, table})
		if j.Kind == tasks.KindDynamicTable {
			text = "ALTER DYNAMIC TABLE " + quoteName([]string{schema, table}) + " REFRESH"
		}
		err := sess.runCancelable(ctx, text, func(ctx context.Context) error {
			var err error
			res, err = s.refreshView(ctx, sess, owner, j)
			return err
		})
		rows := res.rows
		if err != nil {
			rows = -1
		}
		used := res.used
		s.recordQuery(sess, queryRecord{text: text, used: &used,
			start: start, err: err, rows: rows, written: []auth.Object{{Schema: schema, Table: table}}, policies: res.policies,
			analyze:  s.rewrite(ctx, sess, owner, "CREATE OR REPLACE TABLE "+quoteName([]string{s.catalogName(), schema, table})+" AS "+j.Statement),
			stmtType: duckdb.STATEMENT_TYPE_CREATE})
		return rows, err
	}
	return -1, fmt.Errorf("unknown kind of job %q", j.Kind)
}

// stillDue checks that j, as read when its run came due, is still the job
// the scheduler holds the lock of: its owner is resolved by name, and a
// dropped owner's jobs go to a name no user can have (see disownJobs), so
// a run read before the drop would run as a user made again with that
// name. A scheduled run of a job suspended since is skipped too.
func (s *Server) stillDue(ctx context.Context, j tasks.Job) error {
	run, ok := tasks.RunOf(ctx)
	if !ok || s.tasks == nil {
		return nil
	}
	now, err := s.tasks.Store().Job(ctx, j.Kind, j.Name)
	switch {
	case errors.Is(err, tasks.ErrNotFound):
		return fmt.Errorf("%w: %s %s was dropped", tasks.ErrSkipped, j.Kind, j.Name)
	case err != nil:
		return err
	case now.Owner != j.Owner:
		return fmt.Errorf("%w: %s %s belongs to %s now, not to %s, as it did when this run came due",
			tasks.ErrSkipped, j.Kind, j.Name, now.Owner, j.Owner)
	case run.Trigger == tasks.TriggerSchedule && now.State != tasks.StateStarted:
		return fmt.Errorf("%w: %s %s was suspended", tasks.ErrSkipped, j.Kind, j.Name)
	}
	return nil
}

// taskResult counts what a task's statement returns, or the rows a
// CREATE TABLE ... AS wrote.
type taskResult struct {
	rows  int64
	tag   string
	wrote int64
	took  bool
}

func (r *taskResult) Columns([]Column) error { return nil }
func (r *taskResult) Row([]any) error        { r.rows++; return nil }
func (r *taskResult) WroteRows(n int64)      { r.wrote, r.took = n, true }
func (r *taskResult) Complete(tag string) error {
	r.tag = tag
	return nil
}

// tableGone drops the load history and the materialized view or dynamic
// table of a catalog table that was dropped, with its state, or moves them
// to its new name if it was renamed.
func (s *Server) tableGone(ctx context.Context, sess *session, o auth.Object, renameTo string) {
	store := s.tasks.Store()
	table := s.catalogName() + "." + o.String()
	var err error
	if renameTo == "" {
		for _, kind := range []string{tasks.KindMaterializedView, tasks.KindDynamicTable} {
			if j, jerr := store.Job(ctx, kind, o.String()); jerr == nil {
				s.dropViewState(ctx, sess, j)
			}
		}
	}
	if renameTo != "" {
		err = store.RenameTable(ctx, table, s.catalogName()+"."+auth.Object{Schema: o.Schema, Table: renameTo}.String())
	} else {
		err = store.ForgetTable(ctx, table)
	}
	if err != nil {
		s.log.Warn("could not update the load history after a change to the catalog", "table", table, "err", err)
	}
}
