package pgwire

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Erasure and refreshed tables. A materialized view or dynamic table holds
// what its query made of its tables' rows, in its table's current rows and
// older snapshots, and an aggregating one, its groups' partial aggregates
// in its state table, a bitmap of a group's ids among them. An erasure
// request on any table a view reads has the view replaced once the
// person's rows are gone (erasure.Views): its table made again from its
// query, as its owner, as CREATE OR REPLACE TABLE ... AS does, which
// deletes the old table's files; its state dropped; and what its last
// refresh read forgotten, so that the next refresh is full.

// ViewRebuilder replaces materialized views and dynamic tables for erasure
// requests (erasure.Views). Its server is set once made, after the erasure
// service; until then requests try again later.
type ViewRebuilder struct {
	srv atomic.Pointer[Server]
}

// Bind sets the server whose views v replaces.
func (v *ViewRebuilder) Bind(s *Server) { v.srv.Store(s) }

func (v *ViewRebuilder) server() (*Server, error) {
	s := v.srv.Load()
	if s == nil || s.tasks == nil {
		return nil, errors.New("the SQL server is not ready to replace materialized views and dynamic tables")
	}
	return s, nil
}

// leaseWait is how long a request waits for a refresh of a view it is to
// replace to end, before it tries again later.
const leaseWait = 30 * time.Second

// erasureViews returns the views made from tables, schema.table in lower
// case, or from views that are, upstream first: those whose name is one of
// tables, as lineage finds them, and those whose query names one of them
// or such a view, which errs toward more.
func (s *Server) erasureViews(ctx context.Context, tables []string) ([]tasks.Job, error) {
	var all []tasks.Job
	for _, kind := range []string{tasks.KindMaterializedView, tasks.KindDynamicTable} {
		jobs, err := s.tasks.Store().Jobs(ctx, kind)
		if err != nil {
			return nil, err
		}
		all = append(all, jobs...)
	}
	from := map[string]bool{}
	for _, t := range tables {
		from[strings.ToLower(t)] = true
	}
	var out []tasks.Job
	in := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, j := range all {
			name := strings.ToLower(j.Name)
			if in[name] {
				continue
			}
			reads := from[name]
			for t := range from {
				if !reads && namesTable(j.Statement, t) {
					reads = true
				}
			}
			if reads {
				in[name], from[name], changed = true, true, true
				out = append(out, j)
			}
		}
	}
	return out, nil
}

// namesTable reports whether text names table, schema.table, by its name.
func namesTable(text, table string) bool {
	short := table[strings.LastIndex(table, ".")+1:]
	return regexp.MustCompile(`(?i)(^|[^\w$])` + regexp.QuoteMeta(short) + `($|[^\w$])`).MatchString(text)
}

// staleView reports whether j's table, or its state, has a snapshot from
// before since, or a full refresh of it from before then may have left
// files (see clearRefreshFiles): they may hold what rows erased since
// gave.
func (s *Server) staleView(ctx context.Context, j tasks.Job, since time.Time) (bool, error) {
	if s.ddl == nil {
		return false, nil
	}
	schema, table, _ := strings.Cut(j.Name, ".")
	if left, err := s.refreshFilesBefore(ctx, schema, table, since); err != nil || left {
		return left, err
	}
	names := [][2]string{{schema, table}}
	if st := decodeRefresh(j.Refresh); st.State != "" {
		names = append(names, [2]string{refreshSchema, st.State})
	}
	for _, n := range names {
		tbl, err := s.ddl.LoadTable(ctx, n[0], n[1])
		if errors.Is(err, ddl.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		for _, snap := range tbl.Metadata().Snapshots() {
			if time.UnixMilli(snap.TimestampMs).Before(since) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (v *ViewRebuilder) Stale(ctx context.Context, tables []string, since time.Time) ([]string, error) {
	s, err := v.server()
	if err != nil {
		return nil, err
	}
	views, err := s.erasureViews(ctx, tables)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, j := range views {
		stale, err := s.staleView(ctx, j, since)
		if err != nil {
			return nil, err
		}
		if stale {
			out = append(out, j.Name)
		}
	}
	return out, nil
}

func (v *ViewRebuilder) Rebuild(ctx context.Context, tables []string, since time.Time) (erasure.Rebuilt, error) {
	var out erasure.Rebuilt
	s, err := v.server()
	if err != nil {
		return out, err
	}
	views, err := s.erasureViews(ctx, tables)
	if err != nil {
		return out, err
	}
	for _, j := range views {
		stale, err := s.staleView(ctx, j, since)
		if err != nil {
			return out, err
		}
		if !stale {
			continue
		}
		err = s.rebuildView(ctx, j)
		var busy *viewBusy
		switch {
		case err == nil:
			out.Views++
		case errors.As(err, &busy):
			return out, erasure.Busy(err)
		case errors.Is(err, tasks.ErrNotFound):
			// Dropped meanwhile: nothing of it is left.
		case ctx.Err() != nil:
			return out, ctx.Err()
		default:
			out.Failed = append(out.Failed, j.Name+": "+err.Error())
		}
	}
	return out, nil
}

// viewBusy is a view whose refresh runs.
type viewBusy struct{ name string }

func (b *viewBusy) Error() string {
	return "a refresh of " + b.name + " is running; the request replaces it once it ends"
}

// rebuildView replaces j's table, as its owner, from its query, and drops
// its state, holding its job's lease so that no refresh runs meanwhile.
func (s *Server) rebuildView(ctx context.Context, j tasks.Job) error {
	store := s.tasks.Store()
	key := tasks.JobKey(j.Kind, j.Name)
	var lease tasks.Lease
	deadline := time.Now().Add(leaseWait)
	for {
		l, ok, err := store.TryLease(ctx, key)
		if err != nil {
			return err
		}
		if ok {
			lease = l
			break
		}
		if time.Now().After(deadline) {
			return &viewBusy{name: j.Name}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	defer lease.Release()
	// As it is now, holding the lease.
	j, err := store.Job(ctx, j.Kind, j.Name)
	if err != nil {
		return err
	}
	owner, err := s.users.Get(ctx, j.Owner)
	if errors.Is(err, auth.ErrNotFound) {
		return fmt.Errorf("its owner %s no longer exists: an admin can create it again, or drop it", j.Owner)
	}
	if err != nil {
		return err
	}
	if owner.Disabled {
		return fmt.Errorf("its owner %s is disabled (ALTER USER %s LOGIN enables them)", owner.Name, owner.Name)
	}
	es, err := s.OpenEmbedded(ctx, owner, "erasure: replace "+j.Kind+" "+j.Name)
	if err != nil {
		return err
	}
	defer es.Close() //nolint:errcheck
	sess := es.sess
	sess.method, sess.job = methodTask, j.Kind+" "+j.Name
	schema, table, _ := strings.Cut(j.Name, ".")
	create := "CREATE OR REPLACE TABLE " + quoteName([]string{s.catalogName(), schema, table}) + " AS " + j.Statement
	// Where the files of a stopped refresh are, once it is another table.
	var old []string
	if s.ddl != nil {
		if tbl, err := s.ddl.LoadTable(ctx, schema, table); err == nil {
			old = append(old, tbl.Location())
		}
	}
	start := time.Now()
	// CREATE OR REPLACE TABLE ... AS: run again, it replaces the table as once.
	err = sess.runCancelable(ctx, create, s.rerunsAlone(sess, func(ctx context.Context) error {
		return s.execDDL(ctx, sess, owner, create)
	}))
	s.recordQuery(sess, queryRecord{text: create, start: start, err: err, rows: -1,
		written: []auth.Object{{Schema: schema, Table: table}}, stmtType: duckdb.STATEMENT_TYPE_CREATE})
	if err != nil {
		return err
	}
	// Files a stopped full refresh wrote under the old table's location,
	// in none of its snapshots, which dropping it may not have taken. No
	// refresh of the view runs while the request holds its lease.
	if err := s.clearRefreshFiles(ctx, schema, table, true, old...); err != nil {
		return fmt.Errorf("delete the files a stopped refresh of it left: %w", err)
	}
	s.dropViewState(ctx, sess, j)
	st := decodeRefresh(j.Refresh)
	st.Query, st.Sources, st.Table, st.State, st.StateSnapshot = "", nil, 0, "", 0
	st.Action, st.Detail, st.DataTimestamp = actionFull, "replaced by an erasure request", time.Now().UTC()
	if err := store.SetRefresh(ctx, j.Kind, j.Name, st.encode()); err != nil {
		return err
	}
	s.log.Info("erasure replaced a refreshed table", "kind", j.Kind, "name", j.Name)
	return nil
}
