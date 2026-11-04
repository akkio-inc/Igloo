package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/config"
)

// Igloo records what each client statement used in the query history:
// CPU time, file bytes read and memory allocated, as DuckDB's profiler
// measures them. Session connections have the profiler on with only those
// metrics, which costs well under a millisecond per statement to read
// back. DuckDB 1.5's profiler reports peak memory only for the whole
// process since it started, so the history has the memory a statement
// allocated over its run, and the most it was seen holding as the workload
// controller samples DuckDB's memory (see HeldMemory); and it does not
// count bytes written to object storage, so the history has no bytes
// written.

// profilingStatements turn the profiler on, without output, for every kind
// of statement. QUERY_NAME tells a statement's own measurements from those
// of one before it. Each operator's name and the rows it made are what the
// working-set estimate learns from (see LastOperators). (ROWS_RETURNED is
// left out: DuckDB 1.5.6 fails with an internal error rendering it.)
var profilingStatements = []string{
	"SET enable_profiling = 'no_output'",
	"SET profiling_coverage = 'ALL'",
	`SET custom_profiling_settings = '{"CPU_TIME": "true", "TOTAL_BYTES_READ": "true", ` +
		`"TOTAL_MEMORY_ALLOCATED": "true", "QUERY_NAME": "true", "OPERATOR_NAME": "true", "OPERATOR_CARDINALITY": "true"}'`,
}

// Meter reads back what DuckDB measured of the statements one connection
// runs. It owns the connection.
type Meter struct {
	db   *sql.DB
	conn *sql.Conn
	// last is the profile Last read, of the statement it measured.
	last *duckdb.ProfilingInfo
}

// ConnectMetered opens a connection for a client session, like Connect,
// with a Meter for it. Closing the Meter closes the connection.
func (e *Engine) ConnectMetered(ctx context.Context) (*duckdb.Conn, *Meter, error) {
	conn, err := e.Connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, stmt := range profilingStatements {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return nil, nil, errors.Join(err, conn.Close())
		}
	}
	// duckdb-go reads profiles only through database/sql, so the
	// connection gets a database/sql handle of its own.
	db := sql.OpenDB(oneConn{conn})
	sc, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, errors.Join(err, db.Close(), conn.Close())
	}
	return conn, &Meter{db: db, conn: sc}, nil
}

// Last returns what the connection's last statement used, if it was query
// (the text it was prepared from) and DuckDB measured it; nil otherwise.
func (m *Meter) Last(query string) *audit.Resources {
	if m == nil {
		return nil
	}
	m.last = nil
	info, err := duckdb.GetProfilingInfo(m.conn)
	if err != nil || !sameQuery(info.Metrics["QUERY_NAME"], query) {
		return nil
	}
	m.last = &info
	metric := func(name string) int64 {
		n, _ := strconv.ParseInt(info.Metrics[name], 10, 64)
		return n
	}
	cpu, _ := strconv.ParseFloat(info.Metrics["CPU_TIME"], 64)
	return &audit.Resources{
		CPUTime:         time.Duration(cpu * float64(time.Second)),
		BytesRead:       metric("TOTAL_BYTES_READ"),
		MemoryAllocated: metric("TOTAL_MEMORY_ALLOCATED"),
	}
}

// Operator is an operator of a statement's run, as DuckDB's profiler saw
// it: its name, the rows it made, and its children.
type Operator struct {
	Name     string
	Rows     float64
	Children []Operator
}

// LastOperators are the operators of the statement Last measured last, as
// a tree under the profile's root; ok is false if it measured none.
func (m *Meter) LastOperators() (ops Operator, ok bool) {
	if m == nil || m.last == nil {
		return Operator{}, false
	}
	var conv func(p duckdb.ProfilingInfo) Operator
	conv = func(p duckdb.ProfilingInfo) Operator {
		rows, _ := strconv.ParseFloat(p.Metrics["OPERATOR_CARDINALITY"], 64)
		o := Operator{Name: p.Metrics["OPERATOR_NAME"], Rows: rows}
		for _, c := range p.Children {
			o.Children = append(o.Children, conv(c))
		}
		return o
	}
	return conv(*m.last), true
}

// ExplainAnalyze runs fn, which runs an EXPLAIN ANALYZE, with DuckDB's own
// profiling settings, since the plan it prints shows only the metrics they
// collect, and then puts Igloo's back.
//
// The profiler covers only SELECTs (which EXPLAIN ANALYZE is profiled as
// either way) while the metrics change. In DuckDB 1.5.6, a statement that
// changes the metrics while the profiler covers it, after a query the
// profiler measured with the old ones, leaves the profiler stuck on that
// statement: the EXPLAIN ANALYZE then writes its operators' measurements
// into the stuck profile and segfaults (QueryProfiler::Flush), on a DuckDB
// worker thread as often as not.
func (m *Meter) ExplainAnalyze(ctx context.Context, fn func() error) error {
	if m == nil {
		return fn()
	}
	for _, stmt := range []string{"SET profiling_coverage = 'SELECT'", "RESET custom_profiling_settings"} {
		if _, err := m.conn.ExecContext(ctx, stmt); err != nil {
			return errors.Join(err, m.restore(ctx))
		}
	}
	return errors.Join(fn(), m.restore(ctx))
}

// restore puts Igloo's profiling settings back after ExplainAnalyze: the
// metrics first, while the profiler still covers only SELECTs.
func (m *Meter) restore(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)
	var errs []error
	for _, stmt := range []string{profilingStatements[2], profilingStatements[1]} {
		_, err := m.conn.ExecContext(ctx, stmt)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// sameQuery compares statement texts, ignoring the whitespace and
// semicolons around them, which DuckDB drops when it splits statements.
func sameQuery(a, b string) bool {
	trim := func(s string) string { return strings.TrimRight(strings.TrimSpace(s), "; \t\r\n") }
	return a != "" && trim(a) == trim(b)
}

// Close closes the connection.
func (m *Meter) Close() error {
	if m == nil {
		return nil
	}
	return errors.Join(m.conn.Close(), m.db.Close())
}

// oneConn is a connector that hands database/sql one existing connection.
type oneConn struct{ conn *duckdb.Conn }

func (c oneConn) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c oneConn) Driver() driver.Driver                        { return &duckdb.Driver{} }

// Held is what each of the process's DuckDBs holds now, the first and then the
// second (see background.go), which holds nothing while it is not open: All,
// caches included, and Query, what the statements running in it hold, their
// hash tables, sorts, windows, materialized rows and file readers, not the
// tables, metadata and caches it keeps from one statement to the next.
type Held struct {
	All, Query [2]int64
}

// heldMemory reads what a DuckDB holds in all, then for its statements.
const heldMemory = `SELECT coalesce(sum(memory_usage_bytes), 0)::VARCHAR || ' ' || coalesce(sum(memory_usage_bytes)
	FILTER (WHERE tag IN ('HASH_TABLE', 'ORDER_BY', 'WINDOW', 'COLUMN_DATA', 'ALLOCATOR', 'OVERFLOW_STRINGS',
		'PARQUET_READER', 'CSV_READER', 'TRANSACTION')), 0)::VARCHAR FROM system.main.duckdb_memory()`

// held reads what this DuckDB holds in all and for its statements, over a
// connection it keeps for that, one read at a time: a read takes well under
// 10 ms while statements run, and the workload controller reads ten times a
// second.
func (e *Engine) held(ctx context.Context) (all, query int64, err error) {
	e.heldMu.Lock()
	defer e.heldMu.Unlock()
	if e.heldConn == nil {
		c, err := e.connector.Connect(ctx)
		if err != nil {
			return 0, 0, err
		}
		e.heldConn = c.(*duckdb.Conn)
	}
	v, err := queryString(ctx, e.heldConn, heldMemory)
	if err != nil {
		e.heldConn.Close() //nolint:errcheck
		e.heldConn = nil
		return 0, 0, err
	}
	_, err = fmt.Sscan(v, &all, &query)
	return all, query, err
}

// closeHeld closes the connection held reads over.
func (e *Engine) closeHeld() error {
	e.heldMu.Lock()
	defer e.heldMu.Unlock()
	if e.heldConn == nil {
		return nil
	}
	err := e.heldConn.Close()
	e.heldConn = nil
	return err
}

// SetMemoryLimit changes DuckDB's memory limit to n bytes, which it then
// keeps by writing buffers to its spill files (see memlimit.Adapt). It
// fails if DuckDB cannot free enough memory for the new limit.
func (e *Engine) SetMemoryLimit(ctx context.Context, n int64) error {
	e.limit.Store(n)
	if e.bgUsers.Load() > 0 {
		// The two share it: weighed again at once (see background.go).
		e.balanceNow()
		return nil
	}
	return e.setLimit(ctx, n)
}

// HeldMemory reads what both DuckDBs hold (see Held); ok is false if a read
// failed. memlimit.Adapt lowers their limit from what they hold in all, and the
// workload controller charges what each holds for its statements to those
// running in it (see workload.Controller.SampleMemory).
func (e *Engine) HeldMemory(ctx context.Context) (h Held, ok bool) {
	var err error
	if h.All[0], h.Query[0], err = e.held(ctx); err != nil {
		return Held{}, false
	}
	e.bgMu.Lock()
	bg := e.bg
	e.bgMu.Unlock()
	if bg != nil {
		if h.All[1], h.Query[1], err = bg.held(ctx); err != nil {
			return Held{}, false
		}
	}
	return h, true
}

// Sum is what both hold in all.
func (h Held) Sum() int64 { return h.All[0] + h.All[1] }

// setLimit sets DuckDB's memory limit, to n bytes.
func (e *Engine) setLimit(ctx context.Context, n int64) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	if _, err = c.(*duckdb.Conn).ExecContext(ctx, fmt.Sprintf("SET GLOBAL memory_limit = '%dMiB'", n>>20), nil); err != nil {
		return err
	}
	e.current.Store(n)
	return nil
}

// MemoryLimit is the query memory DuckDB has, in bytes: its memory_limit,
// IGLOO_DUCKDB_MEMORY_LIMIT or 80% of the machine's memory. It is zero if
// DuckDB's setting cannot be read.
func (e *Engine) MemoryLimit(ctx context.Context) int64 {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return 0
	}
	defer c.Close() //nolint:errcheck
	v, err := queryString(ctx, c.(*duckdb.Conn), "SELECT current_setting('memory_limit')::VARCHAR")
	if err != nil {
		return 0
	}
	n, err := config.ParseSize(v)
	if err != nil {
		return 0
	}
	return n
}
