package distributed

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// How a worker chooses to write a stage's partial results a row at a time (see
// rowSpec). The stage's first task on the worker probes: it runs the stage's
// grouped SQL over a share of its largest input, and if every group of the
// sample is one row, the stage's tasks on the worker write a row at a time, the
// first included. The stage's other tasks wait for the probe. A worker runs as
// many of a stage's tasks at once as it has slots, often every task it has of
// the stage: learning from a whole task, as it did first, came too late for
// any of them, and a report's stage over 10.6 billion exploded rows, groups of
// one row each, grouped them all.
//
// A sample that finds groups of several rows, or finds no rows, or takes longer
// than probeTimeout, leaves the stage grouped as before, and a task of it that
// finds each of its groups one row still has the next ones write a row at a
// time. Missing groups of several rows costs only time: either way of writing
// partial results gives the same rows.

const (
	// probeShare is the share of its largest input a probe reads, one in
	// probeShare of its rows.
	probeShare = 32
	// probeTimeout bounds a probe, which the stage's other tasks wait for.
	probeTimeout = 5 * time.Second
	// stageRowsAge is how long a worker keeps its choice for a stage.
	stageRowsAge = time.Hour
)

// probeMinRows is the rows a task's largest input must have for a probe: a
// small task costs what it costs either way. Tests lower it.
var probeMinRows int64 = 1_000_000

// stageRows is a worker's choice of how a stage's tasks write its partial
// results: rows says a row at a time. done is closed once the first task's
// probe chose.
type stageRows struct {
	at   time.Time
	done chan struct{}
	rows bool
}

// byRowKey names req's stage.
func byRowKey(req *taskRequest) string {
	return req.Query + "/" + strconv.Itoa(req.Task/tasksPerStage)
}

// rowsAtOnce reports whether req, a task of a stage that may write its partial
// results a row at a time, does: as the stage's probe on this worker chose,
// which req runs on conn, where its views are, if it is the stage's first task
// here, or else waits for. It fails if the probe could not put req's views back
// as they were.
func (w *Worker) rowsAtOnce(ctx context.Context, conn *duckdb.Conn, req *taskRequest) (bool, error) {
	key := byRowKey(req)
	w.byRowMu.Lock()
	if w.byRow == nil {
		w.byRow = map[string]*stageRows{}
	}
	now := time.Now()
	for k, s := range w.byRow {
		if now.Sub(s.at) > stageRowsAge {
			delete(w.byRow, k)
		}
	}
	s := w.byRow[key]
	first := s == nil
	if first {
		s = &stageRows{at: now, done: make(chan struct{})}
		w.byRow[key] = s
	}
	w.byRowMu.Unlock()
	if first {
		rows, err := w.probeRows(ctx, conn, req)
		w.byRowMu.Lock()
		s.rows = rows
		w.byRowMu.Unlock()
		close(s.done)
		return rows, err
	}
	select {
	case <-s.done:
	case <-ctx.Done():
		return false, nil
	case <-time.After(probeTimeout + time.Second):
		return false, nil
	}
	w.byRowMu.Lock()
	defer w.byRowMu.Unlock()
	return s.rows, nil
}

// noteRowsAtOnce has the next tasks of req's stage write their partial results a
// row at a time: req, run grouped, found each of its groups one row.
func (w *Worker) noteRowsAtOnce(req *taskRequest) {
	w.byRowMu.Lock()
	defer w.byRowMu.Unlock()
	if s := w.byRow[byRowKey(req)]; s != nil {
		s.rows = true
	}
	w.log.Debug("writing a stage's partial results a row at a time: a task's groups were each one row", "query", req.Query,
		"stage", req.Task/tasksPerStage)
}

// probeRows runs req's grouped SQL on conn over one in probeShare of the rows of
// its largest input, and reports whether every group of the sample is one row.
// It reports false where it cannot tell: an input too small to probe, a sample
// of no rows, a probe that fails or takes longer than probeTimeout. It fails only
// if it could not put the input's view back as it was.
func (w *Worker) probeRows(ctx context.Context, conn *duckdb.Conn, req *taskRequest) (bool, error) {
	largest := -1
	for i, v := range req.Views {
		if v.Rows >= probeMinRows && (largest < 0 || v.Rows > req.Views[largest].Rows) {
			largest = i
		}
	}
	if largest < 0 {
		return false, nil
	}
	start := time.Now()
	v := req.Views[largest]
	exec := func(ctx context.Context, q string) error {
		_, err := conn.ExecContext(ctx, q, nil)
		return err
	}
	// The view's rows, while it reads a share of them: the first ones, which
	// come from its files as they are, as any share would.
	const full = "__igloo_probe_full"
	if err := exec(ctx, "ALTER VIEW "+quoteIdent(v.Name)+" RENAME TO "+full); err != nil {
		return false, nil
	}
	restore := func() error {
		bg := context.WithoutCancel(ctx)
		if err := exec(bg, "DROP VIEW IF EXISTS "+quoteIdent(v.Name)); err != nil {
			return err
		}
		return exec(bg, "ALTER VIEW "+full+" RENAME TO "+quoteIdent(v.Name))
	}
	var most, groups int64
	err := exec(ctx, "CREATE TEMP VIEW "+quoteIdent(v.Name)+" AS SELECT * FROM "+full+" LIMIT "+strconv.FormatInt(v.Rows/probeShare, 10))
	if err == nil {
		pctx, cancel := context.WithTimeout(ctx, probeTimeout)
		most, groups, err = probeGroups(pctx, conn, req.SQL)
		cancel()
	}
	if rerr := restore(); rerr != nil {
		return false, fmt.Errorf("put view %s back after a probe of its rows: %w", v.Name, rerr)
	}
	byRow := err == nil && groups > 0 && most <= 1
	w.log.Debug("probed a stage's groups", "query", req.Query, "stage", req.Task/tasksPerStage, "groups", groups, "most_in_a_group", most,
		"a_row_at_a_time", byRow, "seconds", time.Since(start).Seconds(), "err", err)
	return byRow, nil
}

// probeGroups returns the most rows a group of query's partial results counts
// (groupRowsColumn), and how many groups it has.
func probeGroups(ctx context.Context, conn *duckdb.Conn, query string) (most, groups int64, err error) {
	res, err := conn.QueryContext(ctx, "SELECT coalesce(max("+quoteIdent(groupRowsColumn)+"), 0), count(*) FROM ("+query+") AS __igloo_probe", nil)
	if err != nil {
		return 0, 0, err
	}
	defer res.Close() //nolint:errcheck
	dest := make([]driver.Value, 2)
	if err := res.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("the probe returned no row")
		}
		return 0, 0, err
	}
	most, _ = asInt64(dest[0])
	groups, _ = asInt64(dest[1])
	return most, groups, nil
}
