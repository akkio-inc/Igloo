package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"time"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// Igloo patch: a query whose result DuckDB streams. QueryContext has DuckDB
// materialize the whole result before it returns the first row: a sort of
// hundreds of millions of rows without LIMIT held every row in memory, past
// the server's memory, before its client saw one. QueryStreamContext runs
// the query's blocking steps (a sort's input, a hash table's build side)
// before it returns, as QueryContext does, and then computes the rest of
// the result as Next reads it, a chunk at a time.
//
// While its rows are open, the connection must run nothing else: DuckDB
// ends a streaming result when another query runs on its connection. The
// context's cancellation interrupts the query until the rows are closed,
// and an error DuckDB meets while it streams the rows is what Next returns.

// ErrFalseInterrupt is the error of a streaming query that DuckDB reports
// interrupted while its context lasts. A thread of a query that fails records
// its error and marks the query interrupted, for the query's other threads to
// stop. The thread reading a streaming result looks for that mark first, and
// reports an interrupt in place of the error, which ends with the query: a
// statement that failed, which nothing canceled. Read whole, the same
// statement reports the thread's error.
var ErrFalseInterrupt = errors.New("DuckDB reported a streaming query interrupted, and nothing interrupted it")

// streamError is the error of a streaming query under ctx that DuckDB ended
// with msg.
func streamError(ctx context.Context, msg string) error {
	err := getDuckDBError(msg)
	var de *Error
	if ctx.Err() == nil && errors.As(err, &de) && de.Type == ErrorTypeInterrupt {
		return ErrFalseInterrupt
	}
	return errors.Join(ctx.Err(), err)
}

// QueryStreamContext is QueryContext with DuckDB streaming the result.
func (s *Stmt) QueryStreamContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	cleanupCtx := s.conn.setContext(ctx)
	var res *mapping.Result
	if err := runWithCtxInterrupt(ctx, s.conn.conn, func(wctx context.Context) error {
		if s.closed {
			return errClosedStmt
		}
		if s.rows {
			return errActiveRows
		}
		if err := s.BindWithCtx(wctx, nargs); err != nil {
			return err
		}
		if err := wctx.Err(); err != nil {
			return err
		}
		var err error
		res, err = s.executeBoundStreaming(wctx)
		return err
	}); err != nil {
		cleanupCtx()
		return nil, err
	}
	s.rows = true
	r := &streamRows{rows: newRowsWithStmt(*res, s), ctx: ctx, cleanup: cleanupCtx}
	r.stop = interruptUntil(ctx, s.conn.conn)
	return r, nil
}

// executeBoundStreaming is executeBound with a streaming pending result.
func (s *Stmt) executeBoundStreaming(ctx context.Context) (res *mapping.Result, err error) {
	s.conn.guard(func() { res, err = s.executeBoundStreamingGuarded(ctx) })
	return res, err
}

// executeBoundStreamingGuarded is executeBoundStreaming within the
// connection's call guard.
func (s *Stmt) executeBoundStreamingGuarded(ctx context.Context) (*mapping.Result, error) {
	var pendingRes mapping.PendingResult
	release := s.conn.holdBind()
	pendingState := mapping.PendingPreparedStreaming(*s.preparedStmt, &pendingRes)
	release()
	if pendingState == mapping.StateError {
		dbErr := getDuckDBError(mapping.PendingError(pendingRes))
		mapping.DestroyPending(&pendingRes)
		return nil, dbErr
	}
	defer mapping.DestroyPending(&pendingRes)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if failed, msg := runPending(pendingRes); failed {
		return nil, streamError(ctx, msg)
	}
	var res mapping.Result
	if mapping.ExecutePending(pendingRes, &res) == mapping.StateError {
		err := streamError(ctx, mapping.ResultError(&res))
		mapping.DestroyResult(&res)
		return nil, err
	}
	return &res, nil
}

// streamRows are the rows of a streaming result.
type streamRows struct {
	*rows
	ctx     context.Context
	cleanup func()
	stop    func()
	closed  bool
}

// Next reads the next row, which DuckDB may compute now. At the end of the
// rows, it returns io.EOF, or the error that ended them.
func (r *streamRows) Next(dst []driver.Value) error {
	err := r.rows.Next(dst)
	if !errors.Is(err, io.EOF) {
		return err
	}
	if msg := mapping.ResultError(&r.res); msg != "" {
		return streamError(r.ctx, msg)
	}
	return io.EOF
}

// Close ends the result, and with it the query if it is still running.
func (r *streamRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	// Destroying the result ends the query, after the interrupts end: none
	// may reach the next query on the connection.
	r.stop()
	err := r.rows.Close()
	r.cleanup()
	return err
}

// interruptUntil interrupts the query on conn, every interruptInterval,
// from when ctx ends until stop is called. stop waits for the interrupts
// to end, so that none reaches a query run after it.
func interruptUntil(ctx context.Context, conn mapping.Connection) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
		case <-done:
			return
		}
		t := time.NewTicker(interruptInterval)
		defer t.Stop()
		for {
			mapping.Interrupt(conn)
			select {
			case <-done:
				return
			case <-t.C:
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
