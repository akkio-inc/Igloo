package flightsql

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/akkio-inc/igloo/internal/pgwire"
)

// handler carries out Flight SQL commands in the caller's session (see
// authorize). A query is prepared, and so authorized, when the client asks
// for its FlightInfo, and runs when the client fetches it with DoGet. A
// statement that returns no rows runs at once instead: clients send DDL and
// DML that way too, and may never fetch, or cancel the fetch of, its empty
// result (ADBC does when it runs the next statement).
type handler struct {
	fsql.BaseServer
	s *Server
}

// errTransactions refuses Flight SQL's transaction API. Statements still
// run in a session, where BEGIN, COMMIT and ROLLBACK work as in SQL.
var errTransactions = status.Error(codes.Unimplemented,
	"Flight SQL transactions are not supported; run BEGIN, COMMIT and ROLLBACK as statements instead")

func flightInfo(schema *arrow.Schema, desc *flight.FlightDescriptor, ticket []byte) *flight.FlightInfo {
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		// No location: the client fetches the result on this connection,
		// in this session.
		Endpoint:     []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
}

func (h *handler) GetFlightInfoStatement(ctx context.Context, cmd fsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return nil, errTransactions
	}
	sess := sessionFrom(ctx)
	if err := sess.acquire(ctx); err != nil {
		return nil, err
	}
	defer sess.release()
	st, handle, err := sess.prepare(ctx, cmd.GetQuery(), true)
	if err != nil {
		return nil, err
	}
	if st.schema.NumFields() == 0 {
		defer sess.drop(handle)
		return sess.runNow(ctx, st, nil, desc)
	}
	ticket, err := fsql.CreateStatementQueryTicket(handle)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return flightInfo(st.schema, desc, ticket), nil
}

func (h *handler) GetSchemaStatement(ctx context.Context, cmd fsql.StatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return nil, errTransactions
	}
	sess := sessionFrom(ctx)
	if err := sess.acquire(ctx); err != nil {
		return nil, err
	}
	defer sess.release()
	st, handle, err := sess.prepare(ctx, cmd.GetQuery(), false)
	if err != nil {
		return nil, err
	}
	sess.drop(handle)
	return &flight.SchemaResult{Schema: flight.SerializeSchema(st.schema, memory.DefaultAllocator)}, nil
}

func (h *handler) DoGetStatement(ctx context.Context, ticket fsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return sessionFrom(ctx).stream(ctx, ticket.GetStatementHandle())
}

func (h *handler) DoPutCommandStatementUpdate(ctx context.Context, cmd fsql.StatementUpdate) (int64, error) {
	if len(cmd.GetTransactionId()) > 0 {
		return 0, errTransactions
	}
	sess := sessionFrom(ctx)
	if err := sess.acquire(ctx); err != nil {
		return 0, err
	}
	defer sess.release()
	st, handle, err := sess.prepare(ctx, cmd.GetQuery(), false)
	if err != nil {
		return 0, err
	}
	defer sess.drop(handle)
	var out countWriter
	if err := sess.run(ctx, st, nil, &out); err != nil {
		return 0, statusOf(err)
	}
	return out.rows, nil
}

func (h *handler) CreatePreparedStatement(ctx context.Context, req fsql.ActionCreatePreparedStatementRequest) (fsql.ActionCreatePreparedStatementResult, error) {
	var res fsql.ActionCreatePreparedStatementResult
	if len(req.GetTransactionId()) > 0 {
		return res, errTransactions
	}
	sess := sessionFrom(ctx)
	if err := sess.acquire(ctx); err != nil {
		return res, err
	}
	defer sess.release()
	st, handle, err := sess.prepare(ctx, req.GetQuery(), false)
	if err != nil {
		return res, err
	}
	params, err := st.ps.ParamArrowSchema()
	if err != nil {
		sess.drop(handle)
		return res, statusOf(err)
	}
	return fsql.ActionCreatePreparedStatementResult{Handle: handle, DatasetSchema: st.schema, ParameterSchema: params}, nil
}

func (h *handler) ClosePreparedStatement(ctx context.Context, req fsql.ActionClosePreparedStatementRequest) error {
	sess := sessionFrom(ctx)
	if err := sess.acquire(ctx); err != nil {
		return err
	}
	defer sess.release()
	sess.drop(req.GetPreparedStatementHandle())
	return nil
}

func (h *handler) GetFlightInfoPreparedStatement(ctx context.Context, cmd fsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	sess := sessionFrom(ctx)
	st, params, ok := sess.lookup(cmd.GetPreparedStatementHandle())
	if !ok {
		return nil, errNoStatement
	}
	if st.schema.NumFields() == 0 {
		if err := sess.acquire(ctx); err != nil {
			return nil, err
		}
		defer sess.release()
		return sess.runNow(ctx, st, params, desc)
	}
	// The command itself is the ticket: DoGet runs the statement.
	return flightInfo(st.schema, desc, desc.GetCmd()), nil
}

// runNow runs a statement that returns no rows, which the caller holds the
// session for, and describes its empty result: no endpoints to fetch, and
// the rows it changed, which ADBC reports as the cursor's rowcount.
func (sess *session) runNow(ctx context.Context, st *statement, params [][]any, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var out countWriter
	if err := sess.run(ctx, st, params, &out); err != nil {
		return nil, statusOf(err)
	}
	total := int64(-1)
	if out.counted {
		total = out.rows
	}
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(emptySchema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		TotalRecords:     total,
		TotalBytes:       -1,
	}, nil
}

func (h *handler) GetSchemaPreparedStatement(ctx context.Context, cmd fsql.PreparedStatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	st, _, ok := sessionFrom(ctx).lookup(cmd.GetPreparedStatementHandle())
	if !ok {
		return nil, errNoStatement
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(st.schema, memory.DefaultAllocator)}, nil
}

func (h *handler) DoGetPreparedStatement(ctx context.Context, cmd fsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return sessionFrom(ctx).stream(ctx, cmd.GetPreparedStatementHandle())
}

// DoPutPreparedStatementQuery binds the parameters a query runs with when
// it is next fetched.
func (h *handler) DoPutPreparedStatementQuery(ctx context.Context, cmd fsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) ([]byte, error) {
	var params [][]any
	for rdr.Next() {
		rows, err := paramRows(rdr.RecordBatch())
		if err != nil {
			return nil, err
		}
		params = append(params, rows...)
	}
	if err := rdr.Err(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "read parameters: %v", err)
	}
	handle := cmd.GetPreparedStatementHandle()
	if !sessionFrom(ctx).bind(handle, params) {
		return nil, errNoStatement
	}
	return handle, nil
}

// DoPutPreparedStatementUpdate runs a statement once for each row of
// parameters the client sends, or with those bound before, and returns
// the rows they changed.
func (h *handler) DoPutPreparedStatementUpdate(ctx context.Context, cmd fsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	sess := sessionFrom(ctx)
	if err := sess.acquire(ctx); err != nil {
		return 0, err
	}
	defer sess.release()
	st, bound, ok := sess.lookup(cmd.GetPreparedStatementHandle())
	if !ok {
		return 0, errNoStatement
	}
	var out countWriter
	sent := false
	for rdr.Next() {
		rows, err := paramRows(rdr.RecordBatch())
		if err != nil {
			return out.rows, err
		}
		if len(rows) == 0 {
			continue
		}
		sent = true
		if err := sess.run(ctx, st, rows, &out); err != nil {
			return out.rows, statusOf(err)
		}
	}
	if err := rdr.Err(); err != nil {
		return out.rows, status.Errorf(codes.InvalidArgument, "read parameters: %v", err)
	}
	if !sent {
		if err := sess.run(ctx, st, bound, &out); err != nil {
			return out.rows, statusOf(err)
		}
	}
	return out.rows, nil
}

// CancelFlightInfo cancels the query a FlightInfo describes: one running
// in the caller's session stops as if its client had canceled the call,
// and one waiting to be fetched is dropped. Clients usually just cancel
// the DoGet call, which does the same.
func (h *handler) CancelFlightInfo(ctx context.Context, req *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error) {
	result := flight.CancelStatusNotCancellable
	sess := sessionFrom(ctx)
	for _, ep := range req.GetInfo().GetEndpoint() {
		handle, ok := ticketHandle(ep.GetTicket().GetTicket())
		if !ok {
			return flight.CancelFlightInfoResult{}, status.Error(codes.InvalidArgument, "the FlightInfo does not describe an Igloo query")
		}
		switch st, canceled := sess.cancel(string(handle)); {
		case canceled:
			result = flight.CancelStatusCancelled
		case st == nil:
			return flight.CancelFlightInfoResult{}, status.Error(codes.NotFound, "the query is neither running nor waiting in this session")
		}
	}
	return flight.CancelFlightInfoResult{Status: result}, nil
}

// cancel cancels the statement with handle if it is running, or drops it
// if it is a query waiting to be fetched, and reports whether it did
// either. It returns the statement, or nil if the session has none with
// handle. Unlike the session's other methods it does not wait for the
// session, which is busy running the statement it may cancel.
func (sess *session) cancel(handle string) (*statement, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.running != nil && sess.runningHandle == handle {
		return sess.running, sess.es.Cancel()
	}
	st, ok := sess.stmts[handle]
	if !ok || !st.once {
		return st, false
	}
	sess.forget(handle)
	// Release it now if the session is free; otherwise it goes when the
	// session ends.
	if sess.tryAcquire() {
		_ = st.ps.Close()
		sess.release()
	}
	return st, true
}

// ticketHandle returns the statement handle in a ticket Igloo issued.
func ticketHandle(ticket []byte) ([]byte, bool) {
	var cmd anypb.Any
	if err := proto.Unmarshal(ticket, &cmd); err != nil {
		return nil, false
	}
	msg, err := cmd.UnmarshalNew()
	if err != nil {
		return nil, false
	}
	switch m := msg.(type) {
	case *pb.TicketStatementQuery:
		return m.GetStatementHandle(), true
	case *pb.CommandPreparedStatementQuery:
		return m.GetPreparedStatementHandle(), true
	}
	return nil, false
}

// CloseSession ends the caller's session, as closing a PostgreSQL
// connection would.
func (h *handler) CloseSession(ctx context.Context, _ *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	sess := sessionFrom(ctx)
	if sess.conn.remove(sess.key) == sess {
		go sess.close()
	}
	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

// drop closes the statement with handle; the caller holds the session.
func (sess *session) drop(handle []byte) {
	sess.mu.Lock()
	st, ok := sess.stmts[string(handle)]
	sess.forget(string(handle))
	sess.mu.Unlock()
	if ok {
		_ = st.ps.Close()
	}
}

// stream runs the statement with handle and returns its result's schema
// and a channel of its record batches, which the statement sends as it
// runs. Errors before the first batch, such as a refusal, are returned
// here, as the call's status; later ones end the stream.
func (sess *session) stream(ctx context.Context, handle []byte) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if err := sess.acquire(ctx); err != nil {
		return nil, nil, err
	}
	st, params, ok := sess.take(handle)
	if !ok {
		sess.release()
		return nil, nil, errNoStatement
	}
	// A small buffer lets DuckDB produce the next batch while the last
	// one is sent.
	chunks := make(chan flight.StreamChunk, 2)
	w := &streamWriter{ctx: ctx, chunks: chunks, ready: make(chan error, 1)}
	sess.mu.Lock()
	sess.running, sess.runningHandle = st, string(handle)
	sess.mu.Unlock()
	go func() {
		defer close(chunks)
		err := sess.run(ctx, st, params, w)
		sess.mu.Lock()
		sess.running, sess.runningHandle = nil, ""
		sess.mu.Unlock()
		if st.once {
			_ = st.ps.Close()
		}
		// Free the session before the client can see the end of the
		// result, so its next call does not find it busy.
		sess.release()
		w.finish(statusOf(err))
	}()
	if err := <-w.ready; err != nil {
		return nil, nil, err
	}
	return w.schema, chunks, nil
}

// countWriter counts the rows a statement changed or returned. counted
// says a command tag gave a count; DDL has none.
type countWriter struct {
	rows    int64
	counted bool
}

var _ pgwire.ArrowWriter = (*countWriter)(nil)

func (c *countWriter) Columns([]pgwire.Column) error { return nil }
func (c *countWriter) Row([]any) error               { return nil }

func (c *countWriter) Records(rdr array.RecordReader) (int64, error) {
	var n int64
	for rdr.Next() {
		n += rdr.RecordBatch().NumRows()
	}
	return n, rdr.Err()
}

func (c *countWriter) Complete(tag string) error {
	if n := pgwire.TagRows(tag); n >= 0 {
		c.rows += n
		c.counted = true
	}
	return nil
}
