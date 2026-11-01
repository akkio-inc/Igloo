package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
)

// Arrow results and prepared statements for embedded sessions, which the
// Arrow Flight SQL endpoint (package flightsql) serves. The rows of a query
// DuckDB runs reach an ArrowWriter as DuckDB's own record batches, never as
// text; everything else about the statement (authorization, the statement
// timeout, cancellation, metrics and the history) is what a statement from
// a PostgreSQL client goes through.

// ArrowWriter is a ResultWriter that also takes results as Apache Arrow
// record batches. The rows of a statement DuckDB runs arrive in Records,
// after Columns; results Igloo produces itself, such as SHOW GRANTS, still
// arrive through Row, as text (see TextSchema).
type ArrowWriter interface {
	ResultWriter
	// Records receives a result's record batches, all with rdr's schema,
	// and returns how many rows it sent. Complete follows.
	Records(rdr array.RecordReader) (int64, error)
}

// ErrStatementTimeout is the error of a statement that the statement
// timeout canceled. Its SQLSTATE, 57014, is that of any canceled
// statement.
var ErrStatementTimeout = errStatementTimeout

// TagRows is the row count in a command tag, such as 5 in "INSERT 0 5", or
// -1 when the tag has none.
func TagRows(tag string) int64 { return tagRows(tag) }

// arrowWriterOf returns the ArrowWriter an embedded session's statement
// writes to through w, or nil when there is none.
func arrowWriterOf(w wire.DataWriter) ArrowWriter {
	if tw, ok := w.(*tagWriter); ok {
		w = tw.DataWriter
	}
	if ew, ok := w.(*embeddedWriter); ok {
		aw, _ := ew.out.(ArrowWriter)
		return aw
	}
	return nil
}

// runArrow runs a statement that returns rows and sends its result to aw
// as DuckDB's record batches.
func (st *statement) runArrow(ctx context.Context, args []driver.NamedValue, w wire.DataWriter, aw ArrowWriter) error {
	// DuckDB computes the batches as the writer takes them, rather than all of
	// them first: the session is the statement's until they are sent.
	n, err := st.sendRecords(ctx, st.stmt.QueryArrowStreamContext, args, aw)
	if errors.Is(err, duckdb.ErrFalseInterrupt) {
		// See afterFalseInterrupt.
		if n > 0 || !st.onlyReads() || st.readsOnce || writesSequence(st.prepared) {
			return streamFailed(uint32(n))
		}
		n, err = st.sendRecords(ctx, st.stmt.QueryArrowContext, args, aw)
		var de *duckdb.Error
		if errors.As(err, &de) && de.Type == duckdb.ErrorTypeTransaction && strings.Contains(de.Msg, "aborted") {
			return streamFailed(0)
		}
	}
	if err != nil {
		return err
	}
	return w.Complete(commandTag(st.stmtType, st.query, n))
}

// sendRecords runs the statement through query and sends its record batches
// to aw, and returns how many rows it sent.
func (st *statement) sendRecords(ctx context.Context, query func(context.Context, []driver.NamedValue) (array.RecordReader, error),
	args []driver.NamedValue, aw ArrowWriter) (int64, error) {
	rdr, err := query(ctx, args)
	if err != nil {
		return 0, err
	}
	if st.names != nil {
		rdr = &renamedReader{RecordReader: rdr, schema: renameSchema(rdr.Schema(), st.names)}
	}
	defer rdr.Release()
	return aw.Records(rdr)
}

// TextSchema is the Arrow schema of a result Igloo produces itself, whose
// rows an ArrowWriter receives through Row: a nullable string for each
// column, holding values in PostgreSQL's text format.
func TextSchema(cols []Column) *arrow.Schema {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{Name: c.Name, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

// SetApplicationName names the client's application, for the history.
func (e *EmbeddedSession) SetApplicationName(name string) { e.sess.setApplicationName(name) }

// Cancel cancels the statement running in the session, as a cancel
// request from a SQL client does, and reports whether one was running.
// Unlike the session's other methods, it may be called while another runs.
func (e *EmbeddedSession) Cancel() bool {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.running == nil {
		return false
	}
	e.sess.running()
	return true
}

// PreparedStatement is a statement prepared in an embedded session to run
// later, perhaps several times with different parameters.
type PreparedStatement struct {
	e  *EmbeddedSession
	ps *wire.PreparedStatement
}

// Prepare parses query, one statement, and checks it against the user's
// role and grants, as a SQL client's Parse message does: a statement the
// user may not run fails here, with SQLSTATE 42501, and is recorded in the
// history. Like any prepared statement it is checked again before it runs
// if grants, the catalog or the session's settings changed since.
func (e *EmbeddedSession) Prepare(ctx context.Context, query string) (*PreparedStatement, error) {
	ctx = context.WithValue(ctx, sessionKey{}, e.sess)
	stmts, err := e.srv.parse(ctx, wire.Query{Query: query})
	if err != nil {
		return nil, pgError(err)
	}
	if len(stmts) != 1 {
		for _, ps := range stmts {
			_ = e.sess.release(ps)
		}
		return nil, wire.NewErrUndefinedStatement()
	}
	return &PreparedStatement{e: e, ps: stmts[0]}, nil
}

// Columns describes the statement's result; it is empty when the
// statement returns no rows.
func (p *PreparedStatement) Columns() []Column { return columnsOf(p.ps.Columns()) }

// Params are the PostgreSQL types (OIDs) of the statement's parameters,
// $1 first.
func (p *PreparedStatement) Params() []uint32 {
	if st, ok := p.e.sess.open[p.ps]; ok {
		return st.params
	}
	return nil
}

// ParamArrowSchema has the Arrow types of the statement's parameters, $1
// first, as DuckDB infers them: what a Flight SQL client should bind.
func (p *PreparedStatement) ParamArrowSchema() (*arrow.Schema, error) {
	if st, ok := p.e.sess.open[p.ps]; ok {
		return st.stmt.ParamArrowSchema()
	}
	return arrow.NewSchema(nil, nil), nil
}

// ArrowSchema is the schema of the record batches Run sends an
// ArrowWriter: DuckDB's Arrow types for a query DuckDB runs, TextSchema for
// a result Igloo produces itself, and no columns for a statement that
// returns no rows.
func (p *PreparedStatement) ArrowSchema() (*arrow.Schema, error) {
	if len(p.ps.Columns()) == 0 {
		return arrow.NewSchema(nil, nil), nil
	}
	if st, ok := p.e.sess.open[p.ps]; ok && st.kind == returnsRows {
		schema, err := st.stmt.ArrowSchema()
		if err != nil {
			return nil, err
		}
		return renameSchema(schema, st.names), nil
	}
	return TextSchema(p.Columns()), nil
}

// Run runs the statement with params, one value for each parameter (nil
// for NULL), and sends its result to out. A cancellation of ctx cancels
// the statement.
func (p *PreparedStatement) Run(ctx context.Context, params []any, out ResultWriter) error {
	args, err := p.e.bind(p.Params(), params)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, sessionKey{}, p.e.sess)
	return p.e.runStatement(ctx, p.ps, args, out)
}

// Close releases the statement.
func (p *PreparedStatement) Close() error { return p.e.sess.release(p.ps) }

// bind renders values as the text-format parameters a SQL client would
// send for parameters of types oids, so the statement decodes and checks
// them as it does a client's.
func (e *EmbeddedSession) bind(oids []uint32, values []any) ([]wire.Parameter, error) {
	if len(values) != len(oids) {
		return nil, pgerror.WithCode(fmt.Errorf("the statement has %d parameters, but %d values were bound", len(oids), len(values)),
			codes.ProtocolViolation)
	}
	params := make([]wire.Parameter, len(values))
	for i, v := range values {
		var text []byte
		if v != nil {
			// A non-nil buffer: pgx returns nil for NULL only.
			b, err := e.types.Encode(oids[i], pgtype.TextFormatCode, v, []byte{})
			if err != nil || b == nil {
				// Let the parameter's decoder, or DuckDB, parse the value.
				b = []byte(textValue(v))
			}
			text = b
		}
		params[i] = wire.NewParameter(e.types, wire.TextFormat, text)
	}
	return params, nil
}

func textValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	}
	return fmt.Sprint(v)
}
