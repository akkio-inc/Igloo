package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
)

// kind is how a statement's result reaches the client.
type kind int

const (
	// returnsRows statements send a row description and data rows.
	returnsRows kind = iota
	// command statements send only a command tag such as "INSERT 0 5".
	command
)

// statement is a DuckDB prepared statement plus what the wire protocol needs
// to describe and run it.
type statement struct {
	stmt     *duckdb.Stmt
	stmtType duckdb.StmtType
	query    string
	// prepared is the text DuckDB prepared: query with array parameters
	// typed (see typeArrayParams).
	prepared string
	kind     kind
	columns  wire.Columns
	encoders []encoder
	params   []uint32
	// clientOIDs are the parameter types the client declared.
	clientOIDs []uint32
	// access is set when grants allowed the statement (see access.go).
	access *access
	// original is the statement before policies rewrote it, when they did
	// (see protect.go); guard records how they applied.
	original string
	guard    *guard
	// effect is what the statement, once it succeeds, does to the objects
	// grants name.
	effect *grantEffect
	// created is the catalog object a CREATE makes, whose owner its user
	// becomes.
	created *createdObject
}

// prepare compiles query on conn. clientOIDs are parameter types the client
// declared, which take precedence over the types DuckDB infers.
//
// As in DuckDB itself, when query holds several statements every statement
// but the last runs during prepare; the last is what gets described and run.
func prepare(ctx context.Context, conn *duckdb.Conn, query string, clientOIDs []uint32) (*statement, error) {
	typed, oids := typeArrayParams(query, clientOIDs)
	ds, err := conn.PrepareContext(ctx, typed)
	if err != nil {
		return nil, err
	}

	st := &statement{stmt: ds.(*duckdb.Stmt), query: query, prepared: typed, clientOIDs: clientOIDs}
	if err := st.describe(oids); err != nil {
		return nil, errors.Join(err, st.close())
	}
	return st, nil
}

// reprepare prepares st's query again on conn, in place. Igloo does so after
// checking a statement against grants, which binds the statement's table
// functions again on the same connection: DuckDB 1.5.6 then fails a
// statement prepared earlier that calls pragma_table_info on an Iceberg
// table with "basic_string::_M_create" when it runs. The statement must
// keep the columns and parameters clients were told about.
func (st *statement) reprepare(ctx context.Context, conn *duckdb.Conn) error {
	fresh, err := prepare(ctx, conn, st.query, st.clientOIDs)
	if err != nil {
		return err
	}
	if !sameShape(fresh, st, st.original == "") {
		_ = fresh.close()
		return pgerror.WithCode(errors.New("the statement's result changed since it was prepared; prepare it again"),
			codes.FeatureNotSupported)
	}
	old := st.stmt
	st.stmt, st.encoders = fresh.stmt, fresh.encoders
	return old.Close()
}

// sameShape reports whether a and b have the same type, result columns
// and parameters. names compares the columns' names too: a statement that
// policies rewrote names a column for an expression after its rewritten
// text, so it keeps the names the client was given for the original.
func sameShape(a, b *statement, names bool) bool {
	return a.stmtType == b.stmtType && a.kind == b.kind && slices.Equal(a.params, b.params) &&
		slices.EqualFunc(a.columns, b.columns, func(x, y wire.Column) bool { return (!names || x.Name == y.Name) && x.Oid == y.Oid })
}

func (st *statement) describe(clientOIDs []uint32) error {
	var err error
	if st.stmtType, err = st.stmt.StatementType(); err != nil {
		return err
	}

	n, err := st.stmt.ColumnCount()
	if err != nil {
		return err
	}
	names := make([]string, n)
	for i := range names {
		if names[i], err = st.stmt.ColumnName(i); err != nil {
			return err
		}
	}

	st.kind = classify(st.stmtType, names)
	if st.kind == returnsRows {
		for i, name := range names {
			info, err := st.stmt.ColumnTypeInfo(i)
			if err != nil {
				return err
			}
			oid, enc := wireType(info.InternalType(), info.Alias())
			st.columns = append(st.columns, wire.Column{Name: name, Oid: oid, Width: -1})
			st.encoders = append(st.encoders, enc)
		}
	}

	for i := 1; i <= st.stmt.NumInput(); i++ {
		if i <= len(clientOIDs) && clientOIDs[i-1] != 0 {
			st.params = append(st.params, clientOIDs[i-1])
			continue
		}
		t, err := st.stmt.ParamType(i)
		if err != nil {
			return err
		}
		st.params = append(st.params, paramOID(t))
	}

	return nil
}

// classify decides whether a statement sends rows. DuckDB reports affected
// rows as a one-column "Count" result; Postgres puts that number in the
// command tag instead, so only RETURNING clauses and queries send rows.
func classify(t duckdb.StmtType, columns []string) kind {
	if len(columns) == 0 {
		return command
	}
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_EXPLAIN, duckdb.STATEMENT_TYPE_PRAGMA,
		duckdb.STATEMENT_TYPE_CALL, duckdb.STATEMENT_TYPE_RELATION:
		return returnsRows
	case duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE,
		duckdb.STATEMENT_TYPE_EXECUTE:
		if len(columns) == 1 && columns[0] == "Count" {
			return command
		}
		return returnsRows
	}
	return command
}

// run executes the statement with args and streams the result to w.
func (st *statement) run(ctx context.Context, args []driver.NamedValue, w wire.DataWriter) error {
	if st.kind == command {
		res, err := st.stmt.ExecContext(ctx, args)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		return w.Complete(commandTag(st.stmtType, st.query, n))
	}
	if aw := arrowWriterOf(w); aw != nil {
		return st.runArrow(ctx, args, w, aw)
	}

	rows, err := st.stmt.QueryContext(ctx, args)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck

	dest := make([]driver.Value, len(st.columns))
	values := make([]any, len(st.columns))
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		for i, v := range dest {
			if v == nil {
				values[i] = nil
				continue
			}
			if values[i], err = st.encoders[i](v); err != nil {
				return fmt.Errorf("column %q: %w", st.columns[i].Name, err)
			}
		}
		// Row encodes values before returning, so the slices can be reused.
		if err := w.Row(values); err != nil {
			return err
		}
	}

	return w.Complete(commandTag(st.stmtType, st.query, int64(w.Written())))
}

func (st *statement) close() error {
	return st.stmt.Close()
}

// commandTag builds the Postgres command tag, e.g. "SELECT 3" or
// "CREATE TABLE". Drivers parse it to report affected rows.
func commandTag(t duckdb.StmtType, query string, n int64) string {
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_PRAGMA, duckdb.STATEMENT_TYPE_CALL,
		duckdb.STATEMENT_TYPE_RELATION:
		return fmt.Sprintf("SELECT %d", n)
	case duckdb.STATEMENT_TYPE_INSERT:
		return fmt.Sprintf("INSERT 0 %d", n)
	case duckdb.STATEMENT_TYPE_UPDATE:
		return fmt.Sprintf("UPDATE %d", n)
	case duckdb.STATEMENT_TYPE_DELETE:
		return fmt.Sprintf("DELETE %d", n)
	case duckdb.STATEMENT_TYPE_COPY:
		return fmt.Sprintf("COPY %d", n)
	case duckdb.STATEMENT_TYPE_TRANSACTION:
		return transactionTag(query)
	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER:
		return ddlTag(query)
	}

	if words := keywords(query, 1); len(words) == 1 {
		return words[0]
	}
	return "OK"
}

func transactionTag(query string) string {
	words := keywords(query, 1)
	if len(words) == 0 {
		return "OK"
	}
	switch words[0] {
	case "BEGIN":
		return "BEGIN"
	case "START":
		return "START TRANSACTION"
	case "COMMIT", "END":
		return "COMMIT"
	case "ROLLBACK", "ABORT":
		return "ROLLBACK"
	}
	return words[0]
}

// ddlModifiers sit between the verb and the object type, as in
// CREATE OR REPLACE TEMP VIEW.
var ddlModifiers = map[string]bool{
	"OR": true, "REPLACE": true, "TEMP": true, "TEMPORARY": true, "UNIQUE": true, "PERSISTENT": true,
}

func ddlTag(query string) string {
	words := keywords(query, 6)
	if len(words) == 0 {
		return "OK"
	}
	for _, w := range words[1:] {
		if !ddlModifiers[w] {
			return words[0] + " " + w
		}
	}
	return words[0]
}

// transactionChange reports how a successful TRANSACTION statement changes
// the session: +1 when it opens a transaction, -1 when it closes one.
func transactionChange(query string) int {
	switch transactionTag(query) {
	case "BEGIN", "START TRANSACTION":
		return 1
	case "COMMIT", "ROLLBACK":
		return -1
	}
	return 0
}

// isTransactionEnd reports whether query ends a transaction. Postgres treats
// these as ending the transaction even when they fail, and so does Igloo.
func isTransactionEnd(query string) bool {
	return transactionChange(query) < 0
}
