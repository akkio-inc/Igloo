package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/sqllex"
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
	// bitmaps says it counts distinct IDs in roaring bitmaps, prepared and
	// run with rb_count_distinct = 'always' (see bitmaps.go).
	bitmaps bool
	// indexPins are the tables it answers counts of from bitmap indexes,
	// at the snapshots it reads, and indexed the statement before that
	// (see bitmaprewrite.go).
	indexPins []indexPin
	indexed   string
	// reportCall says the statement's text calls the report API, which
	// refreshIndexes looks at again (see reportindex.go); reportRan that
	// it has run once since it was prepared.
	reportCall bool
	reportRan  bool
	// original is the statement before policies rewrote it, when they did
	// (see protect.go); guard records how they applied.
	original string
	guard    *guard
	// effect is what the statement, once it succeeds, does to the objects
	// grants name.
	effect *grantEffect
	// created are the catalog objects its CREATE statements make, whose
	// owner its user becomes.
	created []*createdObject
	// changesIceberg says the statement creates, alters or drops a schema,
	// table or view in the Iceberg catalog, so it runs outside the
	// session's transaction block (see outsideTransaction).
	changesIceberg bool
	// staged is set for CREATE [OR REPLACE] TABLE ... AS of an Iceberg
	// table, which Igloo carries out in steps so that filling the table
	// does not hold the catalog lock (see runStagedCreate).
	staged *stagedCreate
	// loads are the catalog's tables DuckDB loaded as it prepared the
	// statement: those it reads (see prepareChecked).
	loads []catalogproxy.Load
	// listsCatalog says the statement walks the catalog's in-memory set of
	// tables or views (information_schema, pg_catalog,
	// duckdb_tables/views/columns, SHOW, DESCRIBE), so it holds the catalog
	// lock exclusively while it runs rather than only for its bind (see
	// catalogRunLock).
	listsCatalog bool
	// unknown says DuckDB could not tell the type of a column of the result
	// when it prepared the statement.
	unknown bool
	// names, when set, are the names clients see for the result's columns,
	// as the Snowflake dialect names them; columns keeps DuckDB's.
	names []string
	// ran says the statement has run, for freshForRun.
	ran bool
	// ids is set for an INSERT into a table with columns in ID domains,
	// which writes their ids too (see idassign.go).
	ids *idWrite
	// idCounts is set for a query that counts member columns through ID
	// domains' ids, or would once their tables have every row's, and text
	// is the statement as prepareChecked got it, to prepare it again when
	// those tables change (see idcount.go).
	idCounts *idCounts
	text     string
	// ai says the statement calls AI functions, which DuckDB bound as it
	// was prepared: they run under an ai.Statement begun before each run
	// (see beginAI).
	ai bool
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
	if st.unknown {
		// Best effort: otherwise the unknown columns go out as text.
		_ = st.describeWithNulls(ctx, conn)
	}
	return st, nil
}

// describeWithNulls describes the result of a statement that DuckDB binds
// only once it has its parameters' values, such as one that calls
// json_object($1, x), as SQLAlchemy's reflection does: until then DuckDB
// reports a single column of unknown type. It prepares the query again with
// each parameter replaced by a NULL of the parameter's type, which DuckDB
// can bind, and takes that statement's columns; st itself runs as prepared.
func (st *statement) describeWithNulls(ctx context.Context, conn *duckdb.Conn) error {
	nulls := sqllex.MapCode(st.prepared, func(code string) string {
		return placeholder.ReplaceAllStringFunc(code, func(m string) string {
			t := "VARCHAR"
			if n, err := strconv.Atoi(m[1:]); err == nil && n >= 1 && n <= len(st.params) {
				if name, ok := paramTypes[st.params[n-1]]; ok {
					t = name
				} else if name, ok := arrayOIDs[st.params[n-1]]; ok {
					t = name
				}
			}
			return "CAST(NULL AS " + t + ")"
		})
	})
	ds, err := conn.PrepareContext(ctx, nulls)
	if err != nil {
		return err
	}
	probe := &statement{stmt: ds.(*duckdb.Stmt), query: nulls}
	defer probe.close() //nolint:errcheck
	if err := probe.describe(nil); err != nil {
		return err
	}
	if probe.unknown || probe.kind != returnsRows {
		return errors.New("the result's columns are still unknown")
	}
	st.columns, st.encoders, st.unknown = probe.columns, probe.encoders, false
	return nil
}

// paramTypes are the DuckDB types of the PostgreSQL types clients declare
// parameters as.
var paramTypes = map[uint32]string{
	pgtype.BoolOID: "BOOLEAN", pgtype.Int2OID: "SMALLINT", pgtype.Int4OID: "INTEGER", pgtype.Int8OID: "BIGINT",
	pgtype.Float4OID: "FLOAT", pgtype.Float8OID: "DOUBLE", pgtype.NumericOID: "DOUBLE", pgtype.TextOID: "VARCHAR",
	pgtype.VarcharOID: "VARCHAR", pgtype.BPCharOID: "VARCHAR", pgtype.DateOID: "DATE", pgtype.TimeOID: "TIME",
	pgtype.TimestampOID: "TIMESTAMP", pgtype.TimestamptzOID: "TIMESTAMPTZ", pgtype.IntervalOID: "INTERVAL",
	pgtype.UUIDOID: "UUID", pgtype.ByteaOID: "BLOB", pgtype.JSONOID: "JSON",
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

// freshForRun prepares st again, on conn, if it is a CREATE statement that
// has run before. DuckDB 1.5.6 runs a prepared CREATE as written only the
// first time: CREATE TABLE ... AS run again makes the table with no
// columns, so that the next statement to read it fails to bind ("the FROM
// clause is missing"). Clients run a prepared statement again whenever
// they reuse one, as psycopg does with a statement it has run five times,
// and as JDBC drivers do.
func (st *statement) freshForRun(ctx context.Context, conn *duckdb.Conn) error {
	if st.stmtType != duckdb.STATEMENT_TYPE_CREATE {
		return nil
	}
	if st.ran {
		if err := st.reprepare(ctx, conn); err != nil {
			return err
		}
	}
	st.ran = true
	return nil
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
			if t, err := st.stmt.ColumnType(i); err == nil && t == duckdb.TYPE_INVALID {
				// DuckDB cannot tell yet (see describeWithNulls). As
				// PostgreSQL does an untyped parameter, send it as text.
				st.columns = append(st.columns, wire.Column{Name: name, Oid: pgtype.TextOID, Width: -1})
				st.encoders = append(st.encoders, toText)
				st.unknown = true
				continue
			}
			info, err := st.stmt.ColumnTypeInfo(i)
			if err != nil {
				return err
			}
			oid, enc := wireType(info.InternalType(), info.Alias())
			col := wire.Column{Name: name, Oid: oid, Width: -1}
			if d, ok := info.Details().(*duckdb.DecimalDetails); ok {
				// PostgreSQL's numeric(p,s) modifier, as pg_attribute has it.
				col.TypeModifier = int32(d.Width)<<16 | int32(d.Scale) + 4
			}
			st.columns = append(st.columns, col)
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
		if st.stmtType == duckdb.STATEMENT_TYPE_CREATE && createsTableAs(st.query) {
			rows, err := st.stmt.QueryContext(ctx, args)
			if err != nil {
				return err
			}
			n, err := countOf(rows)
			if err != nil {
				return err
			}
			reportRows(w, n)
			return w.Complete(commandTag(st.stmtType, st.query, n))
		}
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

	b := newRowBatcher(w, st.columns)
	dest := make([]driver.Value, len(st.columns))
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := b.add(dest, st.encoders); err != nil {
			return err
		}
	}
	if err := b.flush(); err != nil {
		return err
	}
	return w.Complete(commandTag(st.stmtType, st.query, int64(w.Written())))
}

// rowBatch is how many rows go to the client at a time. psql-wire sends a
// batch in a few large writes rather than a network write per row, which
// was the bottleneck for large results; within a portal that a client reads
// a few rows at a time, it still sends them row by row.
const rowBatch = 256

// rowBatcher collects encoded rows for the client.
type rowBatcher struct {
	w       wire.DataWriter
	columns wire.Columns
	rows    [][]any
	values  []any
}

func newRowBatcher(w wire.DataWriter, columns wire.Columns) *rowBatcher {
	return &rowBatcher{w: w, columns: columns, rows: make([][]any, 0, rowBatch), values: make([]any, rowBatch*len(columns))}
}

// add encodes a row scanned from DuckDB and sends the batch once it is full.
func (b *rowBatcher) add(dest []driver.Value, encoders []encoder) error {
	n := len(b.columns)
	row := b.values[len(b.rows)*n : (len(b.rows)+1)*n]
	for i, v := range dest {
		if v == nil {
			row[i] = nil
			continue
		}
		if bytes, ok := v.([]byte); ok {
			// The driver may reuse the bytes for the next row.
			v = append([]byte(nil), bytes...)
		}
		var err error
		if row[i], err = encoders[i](v); err != nil {
			return fmt.Errorf("column %q: %w", b.columns[i].Name, err)
		}
	}
	b.rows = append(b.rows, row)
	if len(b.rows) == rowBatch {
		return b.flush()
	}
	return nil
}

// flush sends the rows collected so far. They are encoded before it
// returns, so their slices are reused.
func (b *rowBatcher) flush() error {
	if len(b.rows) == 0 {
		return nil
	}
	err := wire.WriteRows(b.w, b.rows)
	b.rows = b.rows[:0]
	return err
}

func (st *statement) close() error {
	return st.stmt.Close()
}

// rowsTaker is a writer that takes the rows a statement wrote when its
// command tag does not count them. CREATE TABLE ... AS completes with
// "CREATE TABLE" here, where PostgreSQL sends "SELECT n", and clients
// (Arrow Flight SQL's update count among them) have always seen it so;
// the query history and the task history record its rows all the same.
type rowsTaker interface {
	tookRows(n int64)
}

// reportRows tells w, if it takes them, that the statement wrote n rows.
func reportRows(w wire.DataWriter, n int64) {
	if t, ok := w.(rowsTaker); ok {
		t.tookRows(n)
	}
}

// countOf reads the count a CREATE TABLE ... AS returns, and closes rows.
// DuckDB returns the rows it wrote as a one-row result, not as changed
// rows, which RowsAffected reads (it is 0). 0 when there is no such row.
func countOf(rows driver.Rows) (int64, error) {
	defer rows.Close() //nolint:errcheck
	if len(rows.Columns()) == 0 {
		return 0, nil
	}
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); errors.Is(err, io.EOF) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	n, _ := dest[0].(int64)
	return n, nil
}

// createsTableAs reports whether query is CREATE [OR REPLACE] [TEMP]
// TABLE ... AS: it writes rows, which its command tag does not count.
func createsTableAs(query string) bool {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return false
	}
	sc.keywords("OR", "REPLACE")
	if !sc.keywords("TABLE") && !sc.keywords("TEMP", "TABLE") && !sc.keywords("TEMPORARY", "TABLE") {
		return false
	}
	sc.keywords("IF", "NOT", "EXISTS")
	if _, ok := sc.name(); !ok {
		return false
	}
	return sc.keywords("AS")
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
