package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// TestVectorIcebergColumns stores Snowflake's VECTOR columns, and DuckDB's
// fixed-size arrays, in Iceberg tables, which hold them as lists, and reads
// them with the vector functions.
func TestVectorIcebergColumns(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	ctx := context.Background()
	types := func(name string) string {
		t.Helper()
		cat, err := catalog.Open(ctx, env.direct)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := cat.LoadTable(ctx, catalog.Ident(env.direct.Catalog.Schema, name))
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, f := range tbl.Schema().Fields() {
			out = append(out, f.Name+" "+f.Type.String())
		}
		return strings.Join(out, ", ")
	}
	check := func(got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("got\n  %s\nwant\n  %s", got, want)
		}
	}
	query := func(conn *pgx.Conn, sql string) string {
		t.Helper()
		return strings.Join(rawTexts(t, conn, sql), "\n")
	}

	sf := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	mustExec(t, sf, `create table embeddings (id int, v vector(float, 3), counts vector(int, 2))`)
	check(types("embeddings"), "id long, v list<float>, counts list<int>")
	mustExec(t, sf, `insert into embeddings select 1, [1, 2, 3]::vector(float, 3), [1, 2]::vector(int, 2)
		union all select 2, [3, 2, 1]::vector(float, 3), [0, 4]::vector(int, 2)`)
	check(query(sf, `select id, vector_inner_product(v, [1, 0, 0]::vector(float, 3)), vector_l1_distance(v, [1, 2, 3]::vector(float, 3)),
		vector_l2_distance(counts, [0, 0]::vector(int, 2)) from embeddings order by id`), "1|1|0|2.23606797749979\n2|3|4|4")
	// The query that picks the nearest, as a search does.
	check(query(sf, `select id from embeddings order by vector_cosine_similarity(v, [1, 2, 2.9]::vector(float, 3)) desc limit 1`), "1")
	_, err := sf.Exec(ctx, `select vector_cosine_similarity(v, [1, 2]::vector(float, 2)) from embeddings`)
	if err == nil || !strings.Contains(err.Error(), "vectors of different dimensions") {
		t.Errorf("vectors of different dimensions: %v", err)
	}
	mustExec(t, sf, `create table embeddings_copy as select id, v::vector(float, 3) as v from embeddings`)
	check(types("embeddings_copy"), "id long, v list<float>")
	check(query(sf, `select v from embeddings_copy order by id`), "[1,2,3]\n[3,2,1]")

	// DuckDB's dialect: fixed-size arrays are lists too.
	duck := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, duck, `CREATE TABLE arrays (id INT, v FLOAT[2])`)
	check(types("arrays"), "id int, v list<float>")
	mustExec(t, duck, `CREATE TABLE arrays_ctas AS SELECT 1 AS id, [1, 2]::FLOAT[2] AS v`)
	check(types("arrays_ctas"), "id int, v list<float>")
	check(query(duck, `SELECT array_cosine_similarity(v::FLOAT[2], [2, 4]::FLOAT[2]) FROM arrays_ctas`), "1")
}
