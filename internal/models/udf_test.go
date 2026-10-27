package models

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/ml"
)

// testTable makes a table of leads: region, spend, converted.
func testTable(n int) *Table {
	t := &Table{N: n}
	region := &TableCol{Name: "region", Type: TypeString, TypeName: "VARCHAR"}
	spend := &TableCol{Name: "spend", Type: TypeNumber, TypeName: "DOUBLE"}
	conv := &TableCol{Name: "converted", Type: TypeBool, TypeName: "BOOLEAN"}
	for i := range n {
		r := []string{"north", "south", "east", "west"}[i%4]
		s := float64((i*7919)%1000) / 10
		region.Str, region.Null = append(region.Str, r), append(region.Null, false)
		spend.Num, spend.Null = append(spend.Num, s), append(spend.Null, false)
		c := "false"
		if s > 60 || r == "south" {
			c = "true"
		}
		conv.Str, conv.Null = append(conv.Str, c), append(conv.Null, false)
	}
	t.Cols = []*TableCol{region, spend, conv}
	return t
}

// scoringDB opens DuckDB with the scoring functions of a runtime that holds
// a classifier of testTable, and returns the classifier's SHA-256.
func scoringDB(t *testing.T) (*sql.DB, string, *Classifier) {
	t.Helper()
	c, err := TrainClassifier(testTable(800), "converted", ClassifierConfig{Evaluate: true})
	if err != nil {
		t.Fatal(err)
	}
	blob, sha, err := (&Artifact{Class: ClassClassification, Classifier: c}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	r := NewRuntime()
	mem := NewMemory()
	r.SetBlobs(mem)
	if err := mem.Put(context.Background(), sha, blob); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := r.RegisterServer(conn); err != nil {
		t.Fatal(err)
	}
	return db, sha, c
}

func TestPredictFunction(t *testing.T) {
	db, sha, c := scoringDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE leads AS SELECT ['north', 'south', 'east', 'west'][1 + i % 4] AS region,
		((i * 7919) % 1000) / 10.0 AS spend FROM range(0, 5000) t(i)`); err != nil {
		t.Fatal(err)
	}
	var n int
	q := fmt.Sprintf(`SELECT count(*) FROM leads WHERE CAST(igloo_ml_predict('%s', region, CAST(spend AS DOUBLE)) AS JSON)->>'class' =
		CASE WHEN spend > 60 OR region = 'south' THEN 'True' ELSE 'False' END`, sha)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 4950 {
		t.Errorf("%d of 5000 rows predicted right", n)
	}
	// NULLs are missing values; a category training never saw is "other".
	var j string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT igloo_ml_predict('%s', 'mars', NULL)`, sha)).Scan(&j); err != nil {
		t.Fatal(err)
	}
	if len(j) < 40 || j[:10] != `{"class":"` {
		t.Errorf("prediction %s", j)
	}
	// Constant arguments are folded as DuckDB plans the statement, and
	// EXPLAIN shows plans before and after.
	if _, err := db.ExecContext(ctx, "SET explain_output = 'all'"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`EXPLAIN (FORMAT JSON) SELECT igloo_ml_predict('%s', 'south', 1.0::DOUBLE)`,
		`EXPLAIN (FORMAT JSON) SELECT CAST(igloo_ml_predict('%s', CAST('south' AS VARCHAR), TRY_CAST(NULL AS DOUBLE)) AS JSON)`,
		`EXPLAIN (FORMAT JSON) SELECT json_extract_string(CAST(igloo_ml_predict('%s', CAST('south' AS VARCHAR), TRY_CAST(NULL AS DOUBLE)) AS JSON), '$.class')`,
	} {
		rows, err := db.QueryContext(ctx, fmt.Sprintf(q, sha))
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	for _, bad := range []string{
		"SELECT igloo_ml_predict('nope', 'south', 1.0)",
		fmt.Sprintf("SELECT igloo_ml_predict('%s', 'south')", sha),
		"SELECT igloo_ml_predict(region, 'south', 1.0) FROM leads",
	} {
		if _, err := db.ExecContext(ctx, bad); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
	_ = c
	_ = ml.MissingBin
}
