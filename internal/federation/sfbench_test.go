package federation

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestSnowflakeGatewayThroughput measures how fast DuckDB reads a large
// result through the gateway, with no network in between: 6 million rows
// of the test table's numbers, dates and text. It needs
// IGLOO_TEST_SNOWFLAKE_THROUGHPUT=1.
func TestSnowflakeGatewayThroughput(t *testing.T) {
	if os.Getenv("IGLOO_TEST_SNOWFLAKE_THROUGHPUT") == "" {
		t.Skip("IGLOO_TEST_SNOWFLAKE_THROUGHPUT not set")
	}
	installExtensions(t)
	const batches, perBatch = 75, 80000
	pool := memory.NewGoAllocator()
	ids := array.NewInt64Builder(pool)
	amts := array.NewInt32Builder(pool)
	names := array.NewStringBuilder(pool)
	dates := array.NewDate32Builder(pool)
	for i := range perBatch {
		ids.Append(int64(i) * 7)
		amts.Append(int32(i % 100000))
		names.Append(strings.Repeat("x", 20+i%25))
		dates.Append(arrow.Date32(8000 + i%3000))
	}
	cols := map[string]arrow.Array{"ID": ids.NewArray(), "AMT": amts.NewArray(), "NAME": names.NewArray(), "D": dates.NewArray()}
	fake := &fakeSnowflake{cat: testCatalog(t)}
	fake.answer = func(ctx context.Context, q string) ([][]arrow.Record, error) {
		if strings.HasPrefix(q, "SELECT COUNT(*)") {
			return [][]arrow.Record{{countRecord(batches * perBatch)}}, nil
		}
		r := recordFor(t, cols, q, perBatch)
		out := make([][]arrow.Record, batches)
		for i := range out {
			out[i] = []arrow.Record{r}
		}
		return out, nil
	}
	open := openSnowflake
	openSnowflake = func(Connection, string) sfBackend { return fake }
	t.Cleanup(func() { openSnowflake = open })
	store := NewMemoryStore()
	c := New("sf", Snowflake)
	c.Account, c.User, c.Database, c.Password = "myorg-acct", "u", "db", "pw"
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	_, eng := startEngine(t, store)
	for _, q := range []string{
		"SELECT count(*), sum(id), max(name), max(d), sum(amt) FROM sf.tpch.t1",
		"CREATE TABLE copy AS SELECT id, amt, name, d FROM sf.tpch.t1",
		// A limit DuckDB cannot pass on to the scan: it stops reading.
		"SELECT x.id FROM sf.tpch.t1 x, (VALUES (1)) y(z) LIMIT 1",
		"SELECT count(*) FROM sf.tpch.t1",
	} {
		started := time.Now()
		got, err := query(t, eng, q)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %s in %s", q, got, time.Since(started))
	}
}
