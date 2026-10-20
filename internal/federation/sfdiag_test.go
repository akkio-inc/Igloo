package federation

import (
	"context"
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSnowflakeLiveThroughput measures downloading and encoding a large
// result, apart from DuckDB. It needs IGLOO_TEST_SNOWFLAKE_THROUGHPUT=1 as
// well, and reads TPCH_SF1.LINEITEM (served from Snowflake's result cache
// when the same query ran within a day).
func TestSnowflakeLiveThroughput(t *testing.T) {
	if os.Getenv("IGLOO_TEST_SNOWFLAKE_THROUGHPUT") == "" {
		t.Skip("IGLOO_TEST_SNOWFLAKE_THROUGHPUT not set")
	}
	c, secrets := liveSnowflake(t, "TPCH_SF1")
	cl := newSnowflakeClient(c, secrets)
	defer cl.close()
	ctx := context.Background()
	cat, err := cl.loadCatalog(ctx, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	q, err := parseScan(`COPY (SELECT "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment" FROM "tpch_sf1"."lineitem" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");`)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := planScan(cat, q)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	res, err := cl.query(ctx, scan.sql, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer res.close()
	t.Logf("query: %s, %d batches, id %s", time.Since(started), len(res.batches), res.queryID)

	for _, workers := range []int{3, 16} {
		started = time.Now()
		var rows, bytes atomic.Int64
		var encode atomic.Int64
		next := make(chan sfBatch)
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				var buf []byte
				for b := range next {
					recs, err := fetchBatch(ctx, b, 0)
					if err != nil {
						t.Error(err)
						return
					}
					e := time.Now()
					for _, rec := range recs {
						encs, err := encoders(rec, scan.columns)
						if err != nil {
							t.Error(err)
							return
						}
						for r := 0; r < int(rec.NumRows()); r++ {
							buf = binary.BigEndian.AppendUint16(buf[:0], uint16(len(encs)))
							for _, enc := range encs {
								buf, _ = enc(buf, r)
							}
							bytes.Add(int64(len(buf)))
						}
						rows.Add(rec.NumRows())
					}
					encode.Add(int64(time.Since(e)))
					release(recs)
				}
			})
		}
		for _, b := range res.batches {
			next <- b
		}
		close(next)
		wg.Wait()
		t.Logf("%d workers: %d rows, %d MB of COPY data in %s; encoding took %s of it (summed over workers)",
			workers, rows.Load(), bytes.Load()>>20, time.Since(started), time.Duration(encode.Load()))
	}
}
