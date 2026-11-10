package pgwire

import (
	"context"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
)

// benchQuery is a result shaped like an export of ad delivery: integers, a
// date, a decimal and a string.
const benchQuery = `SELECT range AS id, mod(range, 1000) AS campaign_id, DATE '2026-01-01' + mod(range, 365)::INTEGER AS day,
	mod(range, 997) * 1.25 AS spend, 'creative-' || mod(range, 5000) AS creative FROM range(200000)`

// BenchmarkResults measures sending a large result to a client: as rows in
// text and in binary, and as COPY TO STDOUT in binary and in CSV, each a
// chunk at a time and, to compare, a row at a time (see rowchunks.go).
func BenchmarkResults(b *testing.B) {
	ts := startServer(b)
	ctx := context.Background()
	admin := ts.admin(b, pgx.QueryExecModeSimpleProtocol)
	if _, err := admin.Exec(ctx, "CREATE TABLE memory.main.bench_results AS "+benchQuery); err != nil {
		b.Fatal(err)
	}
	const query = "SELECT * FROM memory.main.bench_results"
	conn, err := tryConnect(ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	defer rowChunks.Store(0)
	for _, read := range []struct {
		name string
		run  func() error
	}{
		{"rows", func() error { return conn.PgConn().Exec(ctx, query).Close() }},
		{"binary", func() error {
			_, err := conn.PgConn().ExecParams(ctx, query, nil, nil, nil, []int16{1}).Close()
			return err
		}},
		{"copy", func() error {
			_, err := conn.PgConn().CopyTo(ctx, io.Discard, "COPY ("+query+") TO STDOUT (FORMAT binary)")
			return err
		}},
		{"csv", func() error {
			_, err := conn.PgConn().CopyTo(ctx, io.Discard, "COPY ("+query+") TO STDOUT (FORMAT csv)")
			return err
		}},
	} {
		for _, way := range []string{"chunks", "a-row-at-a-time"} {
			b.Run(read.name+"/"+way, func(b *testing.B) {
				rowChunks.Store(0)
				if way != "chunks" {
					rowChunks.Store(-1)
				}
				for range b.N {
					if err := read.run(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
