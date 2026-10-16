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

// BenchmarkResults measures sending a large result to a client: as rows,
// and as COPY TO STDOUT in binary.
func BenchmarkResults(b *testing.B) {
	ts := startServer(b)
	conn, err := tryConnect(ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(context.Background()) //nolint:errcheck
	ctx := context.Background()
	b.Run("rows", func(b *testing.B) {
		for range b.N {
			if res := conn.PgConn().Exec(ctx, benchQuery); res.Close() != nil {
				b.Fatal(res.Close())
			}
		}
	})
	b.Run("copy", func(b *testing.B) {
		for range b.N {
			if _, err := conn.PgConn().CopyTo(ctx, io.Discard, "COPY ("+benchQuery+") TO STDOUT (FORMAT binary)"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
