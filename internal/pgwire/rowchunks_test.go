package pgwire

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// chunkQueries are results of every type Igloo sends, with the values at
// the edges of what sending rows a chunk at a time writes itself: those it
// writes, and those it leaves to the way a row at a time is written.
func chunkQueries() []string {
	queries := []string{
		`SELECT * FROM (VALUES
			(true, (-128)::TINYINT, (-32768)::SMALLINT, (-2147483648)::INTEGER, (-9223372036854775807 - 1)::BIGINT, 255::UTINYINT, 65535::USMALLINT, 4294967295::UINTEGER),
			(false, 127::TINYINT, 32767::SMALLINT, 2147483647::INTEGER, 9223372036854775807::BIGINT, 0::UTINYINT, 0::USMALLINT, 0::UINTEGER),
			(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), (true, 0, 0, 0, 0, 1, 1, 1), (false, -1, -1, -1, -1, 9, 9, 9))
			t(b, i8, i16, i32, i64, u8, u16, u32)`,
		`SELECT unnest([0.0, -0.0, 1.5, -1.5, 1e20, 1e-7, 123456789.125, 0.1, 1.0 / 3, 1.7976931348623157e308, 5e-324, 1e21, 1e-320,
			'NaN', 'Infinity', '-Infinity', NULL]::DOUBLE[]) AS f8`,
		`SELECT unnest([0.0, -0.0, 1.5, -1.5, 1e20, 1e-7, 16777217, 0.1, 1.0 / 3, 3.4028235e38, 1e-45, 'NaN', 'Infinity', '-Infinity', NULL]::FLOAT[]) AS f4`,
		`SELECT s, length(s) AS n FROM (VALUES (''), ('a'), ('twelve bytes'), ('thirteen byte'), (repeat('x', 5000)), ('naïve ☃ 日本語'),
			('quote '' back\slash "double"'), (E'tab\there\nnewline'), (NULL)) t(s)`,
		`SELECT d FROM (VALUES (DATE '0001-01-01'), (DATE '9999-12-31'), (DATE '1970-01-01'), (DATE '1969-12-31'), (DATE '2000-02-29'),
			(DATE '1900-02-28'), (DATE '1900-03-01'), (DATE '2100-02-28'), (DATE '2400-02-29'), (DATE '0001-12-31'), (DATE '1582-10-04'),
			(DATE '1582-10-15'), (DATE '2024-12-31'), (DATE '0400-03-01'), (DATE '0399-12-31'), (NULL)) t(d)`,
		`SELECT t, t::TIMESTAMPTZ AS tz FROM (VALUES (TIMESTAMP '1970-01-01 00:00:00'), (TIMESTAMP '1969-12-31 23:59:59.999999'),
			(TIMESTAMP '2024-01-02 01:02:03.5'), (TIMESTAMP '2024-01-02 01:02:03.000001'), (TIMESTAMP '2024-01-02 01:02:03.123456'),
			(TIMESTAMP '2024-01-02 01:02:03.12'), (TIMESTAMP '2024-01-02 23:59:59'), (TIMESTAMP '0001-01-01 00:00:00'),
			(TIMESTAMP '0001-01-01 00:00:00.000001'), (TIMESTAMP '9999-12-31 23:59:59.999999'), (TIMESTAMP '2000-01-01 00:00:00'),
			(TIMESTAMP '1999-12-31 23:59:59.9'), (NULL)) t(t)`,
		// Rows over several chunks, with NULLs at every position of the
		// validity mask's words, and columns written both ways.
		`SELECT i, i % 2 = 0 AS even, CASE WHEN i % 7 = 0 THEN NULL ELSE i * 3 END AS a,
			CASE WHEN i % 5 = 0 THEN NULL ELSE (i % 100)::SMALLINT END AS b, CASE WHEN i % 3 = 0 THEN NULL ELSE 'row-' || i END AS s,
			CASE WHEN i % 11 = 0 THEN NULL ELSE DATE '1990-01-01' + (i % 20000)::INTEGER END AS d,
			CASE WHEN i % 13 = 0 THEN NULL ELSE make_timestamp(i * 987654321) END AS ts,
			CASE WHEN i % 17 = 0 THEN NULL ELSE CAST(((i * 7919) % 2000001 - 1000000) / 1000.0 AS DECIMAL(12,3)) END AS dec,
			CASE WHEN i % 19 = 0 THEN NULL ELSE i * 1.5 END AS f, CASE WHEN i % 23 = 0 THEN NULL ELSE [i, i + 1] END AS l,
			CASE WHEN i % 29 = 0 THEN NULL ELSE {'a': i, 'b': 'x' || i} END AS st FROM range(7000) t(i)`,
		// Values over the whole of each type's range.
		`SELECT (hash(i) >> 1)::BIGINT - (hash(i + 1) >> 1)::BIGINT AS i64, ((hash(i) % 4294967296)::BIGINT - 2147483648)::INTEGER AS i32,
			make_timestamp((hash(i) % 315537897600000000)::BIGINT - 62135596800000000) AS ts,
			make_timestamptz((hash(i + 2) % 315537897600000000)::BIGINT - 62135596800000000) AS tz,
			DATE '0001-01-01' + (hash(i) % 3652059)::INTEGER AS d, (hash(i) % 1000000)::DOUBLE / (1 + hash(i + 3) % 977) AS f8,
			((hash(i) % 2000001)::DOUBLE / 7 - 100000)::FLOAT AS f4 FROM range(5000) t(i)`,
		// Types and values a row at a time writes.
		`SELECT 1 AS n, UUID '6ba7b810-9dad-11d1-80b4-00c04fd430c8' AS u, '\xDE\xAD\xBE\xEF'::BLOB AS bl,
			INTERVAL '1 year 2 months 3 days 04:05:06.789' AS iv, TIME '12:34:56.789' AS t, TIMETZ '12:34:56+02:00' AS ttz,
			170141183460469231731687303715884105727::HUGEINT AS h, 18446744073709551615::UBIGINT AS ub, [1, 2, NULL] AS l,
			{'a': 1, 'b': 'x'} AS st, MAP {'k': 1} AS m, '{"a": [1, 2]}'::JSON AS j, 'null'::JSON AS jn, '"s"'::JSON AS js,
			'0101'::BIT AS bits, TIMESTAMP_S '2024-01-02 03:04:05' AS ts_s, TIMESTAMP_MS '2024-01-02 03:04:05.123' AS ts_ms,
			TIMESTAMP_NS '2024-01-02 03:04:05.123456789' AS ts_ns, 12345678901234567890.0123456789::DECIMAL(38,10) AS wide,
			'happy'::chunk_mood AS mood, 'text' AS s
		UNION ALL SELECT 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL`,
		// Chunks of long values, which go to the client in several writes.
		`SELECT i, repeat(chr(65 + (i % 26)::INTEGER), 2000 + (i % 1000)::INTEGER) AS s FROM range(3000) t(i)`,
		// Integers sent as numerics, and UUIDs.
		`SELECT h, u, id FROM (VALUES
			(170141183460469231731687303715884105727::HUGEINT, 18446744073709551615::UBIGINT, UUID '6ba7b810-9dad-11d1-80b4-00c04fd430c8'),
			((-170141183460469231731687303715884105727 - 1)::HUGEINT, 0, UUID '00000000-0000-0000-0000-000000000000'),
			(18446744073709551616, 1, UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff'), (-18446744073709551616, 9223372036854775808, UUID '7fffffff-ffff-ffff-ffff-ffffffffffff'),
			(18446744073709551615, 9223372036854775807, UUID '80000000-0000-0000-0000-000000000000'), (-1, 10000, UUID '01234567-89ab-cdef-0123-456789abcdef'),
			(0, 9999, NULL), (10000000000000000000, 42, NULL), (9999999999999999999, 7, NULL), (-10000000000000000000000000000, 8, NULL),
			(NULL, NULL, NULL)) t(h, u, id)`,
		`SELECT (hash(i) >> 2)::HUGEINT * (hash(i + 1) >> 2)::HUGEINT - (hash(i + 2) >> 2)::HUGEINT * (hash(i + 3) >> 2)::HUGEINT AS h,
			hash(i) AS u, md5(i::VARCHAR)::UUID AS id, ((hash(i) >> 20)::HUGEINT * (hash(i + 5) >> 21)::HUGEINT)::DECIMAL(38,11) AS d
			FROM range(3000) t(i)`,
		`SELECT i, 'x' AS s FROM range(0) t(i)`,
		`SELECT NULL AS n, NULL::INTEGER AS i, 1 AS one`,
	}
	// Dates and timestamps beyond the years 1 to 9999, and DuckDB's
	// infinities, each in a statement of its own: one that fails, fails
	// both ways.
	for _, v := range []string{
		"DATE '0044-03-15 (BC)'", "DATE '0001-01-01' - 1", "DATE '10000-01-01'", "DATE 'infinity'", "DATE '-infinity'",
		"DATE '5877642-06-25 (BC)'", "DATE '5881580-07-10'", "TIMESTAMP '0044-03-15 (BC) 12:00:00'", "TIMESTAMP '10000-01-01 00:00:00'",
		"TIMESTAMP 'infinity'", "TIMESTAMP '-infinity'", "TIMESTAMP '294247-01-10 04:00:54.775806'", "TIMESTAMP '290309-12-22 (BC) 00:00:00'",
		"TIMESTAMPTZ '0044-03-15 (BC) 12:00:00+00'", "TIMESTAMPTZ '10000-01-01 00:00:00+00'", "TIMESTAMPTZ 'infinity'",
		"TIMESTAMPTZ '-infinity'", "TIMESTAMP '0001-01-01 00:00:00' - INTERVAL 1 MICROSECOND", "TIMESTAMP '9999-12-31 23:59:59.999999' + INTERVAL 1 MICROSECOND",
	} {
		queries = append(queries, "SELECT 'before' AS a, "+v+" AS v, DATE '2024-01-02' AS b")
	}
	// Decimals of each size DuckDB holds them in, at every scale of the
	// narrower and the scales around each count of digits of the wider:
	// the digits around each power of ten thousand, each sign, and others
	// at random.
	rng := rand.New(rand.NewPCG(1, 2))
	for _, width := range []int{4, 9, 18, 19, 28, 38} {
		for scale := 0; scale <= width; scale++ {
			if width > 18 && scale > 5 && scale%4 > 1 && scale < width-1 {
				continue
			}
			values := []string{"0", "1", "9", "10", "99", "100", "101", "999", "1000", "9999", "10000", "10001", "12345", "99999999",
				"100000000", "123456789012", "18446744073709551615", "18446744073709551616", "9999999999999999999", "10000000000000000000",
				strings.Repeat("9", width), strings.Repeat("9", width-1), "1" + strings.Repeat("0", width-1)}
			for range 60 {
				digits := []byte{byte('1' + rng.IntN(9))}
				for range rng.IntN(width) {
					digits = append(digits, byte('0'+rng.IntN(10)))
				}
				values = append(values, string(digits))
			}
			var rows []string
			for _, digits := range values {
				if len(digits) > width {
					continue
				}
				if len(digits) <= scale {
					digits = strings.Repeat("0", scale-len(digits)+1) + digits
				}
				point := len(digits) - scale
				for _, sign := range []string{"", "-"} {
					rows = append(rows, "('"+sign+digits[:point]+"."+digits[point:]+"')")
				}
			}
			rows = append(rows, "(NULL)")
			queries = append(queries, fmt.Sprintf("SELECT v::DECIMAL(%d,%d) AS v FROM (VALUES %s) t(v)", width, scale, strings.Join(rows, ", ")))
		}
	}
	return queries
}

// chunkResult is what a client got for a statement: its rows' values as
// they were sent, its command tag, and its error.
type chunkResult struct {
	rows [][][]byte
	tag  string
	err  string
}

// readChunkResult runs sql and returns what the server sent for it, in the
// simple protocol (format -1), or the extended with every column in text
// (0) or binary (1).
func readChunkResult(conn *pgx.Conn, sql string, format int16) chunkResult {
	ctx := context.Background()
	var res *pgconn.Result
	if format < 0 {
		all, err := conn.PgConn().Exec(ctx, sql).ReadAll()
		if len(all) == 0 {
			return chunkResult{err: fmt.Sprint(err)}
		}
		res = all[0]
		res.Err = err
	} else {
		res = conn.PgConn().ExecParams(ctx, sql, nil, nil, nil, []int16{format}).Read()
	}
	out := chunkResult{rows: res.Rows, tag: res.CommandTag.String()}
	if res.Err != nil {
		out.err = res.Err.Error()
	}
	return out
}

// TestChunkRowsBytes checks that rows sent a chunk at a time are, byte for
// byte, those sent a row at a time, in text and in binary, for every type
// and the values at the edges of each.
func TestChunkRowsBytes(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TYPE memory.main.chunk_mood AS ENUM ('sad', 'ok', 'happy')")
	mustExec(t, conn, "SET search_path = 'memory.main'")
	defer rowChunks.Store(0)
	for _, sql := range chunkQueries() {
		for _, format := range []int16{-1, 0, 1} {
			rowChunks.Store(-1)
			sent := chunksSent.Load()
			want := readChunkResult(conn, sql, format)
			if n := chunksSent.Load() - sent; n != 0 {
				t.Fatalf("a row at a time sent %d chunks", n)
			}
			if want.err != "" {
				t.Errorf("format %d of %.200s fails a row at a time: %s", format, sql, want.err)
			}
			// Every chunk whole, and the first one or two a row at a time, as
			// while the result cache takes a result's first rows.
			for _, first := range []int64{0, 1, 2} {
				rowChunks.Store(first)
				sent := chunksSent.Load()
				got := readChunkResult(conn, sql, format)
				// A chunk has at most 2,048 rows.
				if int64(len(want.rows)) > first*2048 && chunksSent.Load() == sent {
					t.Errorf("format %d, %d chunks a row at a time: no chunk was sent whole for the %d rows of %.200s", format, first, len(want.rows), sql)
				}
				if got.err != want.err || got.tag != want.tag {
					t.Errorf("format %d, %d chunks a row at a time, of %.200s:\n a chunk at a time: %q %q\n   a row at a time: %q %q",
						format, first, sql, got.tag, got.err, want.tag, want.err)
					continue
				}
				if len(got.rows) != len(want.rows) {
					t.Errorf("format %d, %d chunks a row at a time, of %.200s: %d rows a chunk at a time, %d a row at a time",
						format, first, sql, len(got.rows), len(want.rows))
					continue
				}
				for i := range want.rows {
					if !reflect.DeepEqual(got.rows[i], want.rows[i]) {
						t.Errorf("format %d, %d chunks a row at a time, of %.200s, row %d:\n a chunk at a time: %q\n   a row at a time: %q",
							format, first, sql, i, got.rows[i], want.rows[i])
						break
					}
				}
			}
		}
	}
}

// TestChunkRowsError checks that a statement that fails as its rows stream
// fails the same way a chunk at a time, after rows of whole chunks.
func TestChunkRowsError(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	const sql = "SELECT CASE WHEN i = 6000 THEN error('boom') ELSE i END AS i FROM range(10000) t(i)"
	defer rowChunks.Store(0)
	rowChunks.Store(-1)
	want := readChunkResult(conn, sql, -1)
	rowChunks.Store(0)
	got := readChunkResult(conn, sql, -1)
	if want.err == "" || got.err != want.err {
		t.Errorf("a chunk at a time failed with %q, a row at a time with %q", got.err, want.err)
	}
	if count(t, conn, "SELECT 42") != 42 {
		t.Error("the session does not run statements after the failure")
	}
}

// TestChunkRowsCopy checks that COPY ... TO STDOUT sends the same bytes a
// chunk at a time as a row at a time, in each of its formats, with
// delimiters and NULLs that make values need quoting.
func TestChunkRowsCopy(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TYPE memory.main.chunk_mood AS ENUM ('sad', 'ok', 'happy')")
	mustExec(t, conn, "SET search_path = 'memory.main'")
	ctx := context.Background()
	read := func(sql string) (data []byte, tag, failure string) {
		var buf bytes.Buffer
		res, err := conn.PgConn().CopyTo(ctx, &buf, sql)
		if err != nil {
			failure = err.Error()
		}
		return buf.Bytes(), res.String(), failure
	}
	defer rowChunks.Store(0)
	for _, sql := range chunkQueries() {
		for _, options := range []string{"(FORMAT binary)", "", "(FORMAT csv, HEADER)", "(FORMAT csv, DELIMITER '1', NULL '-')",
			"(FORMAT text, DELIMITER '-', NULL '0')"} {
			copy := "COPY (" + sql + ") TO STDOUT " + options
			rowChunks.Store(-1)
			want, wantTag, wantErr := read(copy)
			rowChunks.Store(0)
			sent := chunksSent.Load()
			got, tag, err := read(copy)
			if wantErr == "" && wantTag != "COPY 0" && chunksSent.Load() == sent {
				t.Errorf("%s: no chunk was sent for %.200s", options, sql)
			}
			if wantErr != "" {
				t.Errorf("%s of %.200s fails a row at a time: %s", options, sql, wantErr)
			}
			if err != wantErr || tag != wantTag {
				t.Errorf("%s of %.200s:\n a chunk at a time: %q %q\n   a row at a time: %q %q", options, sql, tag, err, wantTag, wantErr)
				continue
			}
			if !bytes.Equal(got, want) {
				at := 0
				for at < len(got) && at < len(want) && got[at] == want[at] {
					at++
				}
				t.Errorf("%s of %.200s: %d bytes a chunk at a time, %d a row at a time, differing at %d:\n a chunk at a time: %q\n   a row at a time: %q",
					options, sql, len(got), len(want), at, got[max(0, at-40):min(len(got), at+40)], want[max(0, at-40):min(len(want), at+40)])
			}
		}
	}
}

// TestChunkRowsResultCache checks the rows of results the result cache
// takes: one it keeps goes a row at a time, as the values it keeps, and one
// past what it keeps goes on a chunk at a time; both are the rows a session
// without the cache gets a row at a time, the first time and again.
func TestChunkRowsResultCache(t *testing.T) {
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		// Keeps results of up to 64 KiB.
		o.ResultCacheSize = 1 << 20
		o.Metrics = metrics
	})
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.wide AS SELECT i AS id, i % 7 AS k, 'row-' || i AS s FROM range(20000) r(i)")
	uncached := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, uncached, "SET igloo_result_cache = off")
	outcome := func(o string) float64 { return testutil.ToFloat64(metrics.resultCache.WithLabelValues(o)) }
	defer rowChunks.Store(0)
	for _, q := range []struct {
		sql  string
		kept bool
	}{
		{"SELECT id, k, s FROM lake.public.wide WHERE id < 100 ORDER BY id", true},
		{"SELECT id, k, s FROM lake.public.wide ORDER BY id", false},
	} {
		rowChunks.Store(-1)
		want := readChunkResult(uncached, q.sql, -1)
		if want.err != "" {
			t.Fatalf("%s: %s", q.sql, want.err)
		}
		rowChunks.Store(0)
		for _, run := range []string{"first", "second"} {
			sent, hits, kept := chunksSent.Load(), outcome("hit"), outcome("kept")
			got := readChunkResult(admin, q.sql, -1)
			if got.err != "" || got.tag != want.tag || !reflect.DeepEqual(got.rows, want.rows) {
				t.Errorf("the %s run of %s: %d rows, %q %q, want %d rows, %q", run, q.sql, len(got.rows), got.tag, got.err, len(want.rows), want.tag)
			}
			whole := chunksSent.Load() - sent
			switch {
			case q.kept && whole != 0:
				t.Errorf("the %s run of %s, which the cache keeps, sent %d chunks whole", run, q.sql, whole)
			case q.kept && run == "first" && outcome("kept") != kept+1:
				t.Errorf("the cache did not keep %s", q.sql)
			case q.kept && run == "second" && outcome("hit") != hits+1:
				t.Errorf("the second run of %s was not sent from the cache", q.sql)
			case !q.kept && whole == 0:
				t.Errorf("the %s run of %s, past what the cache keeps, sent no chunk whole", run, q.sql)
			case !q.kept && outcome("kept") != kept:
				t.Errorf("the cache kept %s", q.sql)
			}
		}
	}
}

// chunkedQueries are results of several chunks, of columns written both
// ways, with NULLs.
func chunkedQueries() []string {
	var queries []string
	for _, sql := range chunkQueries() {
		if strings.Contains(sql, "range(7000)") || strings.Contains(sql, "range(5000)") {
			queries = append(queries, sql)
		}
	}
	return queries
}

// TestChunkRowsCursor checks that FETCH sends a cursor's rows a chunk at a
// time as it does a row at a time, whatever the counts it is asked for:
// parts of chunks, whole ones and several.
func TestChunkRowsCursor(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	defer rowChunks.Store(0)
	for _, sql := range chunkedQueries() {
		for _, format := range []int16{-1, 0, 1} {
			for _, counts := range [][]string{{"ALL"}, {"1", "10", "ALL"}, {"2048"}, {"2047", "1", "2049"}, {"3000"}, {"100000"}} {
				// fetch reads the whole cursor, with the counts in turn.
				fetch := func(first int64) (results []chunkResult) {
					rowChunks.Store(first)
					mustExec(t, conn, "BEGIN")
					mustExec(t, conn, "DECLARE c CURSOR FOR "+sql)
					for i := 0; ; i++ {
						res := readChunkResult(conn, "FETCH FORWARD "+counts[i%len(counts)]+" FROM c", format)
						results = append(results, res)
						if len(res.rows) == 0 || res.err != "" {
							break
						}
					}
					mustExec(t, conn, "COMMIT")
					return results
				}
				want := fetch(-1)
				sent := chunksSent.Load()
				got := fetch(0)
				if chunksSent.Load() == sent {
					t.Errorf("format %d, FETCH %v: no chunk was sent for %.100s", format, counts, sql)
				}
				if len(got) != len(want) {
					t.Errorf("format %d, FETCH %v of %.100s: %d fetches a chunk at a time, %d a row at a time", format, counts, sql, len(got), len(want))
					continue
				}
				for i := range want {
					if want[i].err != "" || got[i].err != want[i].err || got[i].tag != want[i].tag || !reflect.DeepEqual(got[i].rows, want[i].rows) {
						t.Errorf("format %d, FETCH %v of %.100s, fetch %d: %q %q with %d rows a chunk at a time, %q %q with %d a row at a time",
							format, counts, sql, i, got[i].tag, got[i].err, len(got[i].rows), want[i].tag, want[i].err, len(want[i].rows))
						break
					}
				}
			}
		}
	}
}

// TestChunkRowsPortal checks that a portal read with row limits, as JDBC's
// fetch size reads one, sends its rows a chunk at a time as it does a row
// at a time: as many rows for each Execute, whatever the limits, with
// another statement run between two reads.
func TestChunkRowsPortal(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	h, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Conn.Close() //nolint:errcheck
	_ = h.Conn.SetDeadline(time.Now().Add(5 * time.Minute))
	fe := h.Frontend
	send := func(msgs ...pgproto3.FrontendMessage) {
		t.Helper()
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// receive reads messages up to one of a type in until, which it
	// returns, with the data rows before it.
	receive := func(until ...string) (rows [][][]byte, last string) {
		t.Helper()
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			switch m := msg.(type) {
			case *pgproto3.DataRow:
				row := make([][]byte, len(m.Values))
				for i, v := range m.Values {
					if v != nil {
						row[i] = append([]byte{}, v...)
					}
				}
				rows = append(rows, row)
			case *pgproto3.ErrorResponse:
				t.Fatalf("error: %s", m.Message)
			}
			if name := strings.TrimPrefix(fmt.Sprintf("%T", msg), "*pgproto3."); slices.Contains(until, name) {
				return rows, name
			}
		}
	}
	defer rowChunks.Store(0)
	for _, sql := range chunkedQueries() {
		for _, format := range []int16{0, 1} {
			for _, limits := range [][]uint32{{1}, {10}, {2048}, {2047, 1, 2049}, {5000}, {3, 0}, {100000}} {
				// read reads the whole portal, an Execute for each limit in
				// turn, and returns each Execute's rows.
				read := func(first int64) (reads [][][][]byte) {
					rowChunks.Store(first)
					send(&pgproto3.Query{String: "BEGIN"})
					receive("ReadyForQuery")
					send(&pgproto3.Parse{Query: sql}, &pgproto3.Bind{DestinationPortal: "p", ResultFormatCodes: []int16{format}})
					for i := 0; ; i++ {
						if i == 1 {
							// Another statement between the portal's reads.
							send(&pgproto3.Parse{Name: "other", Query: "SELECT 42"}, &pgproto3.Bind{PreparedStatement: "other"},
								&pgproto3.Execute{}, &pgproto3.Close{ObjectType: 'S', Name: "other"}, &pgproto3.Flush{})
							receive("CloseComplete")
						}
						send(&pgproto3.Execute{Portal: "p", MaxRows: limits[i%len(limits)]}, &pgproto3.Flush{})
						rows, last := receive("PortalSuspended", "CommandComplete")
						reads = append(reads, rows)
						if last == "CommandComplete" {
							break
						}
					}
					send(&pgproto3.Sync{})
					receive("ReadyForQuery")
					send(&pgproto3.Query{String: "COMMIT"})
					receive("ReadyForQuery")
					return reads
				}
				want := read(-1)
				sent := chunksSent.Load()
				got := read(0)
				if chunksSent.Load() == sent {
					t.Errorf("format %d, limits %v: no chunk was sent for %.100s", format, limits, sql)
				}
				if len(got) != len(want) {
					t.Errorf("format %d, limits %v of %.100s: %d reads a chunk at a time, %d a row at a time", format, limits, sql, len(got), len(want))
					continue
				}
				for i := range want {
					if !reflect.DeepEqual(got[i], want[i]) {
						t.Errorf("format %d, limits %v of %.100s, read %d: %d rows a chunk at a time, %d a row at a time, or other values",
							format, limits, sql, i, len(got[i]), len(want[i]))
						break
					}
				}
			}
		}
	}
}
