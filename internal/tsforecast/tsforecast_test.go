package tsforecast

import (
	"context"
	"database/sql"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/modelpack"
	"github.com/akkio-inc/igloo/internal/tsforecast/tsforecasttest"
)

func day(n int) int64 {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, n).UnixMicro()
}

func TestLayout(t *testing.T) {
	ts := []int64{day(3), day(0), day(1), day(1), day(4)}
	ys := []float64{4, 1, 2, 3, math.NaN()}
	g, err := Layout(ts, ys, nil)
	if err != nil {
		t.Fatal(err)
	}
	if g.Freq.Name != "1 day" || len(g.Values) != 5 || g.Points != 3 || g.Values[1] != 2.5 || !isNaN(g.Values[2]) || !isNaN(g.Values[4]) {
		t.Errorf("grid %+v", g)
	}
	if g.End() != day(5) {
		t.Errorf("end %v", time.UnixMicro(g.End()))
	}
	months := []int64{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro(), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).UnixMicro(),
		time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).UnixMicro()}
	g, err = Layout(months, []float64{1, 2, 3}, nil)
	if err != nil || g.Freq.Months != 1 || time.UnixMicro(g.At(3)).UTC().Month() != time.April {
		t.Errorf("months %+v %v", g, err)
	}
	if _, err := Layout([]int64{day(0)}, []float64{1}, nil); err == nil {
		t.Error("one point laid out")
	}
	for in, want := range map[string]string{"D": "1 day", "15min": "15 minutes", "H": "1 hour", "MS": "1 month", "W": "1 week",
		"1 day": "1 day", "2 hours": "2 hours", "Q": "1 quarter"} {
		f, err := ParseFrequency(in)
		if err != nil || f.Name != want {
			t.Errorf("%s: %v %v", in, f, err)
		}
	}
	if f, err := ParseFrequency("auto"); f != nil || err != nil {
		t.Errorf("auto: %v %v", f, err)
	}
	if _, err := ParseFrequency("fortnight"); err == nil {
		t.Error("fortnight read")
	}
}

func TestModelNames(t *testing.T) {
	dir := t.TempDir()
	tsforecasttest.InstallTiny(t, dir, "test-bolt")
	s := New(modelpack.Open(dir))
	for in, want := range map[string]string{"TimesFM 2.5": "timesfm-2.5", "chronos-2": "chronos-2", "Test_Bolt": "test-bolt"} {
		if got, ok := s.Model(in); !ok || got != want {
			t.Errorf("%s: %q %v", in, got, ok)
		}
	}
	if _, ok := s.Model("prophet"); ok {
		t.Error("prophet resolved")
	}
	if _, err := s.MaxContext("timesfm-2.5"); err == nil || !strings.Contains(err.Error(), "igloo models install timesfm-2.5") {
		t.Errorf("not installed: %v", err)
	}
	if _, err := s.MaxContext("timesfm-3.0"); err == nil || !strings.Contains(err.Error(), "non-commercial") {
		t.Errorf("TimesFM 3.0: %v", err)
	}
	if n, err := s.MaxContext("test-bolt"); err != nil || n != 64 {
		t.Errorf("context %d %v", n, err)
	}
}

func duckWith(t *testing.T, svc *Service) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if err := Register(conn, svc); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestFunctionsInDuckDB(t *testing.T) {
	dir := t.TempDir()
	tsforecasttest.InstallTiny(t, dir, "test-bolt")
	db := duckWith(t, New(modelpack.Open(dir)))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE sales AS
		SELECT 'store_' || (i % 3) AS store, TIMESTAMP '2026-01-01' + INTERVAL (i // 3) DAY AS day,
		       10 + (i % 3) + 3 * sin(2 * pi() * (i // 3) / 7) AS amount
		FROM range(3 * 60) t(i) WHERE NOT (i % 3 = 2 AND i // 3 > 1)`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT store, f.kind, f.ts, f.value, f.lower, f.upper, f.status FROM (
		SELECT store, unnest(igloo_ts_forecast('test-bolt', list({'t': day, 'y': amount}),
		                                       '{"horizon": 5, "confidence": 0.8}')) AS f
		FROM sales GROUP BY store) ORDER BY store, f.ts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close() //nolint:errcheck
	n := 0
	for rows.Next() {
		var store, kind, status string
		var ts sql.NullTime
		var value, lower, upper sql.NullFloat64
		if err := rows.Scan(&store, &kind, &ts, &value, &lower, &upper, &status); err != nil {
			t.Fatal(err)
		}
		n++
		if store == "store_2" {
			// Two points: forecast, as min_points is 2.
			continue
		}
		if kind != "forecast" || status != "" || !value.Valid || lower.Float64 > value.Float64 || upper.Float64 < value.Float64 {
			t.Errorf("%s: %s %v %v %v %v %q", store, kind, ts, value, lower, upper, status)
		}
		if ts.Time.Before(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("%s: a forecast at %v, within the history", store, ts.Time)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 15 {
		t.Errorf("%d rows, want 5 for each of 3 stores", n)
	}

	// A series too short for min_points has a status row.
	var status string
	if err := db.QueryRowContext(ctx, `SELECT unnest(igloo_ts_forecast('test-bolt', [{'t': TIMESTAMP '2026-01-01', 'y': 1.0}, {'t': TIMESTAMP '2026-01-02', 'y': 2.0}], '{"horizon": 3, "min_points": 3}')).status`).Scan(&status); err != nil || !strings.Contains(status, "at least 3") {
		t.Errorf("short series: %q %v", status, err)
	}
	// Ending at a timestamp, with the history.
	var hist, fc int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE f.kind = 'history'), count(*) FILTER (WHERE f.kind = 'forecast')
		FROM (SELECT unnest(igloo_ts_forecast('test-bolt', list({'t': day, 'y': amount}),
		      '{"end": `+itoa64(time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro())+`, "history": true}')) AS f
		      FROM sales WHERE store = 'store_0')`).Scan(&hist, &fc); err != nil || hist != 60 || fc != 8 {
		t.Errorf("history %d, forecast %d: %v", hist, fc, err)
	}
	// An unknown model fails the statement.
	if _, err := db.ExecContext(ctx, `SELECT igloo_ts_forecast('prophet', [{'t': TIMESTAMP '2026-01-01', 'y': 1.0}], '{}')`); err == nil ||
		!strings.Contains(err.Error(), "not a forecasting model") {
		t.Errorf("unknown model: %v", err)
	}

	// Anomalies: a spike in the last week of store_0.
	if _, err := db.ExecContext(ctx, `UPDATE sales SET amount = amount + 1000 WHERE store = 'store_0' AND day = TIMESTAMP '2026-02-27'`); err != nil {
		t.Fatal(err)
	}
	var top string
	var flagged bool
	if err := db.QueryRowContext(ctx, `SELECT strftime(d.ts, '%m-%d'), d.is_anomaly FROM (
		SELECT unnest(igloo_ts_detect('test-bolt', list({'t': day, 'y': amount}), [], '{"last_n": 7, "threshold": 0.9}')) AS d
		FROM sales WHERE store = 'store_0') ORDER BY d.probability DESC, d.ts LIMIT 1`).Scan(&top, &flagged); err != nil || top != "02-27" || !flagged {
		t.Errorf("the most anomalous point %s (%v): %v", top, flagged, err)
	}
	var scored, total int
	if err := db.QueryRowContext(ctx, `SELECT count(d.expected), count(*) FROM (
		SELECT unnest(igloo_ts_detect('test-bolt', list({'t': day, 'y': amount}), [],
		       '{"rolling": true, "warmup": 8, "window": 16}')) AS d FROM sales WHERE store = 'store_1')`).Scan(&scored, &total); err != nil ||
		scored != 52 || total != 60 {
		t.Errorf("rolling: %d scored of %d: %v", scored, total, err)
	}
}

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }
