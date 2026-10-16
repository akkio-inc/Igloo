package pgwire

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/config"
)

func TestGDALUse(t *testing.T) {
	refused := []string{
		"SELECT * FROM ST_Read('/etc/passwd')",
		"select * from st_read('x.geojson')",
		`SELECT * FROM "ST_Read"('x')`,
		"SELECT * FROM main.ST_Read('x')",
		"SELECT * FROM ST_Read_Meta('x')",
		"CREATE VIEW v AS SELECT * FROM ST_Read('s3://landing/zips.geojson')",
		"CREATE MACRO m(p) AS TABLE SELECT * FROM st_read(p)",
		// Replacement scans that become ST_Read.
		"SELECT * FROM 's3://landing/dma.gpkg'",
		`SELECT * FROM "/tmp/a.GPKG"`,
		`SELECT * FROM E'/tmp/a.gp\x6bg'`,
		`SELECT * FROM E'/tmp/a.\146gb'`,
		"SELECT * FROM $$x.fgb$$",
		"SELECT * FROM tmp.\"a.fgb\"",
		// GDAL's writer, however the format is spelled.
		"COPY (SELECT 1) TO 'x.geojson' (FORMAT GDAL, DRIVER 'GeoJSON')",
		"COPY t TO 'x' WITH (format 'gdal')",
		`COPY t TO 'x' ("FORMAT" "GDAL")`,
		"COPY t TO 'x' (DRIVER 'GeoJSON', FORMAT gdal)",
		"COPY t TO 'x' (FORMAT getvariable('fmt'))",
		"COPY t TO 'x' (FORMAT 'gd' || 'al')",
		"COPY t TO 'x' (FORMAT (('gdal')))",
		`COPY t TO 'x' (FORMAT E'gd\x61l')`,
		"COPY t TO 'x' (FORMAT $1)",
		"COPY t TO ('/tmp/' || 'x') (FORMAT gdal)",
		"SET VARIABLE fmt = 'gdal'; COPY t TO 'x' (FORMAT getvariable('fmt'))",
		"EXPLAIN ANALYZE COPY t TO 'x' (FORMAT gdal)",
		"PREPARE p AS COPY t TO 'x' (FORMAT 'gdal')",
		"EXPORT DATABASE 'dir' (FORMAT gdal, DRIVER 'GeoJSON')",
	}
	for _, q := range refused {
		if gdalUse(q) == "" {
			t.Errorf("not refused: %s", q)
		}
		if err := forbidden(q); pgCode(err) != "" && pgCode(err) != "42501" || err == nil {
			t.Errorf("forbidden(%q) = %v", q, err)
		}
	}
	allowed := []string{
		"SELECT * FROM ST_ReadSHP('s3://landing/dma.shp')",
		"SELECT * FROM ST_ReadOSM('s3://landing/nyc.osm.pbf')",
		"SELECT ST_Contains(ST_GeomFromText(wkt), ST_Point(lon, lat)) FROM zips",
		"SELECT 'st_read' AS word, 'ST_Read(x)' AS text",
		"SELECT * FROM read_parquet('s3://landing/zips.parquet')",
		"COPY t TO 'x.parquet' (FORMAT parquet)",
		"COPY t TO 'x.csv' (FORMAT 'csv', HEADER)",
		`COPY t TO 'x.csv' ("format" csv)`,
		"COPY (SELECT format FROM t) TO 'x.parquet'",
		"COPY (SELECT a, format FROM t) TO 'x.parquet' (FORMAT parquet, COMPRESSION zstd)",
		"COPY t TO 'x' (FORMAT parquet, PARTITION_BY (a, b))",
		"COPY t FROM 's3://landing/x.csv'",
		"SELECT format('{} {}', 1, 2)",
		"SELECT copy FROM t WHERE x IN (format, y)",
		"COPY t TO 'x.parquet'; SELECT 1",
		"EXPORT DATABASE 'dir' (FORMAT parquet)",
	}
	for _, q := range allowed {
		if use := gdalUse(q); use != "" {
			t.Errorf("%s: refused as %s", q, use)
		}
	}
}

func TestGeometriesAsWKT(t *testing.T) {
	ts := startServer(t)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			for _, wkt := range []string{
				"POINT (-73.98 40.75)", "POINT EMPTY", "POINT Z (1 2 3)", "POINT M (1 2 3)", "POINT ZM (1 2 3 4)",
				"LINESTRING (0 0, 1 1.5, 2 0)", "LINESTRING EMPTY",
				"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 2))",
				"MULTIPOINT (1 2, 3 4)", "MULTIPOINT Z (1 2 3, 4 5 6)",
				"MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
				"MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))",
				"GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))", "GEOMETRYCOLLECTION EMPTY",
			} {
				var got, duckdb string
				if err := conn.QueryRow(context.Background(), "SELECT g, g::VARCHAR FROM (SELECT $1::GEOMETRY AS g)", wkt).Scan(&got, &duckdb); err != nil {
					t.Errorf("%s: %v", wkt, err)
					continue
				}
				if got != duckdb {
					t.Errorf("%s: sent %q, DuckDB prints %q", wkt, got, duckdb)
				}
			}
		})
	}
}

// TestAnalyticsFunctions checks DuckDB's approximate, statistical and time
// zone functions that agencies use and that need no extension.
func TestAnalyticsFunctions(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, `CREATE TABLE imp AS SELECT i AS id, i % 50000 AS user_id, (i % 7)::INT AS campaign,
		TIMESTAMPTZ '2026-10-01 03:30:00+00' + to_minutes(i % 10000) AS served_at, (i % 100) / 10.0 AS cost
		FROM range(200000) t(i)`)
	checks := map[string]string{
		"SELECT approx_count_distinct(user_id) BETWEEN 45000 AND 55000 FROM imp":                                      "true",
		"SELECT abs(approx_quantile(cost, 0.5) - 4.95) < 0.2 FROM imp":                                                "true",
		"SELECT abs(reservoir_quantile(cost, 0.9) - 9) < 1 FROM imp":                                                  "true",
		"SELECT count(*) FROM (SELECT * FROM imp USING SAMPLE reservoir(1000 ROWS) REPEATABLE (7))":                   "1000",
		"SELECT count(*) < 40000 FROM (SELECT * FROM imp USING SAMPLE 10 PERCENT (bernoulli))":                        "true",
		"SELECT round(regr_slope(cost, id % 100), 3), round(regr_r2(cost, id % 100), 3) FROM imp":                     "0.1|1.0",
		"SELECT abs(regr_intercept(cost, id % 100)) < 0.001, regr_count(cost, id) FROM imp":                           "true|200000",
		"SELECT strftime(timezone('America/New_York', TIMESTAMPTZ '2026-10-01 03:30:00+00'), '%Y-%m-%d %H:%M')":       "2026-09-30 23:30",
		"SELECT strftime(min(served_at AT TIME ZONE 'America/Los_Angeles'), '%Y-%m-%d %H:%M') FROM imp":               "2026-09-30 20:30",
		"SELECT date_trunc('week', DATE '2026-10-14')::DATE, dayofweek(DATE '2026-10-14'), isodow(DATE '2026-10-18')": "2026-10-12|3|7",
		"SELECT time_bucket(INTERVAL '15 minutes', TIMESTAMP '2026-10-14 10:52:00')":                                  "2026-10-14 10:45:00",
		"SELECT list_sort(approx_top_k(campaign, 2)) FROM (SELECT * FROM imp WHERE campaign < 2)":                     "[0, 1]",
		"SELECT list_sort(map_keys(histogram(campaign))) FROM imp":                                                    "[0, 1, 2, 3, 4, 5, 6]",
		"SELECT count(*) FROM (SELECT user_id, count(*) AS frequency FROM imp GROUP BY ALL HAVING frequency = 4)":     "50000",
	}
	for q, want := range checks {
		var got string
		if err := conn.QueryRow(context.Background(), "SELECT concat_ws('|', *COLUMNS(*)) FROM ("+q+")").Scan(&got); err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		if got != want {
			t.Errorf("%s: got %s, want %s", q, got, want)
		}
	}
}

// mediaServer starts a server with the media extensions, which the tests
// install into IGLOO_TEST_EXTENSION_DIR (scripts/test.sh does). Its only
// allowed local directory is the spill directory, dir.
func mediaServer(t *testing.T) (*catalogServer, string) {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set; run igloo install-extensions into it")
	}
	dir := t.TempDir()
	return startCatalogServerWithConfig(t, config.Config{DuckDB: config.DuckDB{ExtensionDir: ext, TempDir: dir}}), dir
}

func TestSketches(t *testing.T) {
	cs, _ := mediaServer(t)
	ctx := context.Background()
	conn := cs.admin
	// Impressions of 100,000 people over 3 platforms, 5 campaigns and 7
	// days; sketches per campaign, platform and day, as a reporting table
	// would keep them.
	mustExec(t, conn, `CREATE TABLE impressions AS SELECT (i % 5) AS campaign, (i % 3)::INT AS platform,
		DATE '2026-10-01' + (i % 7)::INT AS day, hash(i % 100000) AS person FROM range(600000) t(i)`)
	mustExec(t, conn, `CREATE TABLE daily_reach AS SELECT campaign, platform, day,
		datasketch_hll(12, person)::BLOB AS hll, datasketch_theta(person)::BLOB AS theta, count(*) AS impressions
		FROM impressions GROUP BY ALL`)
	near := func(q string, want float64) {
		t.Helper()
		var got float64
		if err := conn.QueryRow(ctx, q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if math.Abs(got-want) > 0.05*want {
			t.Errorf("%s: %.0f, want about %.0f", q, got, want)
		}
	}
	near("SELECT datasketch_hll_estimate(datasketch_hll_union(12, hll)) FROM daily_reach", 100000)
	near("SELECT datasketch_theta_estimate(datasketch_theta(theta::sketch_theta)) FROM daily_reach WHERE campaign IN (1, 2)", 40000)
	var exact float64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM (SELECT person FROM impressions WHERE platform = 0
		INTERSECT SELECT person FROM impressions WHERE platform = 1)`).Scan(&exact); err != nil {
		t.Fatal(err)
	}
	near(`WITH p AS (SELECT platform, datasketch_theta(theta::sketch_theta) AS s FROM daily_reach GROUP BY platform)
		SELECT datasketch_theta_estimate(datasketch_theta_intersect(a.s, b.s)) FROM p a, p b WHERE a.platform = 0 AND b.platform = 1`, exact)
}

func TestSpatialSandbox(t *testing.T) {
	cs, dir := mediaServer(t)
	ctx := context.Background()
	conn := cs.admin

	// Geometry and H3 functions work.
	var inside bool
	var meters float64
	var cell string
	if err := conn.QueryRow(ctx, `SELECT ST_Contains(ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'),
			ST_Point(-73.98, 40.75)), ST_Distance_Sphere(ST_Point(-74.0060, 40.7128), ST_Point(-73.9352, 40.7306)),
			h3_h3_to_string(h3_latlng_to_cell(40.7128, -74.0060, 9))`).Scan(&inside, &meters, &cell); err != nil {
		t.Fatal(err)
	}
	if !inside || math.Abs(meters-6286) > 50 || cell != "892a1072893ffff" {
		t.Errorf("inside %v, %.0f m, cell %s", inside, meters, cell)
	}

	// A VRT file in an allowed directory may name any file as its source,
	// which GDAL would open itself. ST_Read is refused even for admins.
	outside := filepath.Join(t.TempDir(), "secret.csv")
	if err := os.WriteFile(outside, []byte("a,b\nsecret1,secret2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	vrt := filepath.Join(dir, "evil.vrt")
	if err := os.WriteFile(vrt, []byte(`<OGRVRTDataSource><OGRVRTLayer name="x"><SrcDataSource relativeToVRT="0">`+
		outside+`</SrcDataSource><SrcLayer>secret</SrcLayer></OGRVRTLayer></OGRVRTDataSource>`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"SELECT * FROM ST_Read('/etc/passwd')",
		"SELECT * FROM ST_Read('" + vrt + "')",
		"SELECT * FROM ST_Read('http://127.0.0.1:9/zips.geojson')",
		"SELECT * FROM ST_Read_Meta('/etc/passwd')",
		"SELECT * FROM '" + filepath.Join(dir, "x.gpkg") + "'",
		"COPY (SELECT ST_Point(1, 2) AS geom) TO '" + filepath.Join(dir, "x.geojson") + "' (FORMAT GDAL, DRIVER 'GeoJSON')",
		"SET VARIABLE fmt = 'gdal'; COPY (SELECT ST_Point(1, 2) AS geom) TO '" + filepath.Join(dir, "y.geojson") + "' (FORMAT getvariable('fmt'), DRIVER 'GeoJSON')",
	} {
		expectDenied(t, conn, q)
	}
	// Spatial's other readers go through DuckDB's file system, which the
	// sandbox confines: they read the allowed directory, and neither other
	// local files nor other URLs.
	writeShapefile(t, filepath.Join(dir, "inside"), "inside")
	secretShp := filepath.Join(filepath.Dir(outside), "secret")
	writeShapefile(t, secretShp, "secret")
	var name string
	if err := conn.QueryRow(ctx, "SELECT name FROM ST_ReadSHP('"+filepath.Join(dir, "inside.shp")+"')").Scan(&name); err != nil || name != "inside" {
		t.Errorf("reading a shapefile in an allowed directory: %q, %v", name, err)
	}
	for _, q := range []string{
		"SELECT * FROM ST_ReadSHP('/etc/passwd')",
		"SELECT * FROM ST_ReadSHP('" + secretShp + ".shp')",
		"SELECT * FROM '" + secretShp + ".shp'",
		"SELECT * FROM ST_ReadSHP('http://127.0.0.1:9/dma.shp')",
		"SELECT * FROM ST_ReadOSM('/etc/passwd')",
		"SELECT * FROM ST_ReadOSM('http://127.0.0.1:9/nyc.osm.pbf')",
		"SELECT * FROM '/etc/passwd.shp'",
	} {
		rows, err := conn.Query(ctx, q)
		if err == nil {
			for rows.Next() {
				vals, _ := rows.Values()
				t.Errorf("%s returned %v", q, vals)
			}
			err = rows.Err()
			rows.Close()
		}
		if err == nil {
			t.Errorf("%s: should fail", q)
		}
	}
	// The refused writes left nothing behind.
	for _, f := range []string{"x.geojson", "y.geojson"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			t.Errorf("%s was written", f)
		}
	}
}

// writeShapefile writes a shapefile of one point, at base.shp, .shx and
// .dbf, with a text attribute name.
func writeShapefile(t *testing.T, base, name string) {
	t.Helper()
	header := func(words int) []byte {
		h := make([]byte, 100)
		binary.BigEndian.PutUint32(h[0:], 9994)
		binary.BigEndian.PutUint32(h[24:], uint32(words))
		binary.LittleEndian.PutUint32(h[28:], 1000)
		binary.LittleEndian.PutUint32(h[32:], 1) // points
		for i, f := range []float64{1, 2, 1, 2} {
			binary.LittleEndian.PutUint64(h[36+8*i:], math.Float64bits(f))
		}
		return h
	}
	shp := header((100 + 8 + 20) / 2)
	shp = binary.BigEndian.AppendUint32(shp, 1)
	shp = binary.BigEndian.AppendUint32(shp, 10)
	shp = binary.LittleEndian.AppendUint32(shp, 1)
	shp = binary.LittleEndian.AppendUint64(shp, math.Float64bits(1))
	shp = binary.LittleEndian.AppendUint64(shp, math.Float64bits(2))
	shx := header((100 + 8) / 2)
	shx = binary.BigEndian.AppendUint32(shx, 50)
	shx = binary.BigEndian.AppendUint32(shx, 10)
	dbf := []byte{3, 126, 10, 16}
	dbf = binary.LittleEndian.AppendUint32(dbf, 1)
	dbf = binary.LittleEndian.AppendUint16(dbf, 32+32+1)
	dbf = binary.LittleEndian.AppendUint16(dbf, 1+10)
	dbf = append(dbf, make([]byte, 20)...)
	field := make([]byte, 32)
	copy(field, "name")
	field[11], field[16] = 'C', 10
	dbf = append(dbf, field...)
	dbf = append(dbf, 0x0d, ' ')
	dbf = append(dbf, []byte(name+strings.Repeat(" ", 10-len(name)))...)
	dbf = append(dbf, 0x1a)
	for ext, b := range map[string][]byte{".shp": shp, ".shx": shx, ".dbf": dbf} {
		if err := os.WriteFile(base+ext, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
