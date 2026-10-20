package federation

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// These tests read a real Snowflake account's sample data
// (SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 and TPCH_SF100), and are skipped unless
// these are set:
//
//	IGLOO_TEST_SNOWFLAKE_ACCOUNT           the account identifier
//	IGLOO_TEST_SNOWFLAKE_USER              a user that signs in with a key pair
//	IGLOO_TEST_SNOWFLAKE_PRIVATE_KEY_FILE  its private key, unencrypted PKCS #8
//	IGLOO_TEST_SNOWFLAKE_WAREHOUSE         a warehouse, best an X-Small
//	IGLOO_TEST_SNOWFLAKE_ROLE              a role that can read the sample data
//
// Each run costs a little warehouse time: a few small queries, and one that
// is canceled after a second.

func liveSnowflake(t *testing.T, schemas ...string) (Connection, string) {
	t.Helper()
	get := func(name string) string {
		v := os.Getenv("IGLOO_TEST_SNOWFLAKE_" + name)
		if v == "" {
			t.Skip("IGLOO_TEST_SNOWFLAKE_" + name + " not set")
		}
		return v
	}
	key := get("PRIVATE_KEY_FILE")
	c := New("sf", Snowflake)
	c.Account, c.User, c.Warehouse, c.Role = get("ACCOUNT"), get("USER"), get("WAREHOUSE"), get("ROLE")
	c.Database, c.Schemas = "SNOWFLAKE_SAMPLE_DATA", schemas
	c.PrivateKeySecret = filepath.Base(key)
	c.ConnectTimeout = 30 * time.Second
	return c, filepath.Dir(key)
}

func liveLogger() *slog.Logger {
	if os.Getenv("IGLOO_TEST_SNOWFLAKE_DEBUG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startLive attaches c on an engine of its own.
func startLive(t *testing.T, c Connection, secrets string) (*Manager, *engine.Engine) {
	t.Helper()
	installExtensions(t)
	log := liveLogger()
	store := NewMemoryStore()
	m := NewManager(store, Options{SecretsDir: secrets}, log)
	if err := m.Create(context.Background(), c, nil); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{DuckDB: config.DuckDB{ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")}}
	eng, err := engine.Open(context.Background(), cfg, log, engine.BeforeLockdown(m.Attach))
	if err != nil {
		t.Fatal(err)
	}
	m.SetConnect(eng.Connect)
	t.Cleanup(func() {
		m.closeAll()
		_ = eng.Close()
	})
	if m.Catalogs()["sf"] != Snowflake {
		st, _ := m.List(context.Background())
		t.Fatalf("not attached: %+v", st)
	}
	return m, eng
}

func TestSnowflakeLive(t *testing.T) {
	c, secrets := liveSnowflake(t, "TPCH_SF1")
	_, eng := startLive(t, c, secrets)

	expect(t, eng, "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM duckdb_tables() WHERE database_name = 'sf'",
		"customer,lineitem,nation,orders,part,partsupp,region,supplier")
	expect(t, eng, "SELECT data_type FROM duckdb_columns() WHERE database_name = 'sf' AND table_name = 'lineitem' AND column_name = 'l_extendedprice'",
		"DECIMAL(12,2)")
	expect(t, eng, "SELECT comment FROM duckdb_tables() WHERE database_name = 'sf' AND table_name = 'nation'",
		"Nation data as defined by TPC-H")
	expect(t, eng, "SELECT count(*) FROM sf.tpch_sf1.lineitem", "6001215")
	expect(t, eng, "SELECT n_name FROM sf.tpch_sf1.nation WHERE n_nationkey = 7", "GERMANY")
	// Strings compare byte by byte, under Snowflake's utf8 collation.
	expect(t, eng, "SELECT n_nationkey::VARCHAR FROM sf.tpch_sf1.nation WHERE n_name = 'GERMANY'", "7")
	expect(t, eng, "SELECT count(*) FROM sf.tpch_sf1.nation WHERE n_name IN ('germany', 'France')", "0")
	expect(t, eng, "SELECT string_agg(n_name, ',' ORDER BY n_name) FROM sf.tpch_sf1.nation WHERE n_name LIKE 'UNITED%'",
		"UNITED KINGDOM,UNITED STATES")
	expect(t, eng, "SELECT r_regionkey::VARCHAR, r_name FROM sf.tpch_sf1.region ORDER BY 1",
		"0|AFRICA\n1|AMERICA\n2|ASIA\n3|EUROPE\n4|MIDDLE EAST")
	expect(t, eng, "SELECT l_orderkey::VARCHAR, l_linenumber::VARCHAR, l_extendedprice::VARCHAR, l_shipdate::VARCHAR FROM sf.tpch_sf1.lineitem "+
		"WHERE l_orderkey = 1 AND l_linenumber = 1", "1|1|21168.23|1996-03-13")
	// Joined with a table of DuckDB's.
	expect(t, eng, "SELECT n.n_name, x.code FROM sf.tpch_sf1.nation n JOIN (VALUES ('FRANCE', 'FR'), ('JAPAN', 'JP')) x(name, code) "+
		"ON n.n_name = x.name ORDER BY 1", "FRANCE|FR\nJAPAN|JP")
}

// TestSnowflakeLiveCatalog lists a whole database: with SHOW commands,
// which need no warehouse.
func TestSnowflakeLiveCatalog(t *testing.T) {
	c, secrets := liveSnowflake(t)
	cl := newSnowflakeClient(c, secrets)
	defer cl.close()
	started := time.Now()
	cat, err := cl.loadCatalog(context.Background(), func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	tables := 0
	for _, s := range cat.schemas {
		tables += len(s.tables)
	}
	t.Logf("%d schemas, %d tables in %s", len(cat.schemas), tables, time.Since(started))
	s := cat.schema("tpch_sf1")
	if s == nil || s.table("lineitem") == nil || len(s.table("lineitem").columns) != 16 || cat.schema("information_schema") != nil {
		t.Errorf("%+v", cat.schemas)
	}
}

// TestSnowflakeLiveTimeout checks that a query past query_timeout is
// canceled, on Snowflake too.
func TestSnowflakeLiveTimeout(t *testing.T) {
	c, secrets := liveSnowflake(t, "TPCH_SF100")
	c.QueryTimeout = time.Second
	_, eng := startLive(t, c, secrets)
	started := time.Now()
	_, err := query(t, eng, "SELECT count(*) FROM sf.tpch_sf100.lineitem WHERE l_comment = 'no such comment'")
	if err == nil || !strings.Contains(err.Error(), "query_timeout") && !strings.Contains(err.Error(), "timeout") {
		t.Errorf("got %v", err)
	}
	if d := time.Since(started); d > 30*time.Second {
		t.Errorf("took %s", d)
	}
}
