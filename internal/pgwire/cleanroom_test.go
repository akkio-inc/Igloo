package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/federation"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/notify"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/tasks"
	"github.com/akkio-inc/igloo/internal/testdb"
)

// testDatabase creates a database for one test, beside the test binary's
// own (package testdb), so each test's clean rooms start empty, and
// returns its URL.
func testDatabase(t *testing.T) string {
	t.Helper()
	base := testdb.URL()
	if base == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "cleanroom_test_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// fakePartners are the recipients and providers Delta Sharing would have.
type fakePartners map[string]bool

func (f fakePartners) RecipientExists(_ context.Context, name string) (bool, error) {
	return f["recipient:"+name], nil
}

func (f fakePartners) ProviderExists(_ context.Context, name string) (bool, error) {
	return f["provider:"+name], nil
}

// roomServer is a catalog server with clean rooms, an agency's CRM of 400
// people in lake.marts.crm, which role agency owns, and a TV maker's
// viewing log of 300 of them as the external table partners.tvco_views, of
// provider "partner", whose recipient is tvco_ads. Role agency's members are
// ana (reader) and nora (none, with SELECT on the CRM); rita (reader) and
// walt (writer) are not.
type roomServer struct {
	*catalogServer
	rooms              *cleanroom.Service
	ana, nora, rita, w *pgx.Conn
}

// newRooms makes a clean room service over policies, with a store of its
// own.
func newRooms(t *testing.T, policies policy.Store) *cleanroom.Service {
	t.Helper()
	ctx := context.Background()
	store, err := cleanroom.OpenStore(ctx, testDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	parse, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		_ = parse.Close()
		_ = connector.Close()
	})
	rooms, err := cleanroom.New(cleanroom.Options{Store: store, Policies: policies, Parser: cleanroom.NewParser(parse.(*duckdb.Conn)),
		Databases: cleanroom.Databases{Catalog: "lake", Local: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	return rooms
}

func startRoomServer(t *testing.T, opts ...func(*Options)) *roomServer {
	t.Helper()
	policies := policy.NewMemoryStore()
	rooms := newRooms(t, policies)
	dir := t.TempDir()
	proxy := dir + "/proxy/"
	if err := os.MkdirAll(proxy, 0o755); err != nil {
		t.Fatal(err)
	}
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		append([]func(*Options){func(o *Options) {
			o.Policies, o.CleanRooms = policies, rooms
			o.AllowedLocations, o.Warehouse = []string{dir + "/"}, dir+"/warehouse/"
			o.external, o.externalFiles = fakeExternal{"partners.tvco_views": true}, proxy
			o.partners = fakePartners{"recipient:tvco_ads": true, "provider:partner": true}
		}}, opts...)...)
	rooms.SetEngine(cs.srv)
	views := proxy + "views.parquet"
	for _, sql := range []string{
		"CREATE SCHEMA lake.marts",
		`CREATE TABLE lake.marts.crm AS SELECT 'e' || i AS email,
			CASE WHEN i < 200 THEN 'big' WHEN i < 320 THEN 'medium' WHEN i < 380 THEN 'small' ELSE 'tiny' END AS segment,
			CASE WHEN i % 2 = 0 THEN 'east' ELSE 'west' END AS region, 'name' || i AS name FROM range(400) r(i)`,
		`COPY (SELECT 'e' || i AS email, CASE WHEN i % 3 = 0 THEN 'news' ELSE 'sports' END AS genre,
			DATE '2026-10-01' + CAST(i % 10 AS INTEGER) AS day, i % 7 AS minutes FROM range(300) r(i)) TO '` + views + `' (FORMAT parquet)`,
		"CREATE SCHEMA memory.partners",
		"CREATE VIEW memory.partners.tvco_views AS SELECT * FROM read_parquet(['" + views + "'])",
		"CREATE ROLE agency",
		"CREATE USER ana WITH PASSWORD 'ana-password' ROLE reader",
		"CREATE USER nora WITH PASSWORD 'nora-password' ROLE none",
		"CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader",
		"CREATE USER walt WITH PASSWORD 'walt-password' ROLE writer",
		"GRANT agency TO ana",
		"GRANT agency TO nora",
		"GRANT SELECT ON TABLE lake.marts.crm TO nora",
		"ALTER TABLE lake.marts.crm OWNER TO agency",
	} {
		mustExec(t, cs.admin, sql)
	}
	login := func(name string) *pgx.Conn {
		conn := connect(t, cs.url(name, name+"-password"), pgx.QueryExecModeSimpleProtocol)
		mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
		return conn
	}
	return &roomServer{catalogServer: cs, rooms: rooms, ana: login("ana"), nora: login("nora"), rita: login("rita"), w: login("walt")}
}

// pick runs a statement Igloo carries out itself, which cannot be a
// subquery, and returns the named columns of its rows, joined by |.
func pick(t *testing.T, conn *pgx.Conn, sql string, cols ...string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, c := range cols {
			i := slices.IndexFunc(rows.FieldDescriptions(), func(f pgconn.FieldDescription) bool { return f.Name == c })
			if i < 0 {
				t.Fatalf("%s has no column %s", sql, c)
			}
			if vals[i] == nil {
				parts = append(parts, "NULL")
			} else {
				parts = append(parts, fmt.Sprint(vals[i]))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

// expectPick checks pick's rows.
func expectPick(t *testing.T, conn *pgx.Conn, sql string, cols []string, want ...string) {
	t.Helper()
	if got := pick(t, conn, sql, cols...); !slices.Equal(got, want) {
		t.Errorf("%s:\n got %q\nwant %q", sql, got, want)
	}
}

// expectCode checks that a statement fails with SQLSTATE code.
func expectCode(t *testing.T, conn *pgx.Conn, sql, code string, mentions ...string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	if pgCode(err) != code {
		t.Errorf("%s: want %s, got %v", sql, code, err)
		return
	}
	for _, m := range mentions {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("%s: error %q does not mention %q", sql, err, m)
		}
	}
}

// setUp makes room tv: the agency's CRM and the TV maker's viewing log.
func (rs *roomServer) setUp(t *testing.T) {
	t.Helper()
	for _, sql := range []string{
		"CREATE CLEAN ROOM tv COMMENT 'Agency and TVCo Ads'",
		"ALTER CLEAN ROOM tv ADD COLLABORATOR agency ROLE agency",
		"ALTER CLEAN ROOM tv ADD COLLABORATOR tvco RECIPIENT tvco_ads PROVIDER partner",
		`ALTER CLEAN ROOM tv ADD DATASET views OF tvco FROM partners.tvco_views
			JOIN ON (email) GROUP BY (genre, day) AGGREGATE (sum(minutes))`,
	} {
		mustExec(t, rs.admin, sql)
	}
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET crm OF agency FROM marts.crm JOIN ON (email) GROUP BY (segment, region) RUNS PER DAY 8")
}

func TestCleanRoomsKeepRawDataToItsParty(t *testing.T) {
	t.Parallel()
	rs := startRoomServer(t)
	ctx := context.Background()
	expectRows(t, rs.rita, "SELECT count(*) FROM marts.crm", "400")

	// Who sets rooms up.
	expectCode(t, rs.ana, "CREATE CLEAN ROOM mine", "42501", "admins")
	rs.setUp(t)
	expectCode(t, rs.admin, "ALTER CLEAN ROOM tv ADD COLLABORATOR readers ROLE reader", "22023", "built-in role")
	expectCode(t, rs.admin, "ALTER CLEAN ROOM tv ADD COLLABORATOR ghosts ROLE nobody", "42704")
	expectCode(t, rs.admin, "ALTER CLEAN ROOM tv ADD COLLABORATOR other RECIPIENT nobody", "42704")
	mustExec(t, rs.admin, "CREATE TABLE lake.public.leads AS SELECT 'e1' AS email")
	mustExec(t, rs.admin, "ALTER TABLE lake.public.leads OWNER TO rita") // hers, but she is in no room
	expectCode(t, rs.rita, "ALTER CLEAN ROOM tv ADD DATASET leads OF agency FROM public.leads JOIN ON (email)", "42704")
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET v2 OF tvco FROM partners.tvco_views JOIN ON (email)", "42501")
	expectCode(t, rs.admin, "ALTER CLEAN ROOM tv ADD DATASET fake OF tvco FROM lake.public.leads JOIN ON (email)", "22023", "external tables")
	// Nobody contributes what they cannot read, the agency's own CRM
	// included, now that it is the room's.
	expectCode(t, rs.admin, "ALTER CLEAN ROOM tv ADD DATASET crm2 OF agency FROM lake.marts.crm JOIN ON (email)", "42501", "clean room tv")
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET bad OF agency FROM marts.crm JOIN ON (nope)", "22023", "not a column")

	// The agency's CRM: its own users read it as before; nobody else does,
	// admins included, however they ask.
	expectRows(t, rs.ana, "SELECT count(*) FROM marts.crm", "400")
	expectRows(t, rs.nora, "SELECT count(*) FROM lake.marts.crm", "400")
	for _, sql := range []string{
		"SELECT count(*) FROM marts.crm",
		"SELECT email FROM lake.marts.crm LIMIT 1",
		"SELECT count(*) FROM (SELECT * FROM marts.crm) x",
		"WITH c AS (SELECT * FROM marts.crm) SELECT count(*) FROM c",
		"SELECT 1 WHERE EXISTS (SELECT 1 FROM marts.crm WHERE email = 'e1')",
		"SELECT count(*) FROM marts.crm a JOIN lake.public.leads b ON a.email = b.email",
		"SELECT segment FROM marts.crm UNION SELECT email FROM lake.public.leads",
		"EXPLAIN SELECT * FROM marts.crm",
		"SUMMARIZE marts.crm",
	} {
		expectDenied(t, rs.rita, sql)
	}
	expectDenied(t, rs.rita, "SELECT count(*) FROM marts.crm", "clean room tv")
	expectDenied(t, rs.admin, "SELECT count(*) FROM lake.marts.crm", "clean room tv")
	expectDenied(t, rs.w, "UPDATE marts.crm SET segment = 'x' WHERE email = 'e1'")
	expectDenied(t, rs.w, "DELETE FROM marts.crm")
	expectDenied(t, rs.w, "CREATE TABLE public.copy AS SELECT * FROM marts.crm")
	// A view over it reads it as its reader.
	mustExec(t, rs.admin, "CREATE VIEW lake.public.crm_view AS SELECT * FROM lake.marts.crm")
	expectDenied(t, rs.rita, "SELECT count(*) FROM public.crm_view")
	expectRows(t, rs.ana, "SELECT count(*) FROM public.crm_view", "400")

	// The partner's data: no local user reads it at all.
	for _, conn := range []*pgx.Conn{rs.ana, rs.rita, rs.admin} {
		expectDenied(t, conn, "SELECT count(*) FROM memory.partners.tvco_views", "clean room tv")
	}
	expectDenied(t, rs.rita, "SELECT count(*) FROM read_parquet('"+strings.TrimSuffix(rs.srv.gov.external, "/")+"/views.parquet')")

	// The web console and Flight SQL take the same path.
	user, err := rs.store.Get(ctx, "rita")
	if err != nil {
		t.Fatal(err)
	}
	es, err := rs.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	if err := es.Run(ctx, "SELECT count(*) FROM lake.marts.crm", &roomRows{}); err == nil || !strings.Contains(err.Error(), "clean room tv") {
		t.Errorf("the console read a dataset: %v", err)
	}
	if _, err := es.Prepare(ctx, "SELECT * FROM memory.partners.tvco_views"); err == nil || !strings.Contains(err.Error(), "clean room tv") {
		t.Errorf("Flight SQL prepared a read of a dataset: %v", err)
	}

	// A party sees that a room keeps its table, as it sees policies;
	// nobody else learns of the table from the catalog (see
	// TestCleanRoomsInTheCatalog).
	refs := "SELECT policy_name, policy_kind, ref_schema_name, ref_entity_name FROM igloo.policy_references ORDER BY 4"
	expectRows(t, rs.ana, refs, "tv|CLEAN ROOM|marts|crm")
	expectRows(t, rs.rita, refs)

	// Other engines (the Iceberg REST catalog) and Delta Sharing ask the
	// same rules.
	if got := rs.srv.ProtectedBy(ctx, "marts", "crm"); !slices.Equal(got, []string{"clean room tv"}) {
		t.Errorf("ProtectedBy: %v", got)
	}
	for name, want := range map[string]bool{"rita": true, "ana": false, "nora": false} {
		u, _ := rs.store.Get(ctx, name)
		if got, err := rs.srv.ProtectedFor(ctx, u, "marts", "crm"); err != nil || got != want {
			t.Errorf("ProtectedFor(%s) = %v, %v", name, got, err)
		}
	}

	// A table can be in rooms for one party only.
	mustExec(t, rs.admin, "CREATE CLEAN ROOM radio")
	mustExec(t, rs.admin, "CREATE ROLE others")
	mustExec(t, rs.admin, "ALTER CLEAN ROOM radio ADD COLLABORATOR others ROLE others")
	mustExec(t, rs.admin, "ALTER CLEAN ROOM radio ADD COLLABORATOR rita USER rita")
	expectCode(t, rs.admin, "ALTER CLEAN ROOM radio ADD DATASET crm OF others FROM marts.crm JOIN ON (email)", "42501", "clean room tv")

	// Removing the dataset and dropping the room leave the tables as they
	// were.
	mustExec(t, rs.admin, "ALTER CLEAN ROOM tv REMOVE DATASET views")
	expectRows(t, rs.rita, "SELECT count(*) FROM memory.partners.tvco_views", "300")
	// Nor does a party contribute data it cannot read: nora has no grant on
	// the external table.
	expectCode(t, rs.nora, "ALTER CLEAN ROOM tv ADD DATASET views2 OF agency FROM memory.partners.tvco_views JOIN ON (email)", "42501")
	mustExec(t, rs.admin, "DROP CLEAN ROOM tv")
	expectRows(t, rs.rita, "SELECT count(*) FROM marts.crm", "400")
	expectCode(t, rs.admin, "DROP CLEAN ROOM tv", "42704")
	mustExec(t, rs.admin, "DROP CLEAN ROOM IF EXISTS tv")
}

func TestCleanRoomRuns(t *testing.T) {
	t.Parallel()
	rs := startRoomServer(t)
	ctx := context.Background()
	rs.setUp(t)
	tvco := cleanroom.Actor{Recipient: "tvco_ads"}

	// Templates are checked when they are added.
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD TEMPLATE leak RUN BY (agency) AS SELECT c.email, count(*) AS n FROM crm c GROUP BY 1",
		"22023", "identifier")
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD TEMPLATE leak RUN BY (agency) AS SELECT min(v.email) AS m FROM views v", "22023")
	expectCode(t, rs.rita, "ALTER CLEAN ROOM tv ADD TEMPLATE x RUN BY (agency) AS SELECT count(*) AS n FROM crm c", "42704")
	expectRows(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE overlap
		FROM BUILTIN audience_overlap (left => crm.email, right => views.email, by => crm.segment) RUN BY (agency)`, "overlap|1")

	// It runs once every party whose data it reads approved this version.
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "42501", "agency and tvco")
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 1")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "42501", "tvco approve")
	expectCode(t, rs.admin, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 1 AS tvco", "42501")
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 2", "22023", "version 1")
	if _, err := rs.rooms.Approve(ctx, tvco, "tv", "overlap", 1, ""); err != nil {
		t.Fatal(err)
	}
	expectPick(t, rs.ana, "SHOW TEMPLATES IN CLEAN ROOM tv",
		[]string{"name", "version", "builtin", "parameters", "run_by", "needs", "approved", "runnable", "problem", "created_by"},
		"overlap|1|audience_overlap(by => crm.segment, left => crm.email, right => views.email)|NULL|agency|agency, tvco|agency, tvco|true|NULL|ana")

	// Small groups are left out: small has no overlap, tiny 20 people.
	expectRows(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "1|succeeded|2|false|clean_rooms.tv__overlap__agency")
	expectRows(t, rs.ana, "SELECT * FROM clean_rooms.tv__overlap__agency ORDER BY 1", "big|200|200|100", "medium|120|100|83.33")
	expectRows(t, rs.nora, "SELECT segment FROM clean_rooms.tv__overlap__agency ORDER BY 1", "big", "medium")
	expectDenied(t, rs.rita, "SELECT * FROM clean_rooms.tv__overlap__agency", "clean room tv")
	expectDenied(t, rs.admin, "SELECT * FROM lake.clean_rooms.tv__overlap__agency")
	expectCode(t, rs.rita, "RUN CLEAN ROOM tv TEMPLATE overlap", "42704")
	expectPick(t, rs.ana, "SHOW RUNS IN CLEAN ROOM tv",
		[]string{"id", "template", "version", "requester", "requested_by", "approvals", "status", "row_count"},
		"1|overlap|1|agency|ana|agency, tvco|succeeded|2")
	expectCode(t, rs.rita, "SHOW RUNS IN CLEAN ROOM tv", "42704")
	// The partner sees the run, not the result.
	run, err := rs.rooms.RunResult(ctx, tvco, "tv", 1)
	if err != nil || run.Result != nil || run.Rows != 2 {
		t.Errorf("the partner's view of the agency's run: %+v, %v", run, err)
	}
	// Nor may it run a template not given to it.
	if _, err := rs.rooms.Run(ctx, tvco, "tv", "overlap", nil, ""); !cleanroom.IsDenied(err) {
		t.Errorf("the partner ran the agency's template: %v", err)
	}

	// A new version needs every approval again.
	expectRows(t, rs.ana, `ALTER CLEAN ROOM tv REPLACE TEMPLATE overlap
		FROM BUILTIN audience_overlap (left => crm.email, right => views.email, by => crm.region) RUN BY (agency)`, "overlap|2")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "42501", "version 2")
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 2")
	if _, err := rs.rooms.Approve(ctx, tvco, "tv", "overlap", 2, ""); err != nil {
		t.Fatal(err)
	}
	expectPick(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", []string{"status", "row_count"}, "succeeded|2")
	expectRows(t, rs.ana, "SELECT region, overlap FROM clean_rooms.tv__overlap__agency ORDER BY 1", "east|150", "west|150")
	// Withdrawing an approval stops runs at once.
	if _, err := rs.rooms.Unapprove(ctx, tvco, "tv", "overlap", ""); err != nil {
		t.Fatal(err)
	}
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "42501", "tvco approve")

	// Parameters are values; ones that narrow the population by fewer than
	// the threshold are refused, and the history says so.
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE reach PARAMETERS (first DATE, last DATE) RUN BY (agency) AS
		SELECT v.genre, count(DISTINCT v.email) AS reach FROM views v JOIN crm c ON v.email = c.email
		WHERE v.day BETWEEN $first AND $last GROUP BY 1 ORDER BY 1`)
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE reach VERSION 1")
	if _, err := rs.rooms.Approve(ctx, tvco, "tv", "reach", 1, ""); err != nil {
		t.Fatal(err)
	}
	expectPick(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE reach (first => '2026-10-01', last => '2026-10-05')",
		[]string{"status", "row_count"}, "succeeded|2")
	expectRows(t, rs.ana, "SELECT * FROM clean_rooms.tv__reach__agency", "news|50", "sports|100")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE reach (first => '2026-10-01', last => '2026-10-01')", "42501", "fewer than 50")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE reach (first => '2026-10-01')", "22023", "needs a value")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE reach (first => 'x', last => '2026-10-02')", "22P02")
	// The history is newest first: the failed run, then the refused one.
	if got := pick(t, rs.ana, "SHOW RUNS IN CLEAN ROOM tv", "status", "parameters"); len(got) < 2 || got[0] != `failed|{"first":"x","last":"2026-10-02"}` ||
		got[1] != `refused|{"first":"2026-10-01","last":"2026-10-01"}` {
		t.Errorf("history: %q", got)
	}

	// A run that fails while it reads the data does not say why, nor with
	// what code: they could depend on another party's values. It ends as a
	// refused run does.
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE broken RUN BY (agency) AS
		SELECT CAST(v.genre AS INTEGER) AS g, count(*) AS n FROM views v GROUP BY 1`)
	if _, err := rs.rooms.Approve(ctx, tvco, "tv", "broken", 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := rs.ana.Exec(ctx, "RUN CLEAN ROOM tv TEMPLATE broken"); pgCode(err) != "42501" || strings.Contains(err.Error(), "news") ||
		!strings.Contains(err.Error(), "does not say why") {
		t.Errorf("a failed run's error: %v", err)
	}
	if got := pick(t, rs.ana, "SHOW RUNS IN CLEAN ROOM tv", "status", "error")[0]; got != "refused|"+cleanroom.Unfinished.Error() {
		t.Errorf("the history of a failed run: %s", got)
	}

	// Each party's run budget counts every run of its in 24 hours: the
	// CRM allows 8.
	for i := range 3 {
		if _, err := rs.ana.Exec(ctx, "RUN CLEAN ROOM tv TEMPLATE reach (first => '2026-10-01', last => '2026-10-05')"); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE reach (first => '2026-10-01', last => '2026-10-05')", "53400", "RUNS PER DAY 8")

	// A dataset whose table gains a policy no longer runs.
	mustExec(t, rs.admin, "CREATE MASKING POLICY hide AS (val VARCHAR) RETURNS VARCHAR -> '***'")
	mustExec(t, rs.admin, "ALTER TABLE lake.marts.crm ALTER COLUMN name SET MASKING POLICY hide")
	if _, err := rs.rooms.Run(ctx, cleanroom.Actor{User: "ana", Roles: []string{"agency"}}, "tv", "reach",
		map[string]any{"first": "2026-10-01", "last": "2026-10-05"}, ""); !cleanroom.IsInvalid(err) || !strings.Contains(err.Error(), "masking") {
		t.Errorf("a dataset with a masking policy ran: %v", err)
	}
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET crm2 OF agency FROM marts.crm JOIN ON (email)", "0A000", "masking")

	// Dropping the room drops its result tables.
	mustExec(t, rs.admin, "DROP CLEAN ROOM tv")
	expectCode(t, rs.ana, "SELECT * FROM clean_rooms.tv__reach__agency", "42P01")
}

// fakeRecipients authenticates partners by fixed tokens.
type fakeRecipients map[string]string

func (f fakeRecipients) Recipient(r *http.Request) (string, error) {
	if name, ok := f[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]; ok {
		return name, nil
	}
	return "", errors.New("missing or invalid bearer token")
}

func TestCleanRoomPartnerAPI(t *testing.T) {
	t.Parallel()
	rs := startRoomServer(t)
	rs.setUp(t)
	api := httptest.NewServer(cleanroom.NewAPI(rs.rooms, fakeRecipients{"dst_tvco": "tvco_ads", "dst_other": "other"}, nil, 0, nil))
	defer api.Close()
	call := func(method, path, token, body string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(method, api.URL+cleanroom.APIPrefix+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}

	if status, _ := call("GET", "", "", ""); status != http.StatusUnauthorized {
		t.Errorf("no token: %d", status)
	}
	if status, _ := call("GET", "", "igloo_not_a_recipient", ""); status != http.StatusUnauthorized {
		t.Errorf("a wrong token: %d", status)
	}
	status, rooms := call("GET", "", "dst_tvco", "")
	if status != http.StatusOK || len(rooms["items"].([]any)) != 1 {
		t.Fatalf("rooms: %d %v", status, rooms)
	}
	if status, _ := call("GET", "", "dst_other", ""); status != http.StatusOK {
		t.Errorf("a recipient in no room: %d", status)
	}
	if status, _ := call("GET", "/tv", "dst_other", ""); status != http.StatusNotFound {
		t.Errorf("a room the recipient is not in: %d", status)
	}
	_, room := call("GET", "/tv", "dst_tvco", "")
	if ds := room["datasets"].([]any); len(ds) != 2 || room["you"].([]any)[0] != "tvco" {
		t.Fatalf("room: %v", room)
	}
	if strings.Contains(mustJSON(t, room), "marts") {
		t.Errorf("the partner sees where the agency keeps its data: %v", room)
	}

	// A template for the partner, which the agency also approves.
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE by_genre
		FROM BUILTIN audience_overlap (left => views.email, right => crm.email, by => views.genre) RUN BY (tvco)`)
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE agency_only RUN BY (agency) AS SELECT count(*) AS n FROM crm c`)
	if status, out := call("POST", "/tv/templates/by_genre/runs", "dst_tvco", "{}"); status != http.StatusForbidden || !strings.Contains(out["message"].(string), "approve") {
		t.Errorf("a run before approval: %d %v", status, out)
	}
	if status, out := call("POST", "/tv/templates/by_genre/approval", "dst_tvco", `{"version": 2}`); status != http.StatusBadRequest {
		t.Errorf("approving a version that is not the current one: %d %v", status, out)
	}
	if status, out := call("POST", "/tv/templates/by_genre/approval", "dst_tvco", `{"version": 1}`); status != http.StatusOK {
		t.Fatalf("approval: %d %v", status, out)
	}
	if status, out := call("POST", "/tv/templates/by_genre/runs", "dst_tvco", "{}"); status != http.StatusForbidden || !strings.Contains(out["message"].(string), "agency") {
		t.Errorf("a run without the agency's approval: %d %v", status, out)
	}
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE by_genre VERSION 1")
	status, run := call("POST", "/tv/templates/by_genre/runs", "dst_tvco", `{"parameters": {}}`)
	if status != http.StatusOK || run["status"] != "succeeded" || run["requester"] != "tvco" {
		t.Fatalf("the partner's run: %d %v", status, run)
	}
	rows := run["result"].(map[string]any)["rows"].([]any)
	if len(rows) != 2 || mustJSON(t, rows[0]) != `["news","100","100","100"]` {
		t.Errorf("the partner's result: %v", rows)
	}
	// Only templates given to the partner run for it.
	if status, _ := call("POST", "/tv/templates/agency_only/runs", "dst_tvco", "{}"); status != http.StatusForbidden {
		t.Errorf("the partner ran the agency's template: %d", status)
	}
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE agency_only VERSION 1")
	expectPick(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE agency_only", []string{"status"}, "succeeded")
	_, history := call("GET", "/tv/runs", "dst_tvco", "")
	items := history["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("history: %v", history)
	}
	for _, it := range items {
		r := it.(map[string]any)
		status, full := call("GET", "/tv/runs/"+mustJSON(t, r["id"]), "dst_tvco", "")
		if mine := r["requester"] == "tvco"; status != http.StatusOK || (full["result"] != nil) != mine {
			t.Errorf("run %v: %d, result shown %v to the partner", r["id"], status, full["result"] != nil)
		}
	}
	// The agency does not see the partner's result.
	if !slices.Contains(pick(t, rs.ana, "SHOW RUNS IN CLEAN ROOM tv", "requester"), "tvco") {
		t.Error("the agency does not see the partner's run in the history")
	}
	if status, _ := call("GET", "/nope", "dst_tvco", ""); status != http.StatusNotFound {
		t.Errorf("a missing room: %d", status)
	}
	// Bearer tokens cross only TLS or localhost.
	req, _ := http.NewRequest("GET", api.URL+cleanroom.APIPrefix, nil)
	req.Host = "data.example.com"
	req.Header.Set("Authorization", "Bearer dst_tvco")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusForbidden {
		t.Errorf("an unencrypted request from afar: %v %v", resp, err)
	}
	_ = auth.RoleAdmin
}

// A clone carries no confinement, which belongs to its room, so only a
// dataset's own party may clone it; and a table's branches and tags, its
// snapshots, are that party's too.
func TestCleanRoomsAndClones(t *testing.T) {
	t.Parallel()
	cat := clonetest.New(t.TempDir())
	crm, err := cat.Create(table.Identifier{"marts", "crm"}, clonetest.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	clonetest.Append(t, crm, 1, 2, 3)
	other, err := cat.Create(table.Identifier{"public", "other"}, clonetest.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	clonetest.Append(t, other, 4)
	rs := startRoomServer(t, func(o *Options) {
		o.Clones = clone.NewManager(cat, clone.NewMemoryStore(), slog.New(slog.DiscardHandler))
	})
	mustExec(t, rs.admin, "CREATE TABLE lake.public.other AS SELECT 4 AS id")
	mustExec(t, rs.admin, "ALTER TABLE lake.marts.crm CREATE BRANCH dev")
	mustExec(t, rs.admin, "GRANT CREATE ON SCHEMA lake.public TO agency")
	rs.setUp(t)

	// The admin's session starts in DuckDB's memory database, where
	// marts.crm names nothing; from the catalog it names the dataset, as
	// in walt's session.
	mustExec(t, rs.admin, "USE lake")
	for _, conn := range []*pgx.Conn{rs.w, rs.admin} {
		for _, sql := range []string{
			"CREATE TABLE lake.public.crm_copy CLONE lake.marts.crm",
			"CREATE OR REPLACE TABLE lake.public.crm_copy CLONE marts.crm AT (BRANCH => 'dev')",
			"CREATE SCHEMA marts_copy CLONE marts",
			"SHOW BRANCHES ON lake.marts.crm",
			"ALTER TABLE lake.marts.crm CREATE TAG v1",
			"ALTER TABLE lake.marts.crm DROP BRANCH dev",
			"SELECT count(*) FROM lake.marts.crm AT (BRANCH => 'dev')",
			// Not even whether a tag exists.
			"SELECT count(*) FROM marts.crm AT (TAG => 'nope')",
		} {
			expectCode(t, conn, sql, "42501", "clean room tv")
		}
	}
	if exists, _ := rs.srv.clones.Exists(context.Background(), table.Identifier{"public", "crm_copy"}); exists {
		t.Fatal("a clone of the agency's CRM was made for someone else")
	}
	// Nor does anyone else replace it with a clone of their own.
	for _, conn := range []*pgx.Conn{rs.w, rs.admin} {
		for _, sql := range []string{
			"CREATE OR REPLACE TABLE lake.marts.crm CLONE lake.public.other",
			"CREATE OR REPLACE SCHEMA marts CLONE public",
		} {
			expectCode(t, conn, sql, "42501", "clean room tv")
		}
	}
	if got := pick(t, rs.ana, "SHOW BRANCHES ON lake.marts.crm", "name"); !slices.Contains(got, "dev") {
		t.Errorf("the agency's branches: %q", got)
	}
	// The party's own clone is its own copy, outside the room.
	mustExec(t, rs.ana, "CREATE TABLE lake.public.crm_copy CLONE lake.marts.crm")
	if got := clonetest.IDs(t, cat.Load(t, "public", "crm_copy")); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Errorf("the agency's clone: %v", got)
	}
}

// The catalog shows a dataset, and a result table, to its own party alone:
// nobody else learns its name, description, columns, tags, size, usage or
// lineage, whatever their role.
func TestCleanRoomsInTheCatalog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	md := &fixedMetadata{md: &catalog.Metadata{
		Schemas: []catalog.SchemaMeta{{Name: "marts"}, {Name: "public"}, {Name: "clean_rooms"}},
		Tables: []catalog.TableMeta{
			{Schema: "marts", Name: "crm", Comment: "The agency's customers", LastModified: at, Snapshots: 1, Rows: 400, Bytes: 9000, Files: 1,
				Columns: []catalog.ColumnMeta{{Name: "email", Type: "VARCHAR", Comment: "Hashed email"}, {Name: "segment", Type: "VARCHAR"}}},
			{Schema: "public", Name: "summary", Comment: "Segments", LastModified: at, Snapshots: 1, Rows: 4, Bytes: 100, Files: 1,
				Columns: []catalog.ColumnMeta{{Name: "segment", Type: "VARCHAR"}, {Name: "n", Type: "BIGINT"}}},
			{Schema: "clean_rooms", Name: "tv__overlap__agency", Comment: "Overlap", LastModified: at, Snapshots: 1, Rows: 2, Bytes: 100, Files: 1,
				Columns: []catalog.ColumnMeta{{Name: "segment", Type: "VARCHAR"}, {Name: "overlap", Type: "BIGINT"}}},
		},
	}}
	rs := startRoomServer(t, func(o *Options) { o.Metadata = md })
	for _, sql := range []string{
		"CREATE TAG pii",
		"ALTER TABLE lake.marts.crm ALTER COLUMN email SET TAG pii = 'email'",
		"CREATE TABLE lake.public.summary AS SELECT segment, count(*) AS n FROM lake.marts.crm GROUP BY 1",
	} {
		mustExec(t, rs.admin, sql)
	}
	rs.setUp(t)
	// Rita is another party in the room; walt and the admin are in none.
	mustExec(t, rs.admin, "ALTER CLEAN ROOM tv ADD COLLABORATOR brand USER rita")
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE overlap
		FROM BUILTIN audience_overlap (left => crm.email, right => views.email, by => crm.segment) RUN BY (agency)`)
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 1")
	if _, err := rs.rooms.Approve(ctx, cleanroom.Actor{Recipient: "tvco_ads"}, "tv", "overlap", 1, ""); err != nil {
		t.Fatal(err)
	}
	expectPick(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", []string{"status"}, "succeeded")
	expectRows(t, rs.ana, "SELECT count(*) FROM marts.crm", "400")

	// The agency sees its own, usage included once the history has it.
	tables := "SELECT table_schema || '.' || table_name FROM igloo.tables ORDER BY 1"
	expectRows(t, rs.ana, tables, "clean_rooms.tv__overlap__agency", "marts.crm", "public.summary")
	expectRows(t, rs.ana, "SELECT column_name, comment FROM igloo.columns WHERE table_name = 'crm' ORDER BY 1", "email|Hashed email", "segment|NULL")
	expectRows(t, rs.ana, "SELECT upstream_name, downstream_name FROM igloo.lineage", "crm|summary")
	expectRows(t, rs.ana, "SELECT ref_entity_name FROM igloo.policy_references ORDER BY 1", "crm", "tv__overlap__agency")
	usage := "SELECT count(*) FROM igloo.table_usage WHERE table_name = 'crm'"
	for deadline := time.Now().Add(10 * time.Second); count(t, rs.ana, usage) != 1; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the agency's reads of its CRM are not in its usage")
		}
	}

	// Nobody else learns of either from the catalog, however they ask.
	queries := []string{
		"SELECT * FROM igloo.tables", "SELECT * FROM igloo.columns", "SELECT * FROM igloo_comments()", "SELECT * FROM pg_description",
		"SELECT * FROM igloo.tag_references", "SELECT * FROM igloo.policy_references", "SELECT * FROM igloo.lineage",
		"SELECT * FROM igloo.table_usage", "SHOW OBJECTS", "SHOW OBJECTS IN SCHEMA marts",
		"SELECT table_name, table_owner, comment, row_count FROM information_schema.tables WHERE table_owner IS NOT NULL",
	}
	for name, conn := range map[string]*pgx.Conn{"another party": rs.rita, "a writer": rs.w, "an admin": rs.admin} {
		expectRows(t, conn, tables, "public.summary")
		for _, q := range queries {
			for _, row := range texts(t, conn, q) {
				for _, h := range []string{"crm", "agency's customers", "Hashed email", "tv__overlap", "Overlap", "CLEAN ROOM"} {
					if strings.Contains(strings.ToLower(row), strings.ToLower(h)) {
						t.Errorf("%s: %s shows %q: %s", name, q, h, row)
					}
				}
			}
		}
		// Not even that public.summary came from a table they may not see.
		expectRows(t, conn, "SELECT count(*) FROM igloo.lineage", "0")
		expectDenied(t, conn, "SELECT * FROM lake.marts.crm LIMIT 5", "clean room tv")
	}

	// Once the room is gone, the tables are everyone's to see as before.
	mustExec(t, rs.admin, "DROP CLEAN ROOM tv")
	expectRows(t, rs.w, tables, "clean_rooms.tv__overlap__agency", "marts.crm", "partners.tvco_views", "public.summary")
}

// An alert runs as its owner: one whose query reads a dataset runs only
// for the dataset's own party, whenever it was made.
func TestCleanRoomsAndAlerts(t *testing.T) {
	t.Parallel()
	hooks := newHooks(t)
	n := notify.New(notify.Options{AllowHosts: []string{"127.0.0.1"}})
	sched := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{Ended: n.RunEnded})
	rs := startRoomServer(t, func(o *Options) { o.Tasks, o.Notify = sched, n })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	for _, sql := range []string{
		"CREATE NOTIFICATION INTEGRATION hook TYPE webhook URL '" + hooks.URL + "/hook'",
		"CREATE USER pat WITH PASSWORD 'pat-password' ROLE writer",
		"GRANT agency TO pat",
		"GRANT USAGE ON INTEGRATION hook TO walt",
		"GRANT USAGE ON INTEGRATION hook TO pat",
	} {
		mustExec(t, rs.admin, sql)
	}
	pat := connect(t, rs.url("pat", "pat-password"), pgx.QueryExecModeSimpleProtocol)
	alert := func(name string) string {
		return "CREATE ALERT " + name + ` SCHEDULE '1 hour' IF (EXISTS (
			SELECT segment, count(*) AS n FROM lake.marts.crm GROUP BY 1)) THEN NOTIFY hook WITH RESULT`
	}
	// Walt's alert, made before the CRM was the room's, no longer runs.
	mustExec(t, rs.w, alert("walts"))
	rs.setUp(t)
	if _, err := rs.w.Exec(context.Background(), "EXECUTE ALERT walts"); err == nil || !strings.Contains(err.Error(), "clean room tv") {
		t.Errorf("walt's alert ran: %v", err)
	}
	expectRows(t, rs.w, "SELECT alert_name, state FROM igloo.alert_history", "walts|condition_failed")
	expectCode(t, rs.w, alert("again"), "42501", "clean room tv")
	// Nor does a message reach it: it is a string, not a query.
	expectCode(t, rs.w, "CALL SYSTEM$SEND_NOTIFICATION('hook', (SELECT max(email) FROM lake.marts.crm))", "42601")

	// The agency's own alert runs.
	mustExec(t, pat, alert("pats"))
	mustExec(t, pat, "EXECUTE ALERT pats")
	if p := hooks.wait(t, 1); p.Name != "pats" || p.Result == nil || len(p.Result.Rows) != 4 {
		t.Errorf("the agency's alert: %s", p.Body)
	}
	if hooks.count() != 1 {
		t.Errorf("%d notifications, want the agency's alone", hooks.count())
	}
}

// A connection's table cannot be a dataset: its database is not Igloo's to
// keep to a party.
func TestCleanRoomsRefuseConnectionTables(t *testing.T) {
	t.Parallel()
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.TLS = federation.TLSDisable // the test databases have no TLS
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.Schemas = []string{schema}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	fs := startFedServer(t, store, func(s *Server) {
		s.rooms = newRooms(t, s.gov.store)
		s.partners = fakePartners{}
		s.rooms.SetEngine(s)
	})
	for _, sql := range []string{
		"CREATE ROLE agency",
		"CREATE CLEAN ROOM tv",
		"ALTER CLEAN ROOM tv ADD COLLABORATOR agency ROLE agency",
	} {
		mustExec(t, fs.admin, sql)
	}
	expectCode(t, fs.admin, "ALTER CLEAN ROOM tv ADD DATASET c OF agency FROM crm."+schema+".customers JOIN ON (region)", "0A000", "connection crm")
	if got := fs.srv.Policies(context.Background()).Catalog.Confinements; len(got) != 0 {
		t.Errorf("confinements after the refusal: %+v", got)
	}
	// A table of the catalog with what the room may read is the way.
	mustExec(t, fs.admin, "CREATE TABLE lake.public.regions AS SELECT region FROM crm."+schema+".customers")
	mustExec(t, fs.admin, "ALTER CLEAN ROOM tv ADD DATASET regions OF agency FROM public.regions JOIN ON (region)")
}

// Only a dataset's party changes it, and only a room's runs its results:
// confinement kept others from reading a dataset, but a writer could
// change it, or replace it, and the party's runs then read the writer's
// rows; or write into a result.
func TestOnlyItsPartyChangesADataset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rs := startRoomServer(t)
	rs.setUp(t)
	mustExec(t, rs.admin, "CREATE USER pat WITH PASSWORD 'pat-password' ROLE writer")
	mustExec(t, rs.admin, "GRANT agency TO pat")
	pat := connect(t, rs.url("pat", "pat-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE overlap
		FROM BUILTIN audience_overlap (left => crm.email, right => views.email, by => crm.segment) RUN BY (agency)`)
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 1")
	if _, err := rs.rooms.Approve(ctx, cleanroom.Actor{Recipient: "tvco_ads"}, "tv", "overlap", 1, ""); err != nil {
		t.Fatal(err)
	}
	mustExec(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap")
	mustExec(t, rs.admin, "CREATE TABLE lake.public.other AS SELECT 'e1' AS email")

	changes := []string{
		"INSERT INTO lake.marts.crm VALUES ('e5', 'planted', 'east', 'x')",
		"INSERT INTO lake.marts.crm SELECT 'e' || i, 'planted', 'east', 'x' FROM range(250, 300) r(i)",
		"INSERT INTO lake.marts.crm VALUES ('e1', 'x', 'y', 'z') RETURNING *",
		"WITH x AS (SELECT 1) INSERT INTO lake.marts.crm SELECT 'e9', 'x', 'y', 'z' FROM x",
		"UPDATE lake.marts.crm SET segment = 'x'",
		"DELETE FROM lake.marts.crm",
		"TRUNCATE lake.marts.crm",
		"MERGE INTO lake.marts.crm c USING lake.public.other o ON c.email = o.email WHEN MATCHED THEN DELETE",
		"DROP TABLE lake.marts.crm",
		"CREATE OR REPLACE TABLE lake.marts.crm AS SELECT 'e1' AS email, 'big' AS segment, 'east' AS region, 'n' AS name",
		"ALTER TABLE lake.marts.crm ADD COLUMN planted INTEGER",
		"ALTER TABLE lake.marts.crm ALTER COLUMN segment SET DEFAULT 'x'",
		"ALTER TABLE lake.marts.crm RENAME TO crm_moved",
		"DROP SCHEMA lake.marts CASCADE",
		// The ALTER that reads every row to convert it, and errs with them.
		"ALTER TABLE lake.marts.crm ALTER COLUMN segment SET DATA TYPE VARCHAR USING error(email || ' is ' || segment)",
		"ALTER TABLE lake.marts.crm ALTER COLUMN email SET DATA TYPE INTEGER",
		"ALTER TABLE ONLY lake.marts.crm ALTER COLUMN segment SET DATA TYPE VARCHAR USING error(email || ' is ' || segment)",
		"ALTER TABLE IF EXISTS ONLY (lake.marts.crm) RENAME COLUMN segment TO s",
		"ALTER TABLE lake.marts.crm * ADD COLUMN planted INTEGER",
	}
	for _, conn := range []*pgx.Conn{rs.w, rs.admin} {
		for _, sql := range changes {
			// Some are refused by other checks first, as for any table
			// with policies, with messages of their own.
			_, err := conn.Exec(ctx, sql)
			if pgCode(err) != "42501" || strings.Contains(err.Error(), "e1 is") {
				t.Errorf("%s: %s: want refused, got %v", userName(t, conn), sql, err)
			}
		}
		expectCode(t, conn, changes[0], "42501", "clean room tv")
	}
	// A run reads what the agency put there.
	expectRows(t, rs.ana, "SELECT count(*) FROM marts.crm", "400")
	// The agency's own writer changes its own data.
	mustExec(t, pat, "UPDATE lake.marts.crm SET region = 'north' WHERE email = 'e1'")

	// A result is written by its runs alone, its own party's users
	// included, and dropped with its room.
	for _, conn := range []*pgx.Conn{rs.w, pat, rs.admin} {
		for _, sql := range []string{
			"INSERT INTO lake.clean_rooms.tv__overlap__agency VALUES ('forged', 1, 1, 100)",
			"DELETE FROM lake.clean_rooms.tv__overlap__agency",
			"DROP TABLE lake.clean_rooms.tv__overlap__agency",
			"CREATE OR REPLACE TABLE lake.clean_rooms.tv__overlap__agency AS SELECT 1 AS x",
		} {
			expectCode(t, conn, sql, "42501", "clean room tv")
		}
	}
	expectRows(t, rs.ana, "SELECT segment FROM clean_rooms.tv__overlap__agency ORDER BY 1", "big", "medium")
	// Other tables are as before.
	mustExec(t, rs.w, "INSERT INTO lake.public.other VALUES ('e2')")
}

// Only a table's owner, a member of the role that owns it, or an admin puts
// it in a clean room, which keeps it from everyone else: a reader of every
// table could otherwise take a shared table from the rest of the company.
func TestOnlyOwnersAddDatasets(t *testing.T) {
	t.Parallel()
	rs := startRoomServer(t)
	rs.setUp(t) // ana, of role agency, which owns the CRM
	for _, sql := range []string{
		"CREATE TABLE lake.public.shared AS SELECT 'e' || i AS email, 'x' AS kind FROM range(100) r(i)",
		"CREATE TABLE lake.public.anas AS SELECT 'e' || i AS email FROM range(100) r(i)",
		"CREATE TABLE lake.public.teams AS SELECT 'e' || i AS email FROM range(100) r(i)",
		"CREATE TABLE lake.public.orphan AS SELECT 'e' || i AS email FROM range(100) r(i)",
		"ALTER TABLE lake.public.anas OWNER TO ana",
		"ALTER TABLE lake.public.teams OWNER TO agency",
		"CREATE USER gone WITH PASSWORD 'gone-password' ROLE writer",
		"ALTER TABLE lake.public.orphan OWNER TO gone",
		"DROP USER gone",
	} {
		mustExec(t, rs.admin, sql)
	}
	// Reading a table, as every reader does, is not enough.
	expectRows(t, rs.ana, "SELECT count(*) FROM public.shared", "100")
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET shared OF agency FROM public.shared JOIN ON (email)", "42501", "only igloo, who owns public.shared")
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET orphan OF agency FROM public.orphan JOIN ON (email)", "42501", "has no owner")
	// Nor may anyone change a dataset's source to a table not theirs.
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv REPLACE DATASET crm OF agency FROM public.shared JOIN ON (email)", "42501", "igloo")
	expectRows(t, rs.rita, "SELECT count(*) FROM public.shared", "100")
	// Its owner, a member of the role that owns it, and an admin may.
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET anas OF agency FROM public.anas JOIN ON (email)")
	mustExec(t, rs.admin, "GRANT SELECT ON TABLE lake.public.teams TO nora")
	mustExec(t, rs.nora, "ALTER CLEAN ROOM tv ADD DATASET teams OF agency FROM public.teams JOIN ON (email)")
	mustExec(t, rs.admin, "ALTER CLEAN ROOM tv ADD DATASET shared OF agency FROM lake.public.shared JOIN ON (email)")
	expectPick(t, rs.ana, "SHOW DATASETS IN CLEAN ROOM tv", []string{"name"}, "anas", "crm", "shared", "teams", "views")
}

// A sum of a measure across parties takes bounds on the measure, and noise
// from every party.
func TestBoundedSumsAcrossParties(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rs := startRoomServer(t)
	rs.setUp(t)
	const minutes = `ALTER CLEAN ROOM tv ADD TEMPLATE minutes RUN BY (agency) AS
		SELECT c.region, count(DISTINCT c.email) AS people, sum(v.minutes) AS minutes
		FROM crm c JOIN views v ON c.email = v.email GROUP BY 1 ORDER BY 1`
	expectCode(t, rs.ana, minutes, "22023", "BOUNDS (minutes BETWEEN lo AND hi)")
	mustExec(t, rs.admin, `ALTER CLEAN ROOM tv REPLACE DATASET views OF tvco FROM partners.tvco_views
		JOIN ON (email) GROUP BY (genre, day) AGGREGATE (sum(minutes)) BOUNDS (Minutes BETWEEN 0 AND 6) NOISE 1`)
	expectCode(t, rs.ana, minutes, "22023", "crm has none")
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv REPLACE DATASET crm OF agency FROM marts.crm JOIN ON (email) GROUP BY (segment, region) NOISE 0.5")
	// With noise, ten runs a day unless a policy says otherwise.
	expectPick(t, rs.ana, "SHOW DATASETS IN CLEAN ROOM tv", []string{"name", "aggregates", "bounds", "runs_per_day", "noise"},
		"crm|NULL|NULL|10|0.5", "views|sum(minutes)|minutes BETWEEN 0 AND 6|10|1")
	mustExec(t, rs.ana, minutes)
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE minutes VERSION 1")
	if _, err := rs.rooms.Approve(ctx, cleanroom.Actor{Recipient: "tvco_ads"}, "tv", "minutes", 1, ""); err != nil {
		t.Fatal(err)
	}
	expectPick(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE minutes", []string{"status", "row_count"}, "succeeded|2")
	expectCode(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE longest RUN BY (agency) AS
		SELECT c.region, max(v.minutes) AS m FROM crm c JOIN views v ON c.email = v.email GROUP BY 1`, "22023")
}

// A table a clean room keeps to its party runs on one machine, for the
// party too: workers read its files, past the confinement.
func TestCleanRoomsAndDistributedExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ds := startDistServer(t, 2)
	err := ds.srv.gov.store.Update(ctx, func(c *policy.Catalog) error {
		return c.SetConfinements("tv", []policy.Confinement{{Schema: "public", Table: "orders", Room: "tv", Users: []string{"rita"}}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.srv.gov.reload(ctx); err != nil {
		t.Fatal(err)
	}
	rita := ds.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, rita, "SET distributed_execution = on")
	orders := "SELECT amount, count(*) FROM lake.public.orders GROUP BY amount"
	expectAllowed(t, rita, orders)
	if q := lastQuery(t, ds.catalogServer, "rita", orders); q.Workers != 0 {
		t.Errorf("a dataset's statement ran on %d workers", q.Workers)
	}
	user, err := ds.store.Get(ctx, "rita")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ds.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	env := &sessionEnv{s: ds.srv, sess: es.sess, user: user, st: &statement{}}
	if _, err := ds.srv.dist.Plan(ctx, orders, env, distributed.ModeOn); !distributed.IsUnsupported(err) || !strings.Contains(err.Error(), "clean room") {
		t.Errorf("planning a dataset: %v", err)
	}
}

// An approval is of a template as it reads the datasets: changing a
// dataset's source or policy after it, as its party may alone, needs every
// approval again, and so does its table being made again.
func TestApprovalsCoverDatasets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rs := startRoomServer(t)
	rs.setUp(t)
	tvco := cleanroom.Actor{Recipient: "tvco_ads"}
	mustExec(t, rs.admin, "CREATE USER pat WITH PASSWORD 'pat-password' ROLE writer")
	mustExec(t, rs.admin, "GRANT agency TO pat")
	pat := connect(t, rs.url("pat", "pat-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, pat, "CREATE TABLE lake.marts.crm2 AS SELECT * FROM lake.marts.crm")
	mustExec(t, rs.admin, "ALTER TABLE lake.marts.crm2 OWNER TO agency")
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE overlap
		FROM BUILTIN audience_overlap (left => crm.email, right => views.email, by => crm.segment) RUN BY (agency)`)
	approve := func() {
		t.Helper()
		mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE overlap VERSION 1")
		if _, err := rs.rooms.Approve(ctx, tvco, "tv", "overlap", 1, ""); err != nil {
			t.Fatal(err)
		}
	}
	runs := func() {
		t.Helper()
		expectPick(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", []string{"status"}, "succeeded")
	}
	approve()
	runs()
	digest := pick(t, rs.ana, "SHOW TEMPLATES IN CLEAN ROOM tv", "digest")[0]

	for _, change := range []struct {
		conn *pgx.Conn
		sql  string
	}{
		// The agency's own policy, and its source.
		{rs.ana, "ALTER CLEAN ROOM tv REPLACE DATASET crm OF agency FROM marts.crm JOIN ON (email) GROUP BY (segment, region) THRESHOLD 60"},
		{rs.ana, "ALTER CLEAN ROOM tv REPLACE DATASET crm OF agency FROM marts.crm2 JOIN ON (email) GROUP BY (segment, region) THRESHOLD 60"},
		// The TV maker's.
		{rs.admin, `ALTER CLEAN ROOM tv REPLACE DATASET views OF tvco FROM partners.tvco_views
			JOIN ON (email) GROUP BY (genre, day) AGGREGATE (sum(minutes)) RUNS PER DAY 20`},
	} {
		mustExec(t, change.conn, change.sql)
		expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "42501", "agency and tvco approve")
		if now := pick(t, rs.ana, "SHOW TEMPLATES IN CLEAN ROOM tv", "digest")[0]; now == digest {
			t.Errorf("%s: the digest an approval approves stayed %s", change.sql, now)
		} else {
			digest = now
		}
		approve()
		runs()
	}

	// Its table made again, with another column, by its party: no run reads
	// it until the dataset is replaced, and approved again.
	mustExec(t, pat, "DROP TABLE lake.marts.crm2")
	mustExec(t, pat, "CREATE TABLE lake.marts.crm2 AS SELECT *, 1 AS planted FROM lake.marts.crm")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "55000", "not what it was")
	mustExec(t, pat, "ALTER CLEAN ROOM tv REPLACE DATASET crm OF agency FROM marts.crm2 JOIN ON (email) GROUP BY (segment, region) THRESHOLD 60")
	expectCode(t, rs.ana, "RUN CLEAN ROOM tv TEMPLATE overlap", "42501", "approve")
	approve()
	runs()
}

// Datasets stored with noise and the old default of 100 runs a day take
// the new default of 10, once: a budget set to 100 later stays.
func TestStoredNoisyBudgetsMigrate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	url := testDatabase(t)
	store, err := cleanroom.OpenStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRoom(ctx, cleanroom.Room{Name: "tv"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AddCollaborator(ctx, cleanroom.Collaborator{Room: "tv", Name: "agency", Kind: cleanroom.KindUser, Principal: "ana"}); err != nil {
		t.Fatal(err)
	}
	for _, d := range []cleanroom.Dataset{
		{Room: "tv", Name: "noisy", Collaborator: "agency", Schema: "marts", Table: "a", Kind: cleanroom.SourceTable,
			Policy: cleanroom.Policy{Join: []string{"email"}, Threshold: 50, RunsPerDay: 100, Noise: 1}},
		{Room: "tv", Name: "exact", Collaborator: "agency", Schema: "marts", Table: "b", Kind: cleanroom.SourceTable,
			Policy: cleanroom.Policy{Join: []string{"email"}, Threshold: 50, RunsPerDay: 100}},
	} {
		if err := store.PutDataset(ctx, d, false); err != nil {
			t.Fatal(err)
		}
	}
	budgets := func(s *cleanroom.Store) string {
		ds, err := s.Datasets(ctx, "tv")
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, d := range ds {
			out = append(out, fmt.Sprintf("%s=%d", d.Name, d.Policy.RunsPerDay))
		}
		return strings.Join(out, " ")
	}
	// A store from before the migration: as if it had never run.
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx) //nolint:errcheck
	mustExec(t, admin, "DELETE FROM igloo_cleanroom.applied")
	store.Close()
	if store, err = cleanroom.OpenStore(ctx, url); err != nil {
		t.Fatal(err)
	}
	if got := budgets(store); got != "exact=100 noisy=10" {
		t.Errorf("after the migration: %s", got)
	}
	// Once only.
	mustExec(t, admin, `UPDATE igloo_cleanroom.datasets SET policy = jsonb_set(policy, '{runs_per_day}', '100') WHERE name = 'noisy'`)
	store.Close()
	if store, err = cleanroom.OpenStore(ctx, url); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got := budgets(store); got != "exact=100 noisy=100" {
		t.Errorf("opened again: %s", got)
	}
}

// A version number named one version only until the template was removed:
// added again, it started at 1, and an approval of "version 1" that its
// approver sent after reviewing the old SQL approved the new. Versions are
// never used twice now, and an approval holds the digest of what it
// approved.
func TestApprovalsBindWhatWasReviewed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rs := startRoomServer(t)
	rs.setUp(t)
	tvco := cleanroom.Actor{Recipient: "tvco_ads"}
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE report RUN BY (agency) AS
		SELECT v.genre, count(DISTINCT v.email) AS reach FROM views v JOIN crm c ON v.email = c.email GROUP BY 1`)
	v, err := rs.rooms.Describe(ctx, tvco, "tv")
	if err != nil {
		t.Fatal(err)
	}
	reviewed := v.Templates[0]
	if reviewed.Version != 1 || len(reviewed.Digest) != 64 {
		t.Fatalf("reviewed: %+v", reviewed)
	}
	// Before TVCo sends its approval, the agency swaps the SQL.
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv REMOVE TEMPLATE report")
	expectRows(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE report RUN BY (agency) AS
		SELECT c.segment, count(DISTINCT v.email) AS reach FROM views v JOIN crm c ON v.email = c.email GROUP BY 1`, "report|2")
	if _, err := rs.rooms.Approve(ctx, tvco, "tv", "report", reviewed.Version, ""); !cleanroom.IsInvalid(err) {
		t.Fatalf("TVCo's approval of what it reviewed landed on other SQL: %v", err)
	}
	if got := pick(t, rs.ana, "SHOW TEMPLATES IN CLEAN ROOM tv", "version", "digest"); len(got) != 1 || !strings.HasPrefix(got[0], "2|") ||
		strings.HasSuffix(got[0], reviewed.Digest) {
		t.Errorf("the new version and its digest: %q", got)
	}
}

// The agency marks one person it wants to know about, after the templates
// were approved, and runs them: a CASE in an aggregate would read that
// person's viewing outright, and the population check, counted on the
// joined data, would tell whether they are in it.
func TestOnePersonStaysHidden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rs := startRoomServer(t)
	rs.setUp(t)
	tvco := cleanroom.Actor{Recipient: "tvco_ads"}
	mustExec(t, rs.admin, "CREATE USER pat WITH PASSWORD 'pat-password' ROLE writer")
	mustExec(t, rs.admin, "GRANT agency TO pat")
	pat := connect(t, rs.url("pat", "pat-password"), pgx.QueryExecModeSimpleProtocol)

	expectCode(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE vip RUN BY (agency) AS
		SELECT v.genre, count(DISTINCT v.email) AS reach, count(CASE WHEN c.segment = 'vip' THEN 1 END) AS vip_views,
		max(CASE WHEN c.segment = 'vip' THEN v.day END) AS vip_last_day
		FROM views v JOIN crm c ON v.email = c.email GROUP BY 1 ORDER BY 1`, "22023")
	mustExec(t, rs.ana, `ALTER CLEAN ROOM tv ADD TEMPLATE seg PARAMETERS (segment VARCHAR) RUN BY (agency) AS
		SELECT v.genre, count(DISTINCT v.email) AS reach FROM views v JOIN crm c ON v.email = c.email
		WHERE c.segment = $segment GROUP BY 1`)
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv APPROVE TEMPLATE seg VERSION 1")
	if _, err := rs.rooms.Approve(ctx, tvco, "tv", "seg", 1, ""); err != nil {
		t.Fatal(err)
	}
	// e42 is in the TV maker's log, e390 is not: the runs go alike.
	var outcomes []string
	for _, target := range []string{"e42", "e390"} {
		mustExec(t, pat, "UPDATE lake.marts.crm SET segment = 'big' WHERE segment = 'probe'")
		mustExec(t, pat, "UPDATE lake.marts.crm SET segment = 'probe' WHERE email = '"+target+"'")
		_, err := rs.ana.Exec(ctx, "RUN CLEAN ROOM tv TEMPLATE seg (segment => 'probe')")
		outcomes = append(outcomes, fmt.Sprint(pgCode(err))+"|"+pick(t, rs.ana, "SHOW RUNS IN CLEAN ROOM tv", "status", "row_count")[0])
	}
	if outcomes[0] != outcomes[1] || outcomes[0] != "42501|refused|0" {
		t.Errorf("runs for a person seen and one not: %q", outcomes)
	}
	// Nothing in a run says how many groups were left out.
	if cols := pick(t, rs.ana, "SHOW RUNS IN CLEAN ROOM tv", "id"); len(cols) != 2 {
		t.Errorf("runs: %q", cols)
	}
	rows, err := rs.ana.Query(ctx, "SHOW RUNS IN CLEAN ROOM tv", pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range rows.FieldDescriptions() {
		if strings.Contains(f.Name, "suppressed") {
			t.Errorf("SHOW RUNS has %s", f.Name)
		}
	}
	rows.Close()
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestCleanRoomDatasetWithoutIDs keeps the columns that hold ID domains'
// ids out of a clean room: a member table's dataset has its other columns,
// and naming the column of ids is refused.
func TestCleanRoomDatasetWithoutIDs(t *testing.T) {
	t.Parallel()
	rs := startRoomServer(t)
	ctx := context.Background()
	conn, err := rs.srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `CREATE TABLE lake.marts.ids AS SELECT 'e' || i AS email,
		CASE WHEN i < 300 THEN 'big' ELSE 'small' END AS segment, i AS _igloo_id_email FROM range(400) r(i)`, nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	rs.srv.catalogChanged()
	mustExec(t, rs.admin, "ALTER TABLE lake.marts.ids OWNER TO agency")
	err = rs.srv.idDomains.Update(ctx, func(c *iddomain.Catalog) error {
		if err := c.AddDomain(iddomain.Domain{Name: "people", Type: "VARCHAR", Generation: 1}); err != nil {
			return err
		}
		return c.AddMember(iddomain.Member{Domain: "people", Schema: "marts", Table: "ids", Column: "email"})
	})
	if err != nil {
		t.Fatal(err)
	}
	rs.srv.forgetMembers()
	rs.setUp(t)
	// The dataset has no column of ids to join on, and its error lists
	// the columns it has.
	expectCode(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET bad OF agency FROM marts.ids JOIN ON (_igloo_id_email)", "22023", "(it has email, segment)")
	mustExec(t, rs.ana, "ALTER CLEAN ROOM tv ADD DATASET ids OF agency FROM marts.ids JOIN ON (email) GROUP BY (segment)")
}
