package console

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// fixedMetadata stands in for what the Iceberg catalog records about the
// stand-in catalog's tables.
type fixedMetadata struct{ md *catalog.Metadata }

func (f fixedMetadata) Metadata() *catalog.Metadata         { return f.md }
func (fixedMetadata) RefreshMetadata(context.Context) error { return nil }
func (fixedMetadata) TableChanged(string, string)           {}

// newCatalogConsole is a console whose server has a stand-in catalog,
// lake: public.orders, made from public.secrets and made into
// public.report. nora, whose role is none, may read orders only.
func newCatalogConsole(t *testing.T) *testConsole {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.Open(ctx, config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	users := &store{users: map[string]auth.User{}}
	for name, role := range map[string]auth.Role{"igloo": auth.RoleAdmin, "walt": auth.RoleWriter, "nora": auth.RoleNone} {
		users.add(t, name, name+"-password", role)
	}
	history := audit.New(audit.NewMemoryStore(1000, 1000), audit.Options{}, log)
	eng.SetHistory(history, pgwire.HistoryViewer)
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	md := &catalog.Metadata{
		Schemas: []catalog.SchemaMeta{{Name: "public", Comment: "Shared tables"}},
		Tables: []catalog.TableMeta{
			{Schema: "public", Name: "orders", Comment: "One row per order", LastModified: at, Rows: 2, Bytes: 1000, Files: 1, Snapshots: 2,
				Columns: []catalog.ColumnMeta{{Name: "id", Type: "INTEGER"}, {Name: "amount", Type: "INTEGER", Comment: "In cents"}}},
			{Schema: "public", Name: "secrets", Comment: "Launch codes", LastModified: at, Rows: 1, Bytes: 10, Files: 1, Snapshots: 1,
				Columns: []catalog.ColumnMeta{{Name: "id", Type: "INTEGER"}, {Name: "code", Type: "VARCHAR", Comment: "The code itself"}}},
			{Schema: "public", Name: "report", Comment: "Daily report", LastModified: at, Rows: 2, Bytes: 100, Files: 1, Snapshots: 1,
				Columns: []catalog.ColumnMeta{{Name: "id", Type: "INTEGER"}, {Name: "amount", Type: "INTEGER"}}},
		},
	}
	sql, err := pgwire.New(eng, pgwire.Options{Users: users, Grants: auth.NewMemoryGrants(users), Catalog: "lake", Audit: history,
		Metadata: fixedMetadata{md}}, log)
	if err != nil {
		t.Fatal(err)
	}
	es, err := sql.OpenEmbedded(ctx, auth.User{Name: "igloo", Role: auth.RoleAdmin}, "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"ATTACH ':memory:' AS lake",
		"CREATE SCHEMA lake.public",
		"CREATE TABLE lake.public.secrets (id INTEGER, code VARCHAR)",
		"INSERT INTO lake.public.secrets VALUES (1, 'x')",
		"CREATE TABLE lake.public.orders (id INTEGER, amount INTEGER)",
		"INSERT INTO lake.public.orders SELECT id, 10 FROM lake.public.secrets",
		"CREATE TABLE lake.public.report AS SELECT * FROM lake.public.orders",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.orders ALTER COLUMN amount SET TAG pii = 'money'",
		"GRANT SELECT ON TABLE lake.public.orders TO nora",
		"CREATE TABLE lake.main.notes (id INTEGER)",
	} {
		if err := es.Run(ctx, stmt, &collect{}); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	_ = es.Close()
	c, err := New(Options{SQL: sql, Users: users, Key: bytes.Repeat([]byte{7}, 32), Log: log})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(c)
	t.Cleanup(func() {
		srv.Close()
		_ = eng.Close()
	})
	return &testConsole{Console: c, sql: sql, users: users, srv: srv}
}

// get fetches a JSON endpoint, returning its status and body.
func (b *browser) get(path string) (int, string) {
	b.t.Helper()
	resp := b.do("GET", path, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func searchFor(t *testing.T, b *browser, q string) []match {
	t.Helper()
	status, body := b.get("/api/catalog/search?q=" + url.QueryEscape(q))
	if status != http.StatusOK {
		t.Fatalf("search %q: %d %s", q, status, body)
	}
	var out struct{ Results []match }
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	return out.Results
}

func names(ms []match) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Schema+"."+m.Name)
	}
	return out
}

func TestCatalogSearch(t *testing.T) {
	tc := newCatalogConsole(t)
	admin := tc.signedIn(t, "igloo")
	nora := tc.signedIn(t, "nora")

	if got := names(searchFor(t, admin, "")); len(got) != 3 {
		t.Errorf("everything: %q", got)
	}
	for q, want := range map[string]string{
		"ORDERS": "public.orders", "cents": "public.orders", "money": "public.orders", "launch": "public.secrets",
		"daily report": "public.report", "code itself": "public.secrets",
	} {
		if got := names(searchFor(t, admin, q)); len(got) != 1 || got[0] != want {
			t.Errorf("search %q: %q, want %s", q, got, want)
		}
	}
	m := searchFor(t, admin, "cents")[0]
	if len(m.MatchedOther) != 1 || m.MatchedOther[0] != "column amount" || m.Owner != "igloo" || m.Comment != "One row per order" {
		t.Errorf("why orders matched: %+v", m)
	}
	if got := searchFor(t, admin, "money"); got[0].Tags[0] != "pii = money" {
		t.Errorf("tags: %+v", got[0])
	}
	// The owner is searched too: the admin created every table.
	if got := names(searchFor(t, admin, "igloo")); len(got) != 3 {
		t.Errorf("by owner: %q", got)
	}

	// Nora finds only what she may see, whatever she searches for.
	if got := names(searchFor(t, nora, "")); len(got) != 1 || got[0] != "public.orders" {
		t.Errorf("nora's catalog: %q", got)
	}
	for _, q := range []string{"launch", "code", "secrets", "report", "daily"} {
		if got := searchFor(t, nora, q); len(got) != 0 {
			t.Errorf("nora searching %q finds %q", q, names(got))
		}
	}
}

func TestCatalogObject(t *testing.T) {
	tc := newCatalogConsole(t)
	admin := tc.signedIn(t, "igloo")
	nora := tc.signedIn(t, "nora")

	type graph struct {
		Nodes []node
		Edges []edge
	}
	var page struct {
		Object     map[string]any
		Columns    []map[string]any
		Tags       []map[string]any
		Lineage    graph
		CanComment bool `json:"can_comment"`
	}
	status, body := admin.get("/api/catalog/object?catalog=lake&schema=public&name=ORDERS")
	if status != http.StatusOK {
		t.Fatalf("admin: %d %s", status, body)
	}
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	labels := func(g graph) map[string]int {
		out := map[string]int{}
		for _, n := range g.Nodes {
			out[n.Label] = n.Depth
		}
		return out
	}
	if page.Object["table_owner"] != "igloo" || page.Object["row_count"] != "2" || len(page.Columns) != 2 || len(page.Tags) != 1 ||
		!page.CanComment {
		t.Errorf("the admin's page: %s", body)
	}
	if got := labels(page.Lineage); got["public.secrets"] != -1 || got["public.report"] != 1 || got["public.orders"] != 0 || len(page.Lineage.Edges) != 2 {
		t.Errorf("the admin's lineage: %+v", page.Lineage)
	}

	status, body = nora.get("/api/catalog/object?catalog=lake&schema=public&name=orders")
	if status != http.StatusOK {
		t.Fatalf("nora: %d %s", status, body)
	}
	page.CanComment = true
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	if got := labels(page.Lineage); len(got) != 3 || got["1 hidden upstream table"] != -1 || got["1 hidden downstream table"] != 1 || page.CanComment {
		t.Errorf("nora's page: %+v %v", page.Lineage, page.CanComment)
	}
	for _, hidden := range []string{"secrets", "report", "Launch", "Daily"} {
		if strings.Contains(body, hidden) {
			t.Errorf("nora's page shows %q: %s", hidden, body)
		}
	}
	// A table she may not see is not found, just as one that does not
	// exist.
	statusHidden, bodyHidden := nora.get("/api/catalog/object?catalog=lake&schema=public&name=secrets")
	statusMissing, bodyMissing := nora.get("/api/catalog/object?catalog=lake&schema=public&name=nothing")
	if statusHidden != http.StatusNotFound || statusMissing != http.StatusNotFound ||
		strings.Replace(bodyHidden, "secrets", "nothing", 1) != bodyMissing {
		t.Errorf("hidden: %d %s; missing: %d %s", statusHidden, bodyHidden, statusMissing, bodyMissing)
	}
}

func TestCatalogComment(t *testing.T) {
	tc := newCatalogConsole(t)
	walt := tc.signedIn(t, "walt")
	nora := tc.signedIn(t, "nora")
	// The test engine starts sessions in its own database, not in the stand-in
	// catalog, whose main schema "lake"."notes" names.
	text := "Notes, by Walt's team"
	resp := walt.do("POST", "/api/catalog/comment", map[string]any{"kind": "table", "schema": "lake", "name": "notes", "comment": text}, nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("walt: %d %s", resp.StatusCode, body)
	}
	_, events := walt.query("SELECT comment FROM duckdb_tables() WHERE table_name = 'notes'")
	if len(events) < 3 || events[2].Rows == nil || *events[2].Rows[0][0] != text {
		t.Errorf("the comment was not set: %+v", events)
	}
	resp = nora.do("POST", "/api/catalog/comment", map[string]any{"kind": "table", "schema": "lake", "name": "notes", "comment": "x"}, nil)
	var e map[string]string
	decode(t, resp, &e)
	if resp.StatusCode != http.StatusForbidden || e["code"] != "42501" {
		t.Errorf("nora: %d %v", resp.StatusCode, e)
	}
	for _, bad := range []map[string]any{
		{"kind": "table", "schema": "main"},
		{"kind": "column", "schema": "main", "name": "notes"},
		{"kind": "database", "schema": "main", "name": "notes"},
	} {
		if resp := walt.do("POST", "/api/catalog/comment", bad, nil); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%v: %d", bad, resp.StatusCode)
		}
	}
	// Changes need the CSRF token, as everywhere.
	walt.csrf = ""
	if resp := walt.do("POST", "/api/catalog/comment", map[string]any{"kind": "table", "schema": "main", "name": "notes"}, nil); resp.StatusCode != http.StatusForbidden {
		t.Errorf("without CSRF: %d", resp.StatusCode)
	}
}

func TestLineageGraph(t *testing.T) {
	db := func(schema any) any {
		if schema == nil {
			return nil
		}
		return "lake"
	}
	row := func(upSchema, up, downSchema, down any) map[string]any {
		return map[string]any{"upstream_type": "TABLE", "upstream_database": db(upSchema), "upstream_schema": upSchema, "upstream_name": up,
			"downstream_database": db(downSchema), "downstream_schema": downSchema, "downstream_name": down}
	}
	rows := []map[string]any{
		row("s", "a", "s", "b"), row("s", "b", "s", "c"), row("s", "c", "s", "d"), row("s", "d", "s", "e"),
		row(nil, nil, "s", "b"), row(nil, nil, "s", "b"), row("s", "c", nil, nil),
		{"upstream_type": "LOCATION", "upstream_name": "s3://drops/", "downstream_database": "lake", "downstream_schema": "s", "downstream_name": "a"},
		row("s", "x", "s", "y"),
		// A connection's table of the same schema and name as a table of
		// the catalog is another table.
		{"upstream_type": "TABLE", "upstream_database": "crm", "upstream_schema": "s", "upstream_name": "a",
			"downstream_database": "lake", "downstream_schema": "s", "downstream_name": "a"},
	}
	nodes, edges := lineageGraph(rows, "lake", "S", "B")
	depth := map[string]int{}
	for _, n := range nodes {
		depth[n.Label] = n.Depth
	}
	for label, want := range map[string]int{"S.B": 0, "s.a": -1, "s3://drops/": -2, "crm.s.a": -2, "2 hidden upstream tables": -1, "s.c": 1,
		"s.d": 2, "s.e": 3, "1 hidden downstream table": 2} {
		if got, ok := depth[label]; !ok || got != want {
			t.Errorf("%s at %d (%v), want %d", label, got, ok, want)
		}
	}
	if _, ok := depth["s.x"]; ok || len(edges) != 8 {
		t.Errorf("nodes %v, %d edges", depth, len(edges))
	}
	for _, n := range nodes {
		if n.Label == "crm.s.a" && (n.Catalog != "crm" || n.Schema != "s" || n.Name != "a") {
			t.Errorf("the connection's table: %+v", n)
		}
	}
}

// The console's own lookups, which its pages make to show themselves, run
// under an application name of their own, so the history page can leave
// them out; what a user runs and changes runs under the console's. Both
// are in the history.
func TestLookupsAreNamed(t *testing.T) {
	tc := newCatalogConsole(t)
	walt := tc.signedIn(t, "walt")
	admin := tc.signedIn(t, "igloo")

	walt.rows("SELECT count(*) FROM lake.public.orders")
	if status, body := walt.get("/api/catalog/object?catalog=lake&schema=public&name=orders"); status != http.StatusOK {
		t.Fatalf("catalog page: %d %s", status, body)
	}
	if status, body := walt.get("/api/catalog/search?q=orders"); status != http.StatusOK {
		t.Fatalf("search: %d %s", status, body)
	}
	resp := walt.do("POST", "/api/query", map[string]any{"sql": "SHOW TASKS", "lookup": true}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a lookup: %s", resp.Status)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	// The stand-in catalog's main schema, as in TestCatalogComment.
	resp = walt.do("POST", "/api/catalog/comment", map[string]any{"kind": "table", "schema": "lake", "name": "notes", "comment": "Notes"}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("comment: %s", resp.Status)
	}

	rows := admin.rows(`SELECT application_name, string_agg(DISTINCT statement_type, ',' ORDER BY statement_type)
		FROM igloo.query_history WHERE user_name = 'walt' GROUP BY ALL ORDER BY 1`)
	var got []string
	for _, r := range rows {
		got = append(got, *r[0]+"|"+*r[1])
	}
	if want := []string{"Igloo console|COMMENT,SELECT", "Igloo console lookup|SELECT,SHOW TASKS,WITH"}; strings.Join(got, ";") != strings.Join(want, ";") {
		t.Errorf("walt's history by application:\n got %q\nwant %q", got, want)
	}
}
