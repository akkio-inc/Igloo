package erasure

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/tasks"
)

func testDatabase(t *testing.T) string {
	t.Helper()
	url := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	return url
}

func TestStores(t *testing.T) {
	stores := map[string]Store{"memory": NewMemoryStore()}
	if url := os.Getenv("IGLOO_TEST_DATABASE_URL"); url != "" {
		pg, err := OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close()
		// Another replica starting up must not fail on the tables.
		again, err := OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		again.Close()
		stores["postgres"] = pg
	}
	for name, store := range stores {
		t.Run(name, func(t *testing.T) { testStore(t, store) })
	}
}

func testStore(t *testing.T, s Store) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	id := audit.NewID(now)
	r := &Request{ID: id, Requester: "privacy", Created: now, Status: StatusQueued, Step: StepFind, Salt: "salt",
		Subjects: []Subject{newSubject("salt", "pii", "email", []string{"ann@example.com"})}}
	if err := s.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Status, r.Tables = StatusRunning, []Table{{Name: "crm.people", RowsDeleted: 2}}
	if err := s.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, id)
	if err != nil || got.Status != StatusRunning || got.Tables[0].RowsDeleted != 2 || got.Subjects[0].Plain[0] != r.Subjects[0].Plain[0] {
		t.Fatalf("get: %+v, %v", got, err)
	}
	if _, err := s.Get(ctx, "nope"); err != ErrNotFound {
		t.Errorf("get of a missing request: %v", err)
	}
	all, err := s.List(ctx)
	if err != nil || !slices.ContainsFunc(all, func(x *Request) bool { return x.ID == id }) {
		t.Errorf("list: %v", err)
	}

	files := []File{{Path: "s3://b/" + id + "/a", Table: "crm.people", Kind: fileMatched}, {Path: "s3://b/" + id + "/b", Table: "crm.people", Kind: fileExpired}}
	if err := s.AddFiles(ctx, id, files); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(ctx, id, files[:1]); err != nil {
		t.Fatal(err)
	}
	cache := "disk:" + id
	if err := s.Seen(ctx, cache, "replica-1", now); err != nil {
		t.Fatal(err)
	}
	pending := func() []string {
		t.Helper()
		u, err := s.Unpurged(ctx, cache)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(u, func(u Unpurged) bool { return u.Request == id })
		if i < 0 {
			return nil
		}
		return u[i].Paths
	}
	if p := pending(); p != nil {
		t.Errorf("files not deleted yet are not for caches to drop: %v", p)
	}
	deleted := now.Add(time.Second)
	if err := s.Deleted(ctx, id, []string{files[0].Path}, deleted); err != nil {
		t.Fatal(err)
	}
	if p := pending(); !slices.Equal(p, []string{files[0].Path}) {
		t.Errorf("unpurged: %v", p)
	}
	if err := s.Purged(ctx, id, cache, deleted); err != nil {
		t.Fatal(err)
	}
	if p := pending(); p != nil {
		t.Errorf("purged files are still pending: %v", p)
	}
	purges, err := s.Purges(ctx, id)
	if err != nil || !purges[cache].Equal(deleted) {
		t.Errorf("purges: %v, %v", purges, err)
	}
	stored, err := s.Files(ctx, id)
	if err != nil || len(stored) != 2 || !stored[0].Deleted.Equal(deleted) || !stored[1].Deleted.IsZero() {
		t.Errorf("files: %+v, %v", stored, err)
	}
	caches, err := s.Caches(ctx)
	if err != nil || !slices.ContainsFunc(caches, func(c Cache) bool { return c.ID == cache && c.Host == "replica-1" }) {
		t.Errorf("caches: %+v, %v", caches, err)
	}
}

// The history stores find candidates in PostgreSQL by the hashes alone,
// with the same words and literals redaction looks at.
func TestPostgresRedaction(t *testing.T) {
	url := testDatabase(t)
	ctx := context.Background()
	history, err := audit.OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	jobs, err := tasks.OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer jobs.Close()

	now := time.Now()
	email := "erase-" + audit.NewID(now)[:8] + "@example.com"
	queries := []audit.Query{
		{Text: "SELECT * FROM crm.people WHERE email = '" + strings.ToUpper(email) + "'"},
		{Text: "SELECT * FROM crm.people WHERE email IN ('x@y.z', '  " + email + "')"},
		{Text: "SELECT * FROM crm.people WHERE note LIKE '%" + email + "%' -- " + email},
		{Text: `SELECT '{"email":"` + email + `"}'::JSON`},
		{Text: "SELECT * FROM crm.people WHERE email = 'someone-else@example.com'"},
		{Text: "INSERT INTO crm.report SELECT * FROM crm.people", StatementType: "INSERT"},
		{Text: "SELECT 1", StatementType: "INSERT", TablesRead: []string{"lake.crm.people"}, TablesWritten: []string{"lake.crm.copy"}},
	}
	for i := range queries {
		queries[i].ID, queries[i].SessionID, queries[i].User, queries[i].Role = audit.NewID(now), "s", "ana", "admin"
		queries[i].Start, queries[i].Outcome = now, audit.OutcomeOK
		if queries[i].StatementType == "" {
			queries[i].StatementType = "SELECT"
		}
	}
	if err := history.WriteQueries(ctx, queries); err != nil {
		t.Fatal(err)
	}
	load := tasks.Load{ID: audit.NewID(now), Table: "lake.crm.people", File: "s3://drops/a.csv", Status: tasks.LoadFailed,
		FirstError: `Could not convert string "` + email + `" to INT64`, Start: now, User: "ana"}
	if err := jobs.WriteLoads(ctx, []tasks.Load{load}); err != nil {
		t.Fatal(err)
	}

	r := &Request{Salt: "pepper", Subjects: []Subject{newSubject("pepper", "pii", "email", []string{email})}}
	m := newMatcher(r)
	n, err := history.Redact(ctx, m)
	if err != nil || n != 4 {
		t.Errorf("query history: redacted %d, %v; want 4", n, err)
	}
	if n, err := history.Redact(ctx, m); err != nil || n != 0 {
		t.Errorf("redacting again: %d, %v", n, err)
	}
	if n, err := jobs.Redact(ctx, m); err != nil || n != 1 {
		t.Errorf("load history: redacted %d, %v; want 1", n, err)
	}
	got, err := history.Queries(ctx, audit.Viewer{All: true}, audit.Page{Since: now.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range got {
		if strings.Contains(strings.ToLower(q.Text), email) {
			t.Errorf("history still holds the identifier: %s", q.Text)
		}
	}
	if !slices.ContainsFunc(got, func(q audit.Query) bool { return strings.Contains(q.Text, "someone-else@example.com") }) {
		t.Errorf("an unrelated statement was redacted")
	}
	loads, err := jobs.Loads(ctx, audit.Viewer{All: true}, audit.Page{Since: now.Add(-time.Second)})
	if err != nil || len(loads) == 0 || strings.Contains(loads[0].FirstError, email) {
		t.Errorf("load history: %+v, %v", loads, err)
	}

	// Lineage: statements that read the table, as recorded or by name.
	writes, err := history.Writes(ctx, []string{"lake.crm.people"}, []string{"people"})
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, w := range writes {
		texts = append(texts, w.Text)
	}
	for _, want := range []string{"INSERT INTO crm.report SELECT * FROM crm.people", "SELECT 1"} {
		if !slices.Contains(texts, want) {
			t.Errorf("writes: %q, want %q among them", texts, want)
		}
	}
}
