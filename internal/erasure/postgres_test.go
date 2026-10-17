package erasure

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/tasks"
	"github.com/akkio-inc/igloo/internal/testdb"
)

func testDatabase(t *testing.T) string {
	t.Helper()
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	return url
}

func TestStores(t *testing.T) {
	stores := map[string]Store{"memory": NewMemoryStore()}
	if url := testdb.URL(); url != "" {
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
	r := &Request{ID: id, Requester: "privacy", Created: now, Status: StatusQueued, Step: StepFind, Salt: "salt", Sealed: "sealed",
		Subjects: []Subject{{Tag: "pii", Value: "email", Count: 1}}}
	if err := s.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Status, r.Tables = StatusRunning, []Table{{Name: "crm.people", Ident: []string{"crm", "people"}, RowsDeleted: 2}}
	if err := s.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, id)
	if err != nil || got.Status != StatusRunning || got.Tables[0].RowsDeleted != 2 || got.Sealed != "sealed" || got.Tables[0].Ident[1] != "people" {
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
	deleted := now.Add(time.Second)
	if err := s.Deleted(ctx, id, []string{files[0].Path}, deleted); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Files(ctx, id)
	if err != nil || len(stored) != 2 || !stored[0].Deleted.Equal(deleted) || !stored[1].Deleted.IsZero() {
		t.Errorf("files: %+v, %v", stored, err)
	}
	// Once the request ends, its paths go: they can hold identifiers.
	if err := s.Forget(ctx, id); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Files(ctx, id); err != nil || len(stored) != 0 {
		t.Errorf("files after Forget: %+v, %v", stored, err)
	}

	// Caches are emptied of what any request deleted, up to when.
	cache := "disk:" + id
	if err := s.Seen(ctx, cache, "replica-1", now); err != nil {
		t.Fatal(err)
	}
	r.DeletedThrough = deleted
	if err := s.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	if latest, err := s.LatestDeletion(ctx); err != nil || latest.Before(deleted) {
		t.Errorf("latest deletion: %v, %v", latest, err)
	}
	cacheOf := func() Cache {
		t.Helper()
		caches, err := s.Caches(ctx)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(caches, func(c Cache) bool { return c.ID == cache })
		if i < 0 {
			t.Fatalf("cache %s is not listed: %+v", cache, caches)
		}
		return caches[i]
	}
	if c := cacheOf(); c.Host != "replica-1" || !c.Emptied.IsZero() {
		t.Errorf("cache: %+v", c)
	}
	if err := s.Emptied(ctx, cache, deleted); err != nil {
		t.Fatal(err)
	}
	if err := s.Emptied(ctx, cache, now); err != nil {
		t.Fatal(err)
	}
	if c := cacheOf(); !c.Emptied.Equal(deleted) {
		t.Errorf("emptied: %v, want %v", c.Emptied, deleted)
	}
}

// The history stores read every record to the redactor, nothing about
// the identifiers reaches PostgreSQL, and query tags and application names
// are redacted too.
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
		{Text: "SELECT 1", QueryTag: "campaign for " + email},
		{Text: "SELECT 2", ApplicationName: email},
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

	r := &Request{salt: "pepper", Subjects: []Subject{testSubject("pepper", "pii", "email", email)}}
	x := newRedactor(r)
	n, err := history.Redact(ctx, x)
	if err != nil || n != 6 {
		t.Errorf("query history: redacted %d, %v; want 6", n, err)
	}
	if n, err := history.Redact(ctx, x); err != nil || n != 0 {
		t.Errorf("redacting again: %d, %v", n, err)
	}
	if n, err := jobs.Redact(ctx, x); err != nil || n != 1 {
		t.Errorf("load history: redacted %d, %v; want 1", n, err)
	}
	got, err := history.Queries(ctx, audit.Viewer{All: true}, audit.Page{Since: now.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range got {
		if strings.Contains(strings.ToLower(q.Text+q.QueryTag+q.ApplicationName), email) {
			t.Errorf("history still holds the identifier: %+v", q)
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
