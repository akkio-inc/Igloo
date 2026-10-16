package notify

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/testdb"
)

// testStores runs fn against a MemoryStore, and a PostgresStore when
// IGLOO_TEST_DATABASE_URL is set (make test starts one).
func testStores(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryStore()) })
	url := testdb.URL()
	if url == "" {
		return
	}
	t.Run("postgres", func(t *testing.T) {
		ctx := context.Background()
		s, err := OpenPostgres(ctx, url, bytes.Repeat([]byte{7}, 32))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		for _, table := range []string{"usage", "integrations", "alert_history", "claims"} {
			if _, err := s.pool.Exec(ctx, "DELETE FROM igloo_notify."+table); err != nil {
				t.Fatal(err)
			}
		}
		fn(t, s)
	})
}

func TestStore(t *testing.T) {
	testStores(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		in := Integration{Name: "ops", Type: TypeSlack, Enabled: true, URL: "https://hooks.slack.com/services/T1/B2/s3cr3t-t0ken",
			Builtin: Builtin{FailedRuns: true, IdlePipes: 6 * time.Hour}, Comment: "the ops channel", CreatedBy: "igloo"}
		if err := s.CreateIntegration(ctx, in, false); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateIntegration(ctx, in, false); !errors.Is(err, ErrExists) {
			t.Errorf("again: %v", err)
		}
		got, err := s.Integration(ctx, "ops")
		if err != nil || got.URL != in.URL || got.Builtin != in.Builtin || got.Comment != in.Comment || !got.Enabled ||
			got.CreatedBy != "igloo" {
			t.Fatalf("read back %+v, %v", got, err)
		}
		if _, err := s.Integration(ctx, "nope"); !errors.Is(err, ErrNotFound) {
			t.Errorf("missing: %v", err)
		}

		for _, u := range []Usage{{Integration: "ops", Grantee: "analysts", GranteeType: "role", GrantedBy: "igloo"},
			{Integration: "ops", Grantee: "ana", GranteeType: "user"}, {Integration: "ops", Grantee: "ana", GranteeType: "user"}} {
			if err := s.GrantUsage(ctx, u); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.GrantUsage(ctx, Usage{Integration: "nope", Grantee: "ana", GranteeType: "user"}); !errors.Is(err, ErrNotFound) {
			t.Errorf("a grant on a missing integration: %v", err)
		}
		if us, _ := s.Usage(ctx); len(us) != 2 {
			t.Errorf("usage: %+v", us)
		}

		// Replacing keeps who may use it; dropping takes it away.
		in.Type, in.URL = TypeWebhook, "https://example.com/hook"
		if err := s.CreateIntegration(ctx, in, true); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Integration(ctx, "ops"); got.Type != TypeWebhook || got.URL != in.URL {
			t.Errorf("replaced: %+v", got)
		}
		if err := s.ForgetGrantee(ctx, "user", "ana"); err != nil {
			t.Fatal(err)
		}
		if us, _ := s.Usage(ctx); len(us) != 1 || us[0].Grantee != "analysts" {
			t.Errorf("after forgetting ana: %+v", us)
		}
		in.Enabled = false
		if err := s.UpdateIntegration(ctx, in); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Integration(ctx, "ops"); got.Enabled {
			t.Error("still enabled")
		}
		if err := s.DropIntegration(ctx, "ops"); err != nil {
			t.Fatal(err)
		}
		if us, _ := s.Usage(ctx); len(us) != 0 {
			t.Errorf("grants outlive their integration: %+v", us)
		}

		// The alert history shows users their own records.
		now := time.Now().Truncate(time.Millisecond)
		for i, r := range []Record{
			{ID: "a1", Type: RecordAlert, Name: "pacing", User: "ana", Trigger: "schedule", Start: now.Add(-time.Minute), State: StateTriggered, Rows: 3},
			{ID: "a2", Type: RecordAlert, Name: "drops", User: "bo", Trigger: "manual", Start: now, State: StateConditionFalse, Rows: 0},
			{ID: "a3", Type: RecordBuiltin, Name: BuiltinFailedRuns, Trigger: TriggerEvent, Start: now, State: StateActionFailed, Rows: -1,
				Error: "rate limited"},
		} {
			if err := s.WriteRecord(ctx, r); err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
		}
		if rs, _ := s.Records(ctx, audit.Viewer{User: "ana"}, audit.Page{}); len(rs) != 1 || rs[0].ID != "a1" || rs[0].Rows != 3 {
			t.Errorf("ana's history: %+v", rs)
		}
		if rs, _ := s.Records(ctx, audit.Viewer{All: true}, audit.Page{Limit: 2}); len(rs) != 2 || rs[0].ID != "a3" || rs[1].ID != "a2" {
			t.Errorf("everything, newest first: %+v", rs)
		}

		// A claim holds for its window.
		for i, want := range []bool{true, false} {
			if ok, err := s.Claim(ctx, "ops/failed_runs/task/x", time.Hour); err != nil || ok != want {
				t.Errorf("claim %d: %v, %v", i, ok, err)
			}
		}
		if ok, _ := s.Claim(ctx, "ops/failed_runs/task/x", 0); !ok {
			t.Error("a claim without a window")
		}
		if err := s.Prune(ctx, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if rs, _ := s.Records(ctx, audit.Viewer{All: true}, audit.Page{}); len(rs) != 2 {
			t.Errorf("after pruning: %+v", rs)
		}
		if ok, _ := s.Claim(ctx, "ops/failed_runs/task/x", time.Hour); !ok {
			t.Error("pruned claims still hold")
		}
	})
}

// TestSealed checks that secrets are stored encrypted, bound to their
// integration, and unreadable with another key.
func TestSealed(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, url, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, name := range []string{"sealed_a", "sealed_b"} {
		_, _ = s.pool.Exec(ctx, "DELETE FROM igloo_notify.integrations WHERE name = $1", name)
		in := Integration{Name: name, Type: TypeWebhook, Enabled: true, URL: "https://example.com/hook/very-secret-" + name,
			Headers: map[string]string{"Authorization": "Bearer header-secret-" + name}, Secret: "signing-secret-" + name}
		if err := s.CreateIntegration(ctx, in, false); err != nil {
			t.Fatal(err)
		}
	}
	var dump string
	if err := s.pool.QueryRow(ctx, `SELECT string_agg(i::text, ' ') FROM igloo_notify.integrations i
		WHERE name LIKE 'sealed_%'`).Scan(&dump); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"very-secret", "header-secret", "signing-secret", "/hook/"} {
		if bytes.Contains([]byte(dump), []byte(secret)) {
			t.Errorf("the table holds %q in the clear: %s", secret, dump)
		}
	}
	if !bytes.Contains([]byte(dump), []byte("https://example.com")) {
		t.Errorf("the target is not kept for SHOW INTEGRATIONS: %s", dump)
	}

	// Secrets moved to another integration's row do not decrypt there.
	if _, err := s.pool.Exec(ctx, `UPDATE igloo_notify.integrations SET sealed = (SELECT sealed FROM igloo_notify.integrations
		WHERE name = 'sealed_a') WHERE name = 'sealed_b'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Integration(ctx, "sealed_b"); err == nil {
		t.Error("another integration's secrets decrypt")
	}
	other, err := OpenPostgres(ctx, url, bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Integration(ctx, "sealed_a"); err == nil {
		t.Error("secrets decrypt with another key")
	}
	if got, err := s.Integration(ctx, "sealed_a"); err != nil || got.Secret != "signing-secret-sealed_a" {
		t.Errorf("read back: %+v, %v", got, err)
	}
	_, _ = s.pool.Exec(ctx, "DELETE FROM igloo_notify.integrations WHERE name LIKE 'sealed_%'")
}
