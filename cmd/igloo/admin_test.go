package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/testdb"
)

// Passwords for these tests; none is a real secret.
const (
	strongPassword = "Kq7vT2mXz9LpR4wN8sYb3cJh" // 24 characters
	short15        = "Kq7vT2mXz9LpR4w"
	long16         = "Kq7vT2mXz9LpR4wN"
)

func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestEnsureAdminRefusesWeakPasswordsForANewAccount(t *testing.T) {
	ctx := context.Background()
	for _, password := range []string{
		"change-me", "CHANGE-ME", "changeme", "please-change-me-0123456789", "Change_Me_Now_0123456789",
		"password", "admin", "igloo", "secret", "default", "zzzzzzzzzzzzzzzzzzzzzzzz",
		short15,
	} {
		store := auth.NewMemoryStore()
		var logs bytes.Buffer
		err := ensureAdmin(ctx, store, "igloo", password, testLogger(&logs))
		if err == nil {
			t.Errorf("%q: created the admin account", password)
			continue
		}
		if _, err := store.Get(ctx, "igloo"); !errors.Is(err, auth.ErrNotFound) {
			t.Errorf("%q: the account exists after the refusal: %v", password, err)
		}
		if msg := err.Error(); !strings.Contains(msg, "IGLOO_ADMIN_PASSWORD") || !strings.Contains(msg, "openssl rand") {
			t.Errorf("%q: the error should name the variable and say what to do: %s", password, msg)
		}
	}
}

func TestEnsureAdminErrorsDoNotContainThePassword(t *testing.T) {
	ctx := context.Background()
	// Dictionary words such as "password" are in any message about
	// passwords, so these are values that could appear only if echoed.
	for _, password := range []string{"change-me", "Xy-change-me-Qz-0123456789", "Tr0ub4dor&3", short15, "qqqqqqqqqqqqqqqqqqqq"} {
		var logs bytes.Buffer
		err := ensureAdmin(ctx, auth.NewMemoryStore(), "igloo", password, testLogger(&logs))
		if err == nil {
			t.Fatalf("%q: created the admin account", password)
		}
		if strings.Contains(err.Error(), password) {
			t.Errorf("the error repeats the password %q: %s", password, err)
		}
		if strings.Contains(logs.String(), password) {
			t.Errorf("the log repeats the password %q: %s", password, logs.String())
		}
	}
}

func TestEnsureAdminRefusesTheUsersName(t *testing.T) {
	name := "warehouse_administrator"
	if err := ensureAdmin(context.Background(), auth.NewMemoryStore(), name, name, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("created an admin whose password is its name")
	}
}

func TestEnsureAdminCreatesWithSixteenCharacters(t *testing.T) {
	ctx := context.Background()
	store := auth.NewMemoryStore()
	var logs bytes.Buffer
	if err := ensureAdmin(ctx, store, "igloo", long16, testLogger(&logs)); err != nil {
		t.Fatal(err)
	}
	u, err := store.Get(ctx, "igloo")
	if err != nil || u.Role != auth.RoleAdmin || !auth.IsVerifier(u.Verifier) {
		t.Fatalf("admin = %+v, %v", u, err)
	}
	if !strings.Contains(logs.String(), "created admin user") {
		t.Errorf("no log of the new account: %s", logs.String())
	}
}

// An install made before the check, with a short password in its
// environment, keeps starting: the account exists, so the variable is not
// used, and Igloo only warns, without repeating it.
func TestEnsureAdminWarnsAboutAWeakPasswordForAnExistingAccount(t *testing.T) {
	ctx := context.Background()
	store := auth.NewMemoryStore()
	if err := ensureAdmin(ctx, store, "igloo", strongPassword, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Get(ctx, "igloo")
	for _, password := range []string{"Sh0rt-pass", short15, "change-me"} {
		var logs bytes.Buffer
		if err := ensureAdmin(ctx, store, "igloo", password, testLogger(&logs)); err != nil {
			t.Fatalf("%q: an existing account stopped Igloo starting: %v", password, err)
		}
		out := logs.String()
		if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "IGLOO_ADMIN_PASSWORD") {
			t.Errorf("%q: no warning naming the variable: %s", password, out)
		}
		if strings.Contains(out, password) {
			t.Errorf("the warning repeats the password %q: %s", password, out)
		}
		if after, _ := store.Get(ctx, "igloo"); after.Verifier != before.Verifier {
			t.Errorf("%q: the account's password changed", password)
		}
	}

	// A strong value for an existing account changes nothing and says nothing.
	var logs bytes.Buffer
	if err := ensureAdmin(ctx, store, "igloo", long16, testLogger(&logs)); err != nil || logs.Len() != 0 {
		t.Errorf("strong password for an existing account: %v, logs %q", err, logs.String())
	}
	if after, _ := store.Get(ctx, "igloo"); after.Verifier != before.Verifier {
		t.Error("the account's password changed")
	}
}

// failingStore fails to look users up.
type failingStore struct{ auth.Store }

func (failingStore) Get(context.Context, string) (auth.User, error) {
	return auth.User{}, errors.New("connection refused")
}

func TestEnsureAdminReportsLookupFailures(t *testing.T) {
	err := ensureAdmin(context.Background(), failingStore{auth.NewMemoryStore()}, "igloo", short15, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("got %v", err)
	}
}

// TestEnsureAdminReplicas starts replicas at the same time against one
// metadata database, as a Deployment's pods do, when IGLOO_TEST_DATABASE_URL
// is set (make test starts one).
func TestEnsureAdminReplicas(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	const replicas = 8
	stores := make([]*auth.PostgresStore, replicas)
	for i := range stores {
		s, err := auth.OpenPostgresStore(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		stores[i] = s
	}
	// start runs ensureAdmin on every store at once, replica i with
	// passwords[i % len(passwords)], and returns their errors and logs.
	start := func(stores []*auth.PostgresStore, user string, passwords ...string) ([]error, string) {
		errs := make([]error, len(stores))
		logs := make([]bytes.Buffer, len(stores))
		var wg sync.WaitGroup
		for i, s := range stores {
			wg.Go(func() {
				errs[i] = ensureAdmin(ctx, s, user, passwords[i%len(passwords)], testLogger(&logs[i]))
			})
		}
		wg.Wait()
		var all strings.Builder
		for i := range logs {
			all.Write(logs[i].Bytes())
		}
		return errs, all.String()
	}

	// A weak password: every replica refuses, and none creates the account.
	errs, _ := start(stores, "admin_weak", "change-me")
	for i, err := range errs {
		if err == nil {
			t.Errorf("replica %d started with a placeholder password and no account", i)
		}
	}
	if _, err := stores[0].Get(ctx, "admin_weak"); !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("a weak password created the account: %v", err)
	}

	// A strong password: exactly one replica creates the account, and all start.
	errs, logs := start(stores, "admin_strong", strongPassword)
	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d: %v", i, err)
		}
	}
	if n := strings.Count(logs, "created admin user"); n != 1 {
		t.Errorf("%d replicas created the account, want 1", n)
	}
	created, err := stores[0].Get(ctx, "admin_strong")
	if err != nil {
		t.Fatal(err)
	}

	// Replicas with a short value in their environment, after the account
	// exists (an upgrade), all start with a warning and change nothing.
	errs, logs = start(stores, "admin_strong", short15)
	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d with a short password and an existing account: %v", i, err)
		}
	}
	if n := strings.Count(logs, "level=WARN"); n != replicas {
		t.Errorf("%d warnings, want %d: %s", n, replicas, logs)
	}
	if strings.Contains(logs, short15) {
		t.Error("a warning repeats the password")
	}

	if !madeWith(t, created, strongPassword) {
		t.Error("the account's password is not the one it was created with")
	}

	// Replicas with different values racing to create the account (half of
	// them short): whichever gets there first, only the strong one may
	// create it, and every replica with it starts.
	for round := range 5 {
		user := fmt.Sprintf("admin_mixed_%d", round)
		errs, _ := start(stores, user, short15, strongPassword)
		for i, err := range errs {
			if i%2 == 1 && err != nil {
				t.Errorf("round %d: replica %d with the strong password: %v", round, i, err)
			}
		}
		u, err := stores[0].Get(ctx, user)
		if err != nil {
			t.Fatalf("round %d: no account: %v", round, err)
		}
		if !madeWith(t, u, strongPassword) {
			t.Errorf("round %d: the account was created with the short password", round)
		}
	}
}

// madeWith reports whether u's verifier was made from password.
func madeWith(t *testing.T, u auth.User, password string) bool {
	t.Helper()
	creds, err := auth.ParseVerifier(u.Verifier)
	if err != nil {
		t.Fatal(err)
	}
	v, err := auth.NewVerifierWith(password, creds.KeyFactors)
	if err != nil {
		t.Fatal(err)
	}
	return v == u.Verifier
}
