package auth

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/xdg-go/scram"
)

// postgresVerifier was produced by PostgreSQL 17 for the password below, so
// accounts copied from Postgres (or set with psql's \password) keep working.
const (
	postgresVerifier = "SCRAM-SHA-256$4096:pgwMjYmXL0hco19rzQ3u2A==$IPoyWaURz8j4p0emZuNjRNDcEujy217e/WY2AausmUU=:1lgzgbspoHbYwWMpUZ8Wj1eRW4ahJy2jGCKplEoNrZY="
	postgresPassword = "correct horse battery staple"
)

// login runs a full SCRAM exchange of password against verifier.
func login(t *testing.T, verifier, password string) bool {
	t.Helper()
	creds, err := ParseVerifier(verifier)
	if err != nil {
		t.Fatal(err)
	}
	server, err := scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) { return creds, nil })
	if err != nil {
		t.Fatal(err)
	}
	client, err := scram.SHA256.NewClient("", password, "")
	if err != nil {
		t.Fatal(err)
	}

	sc, cc := server.NewConversation(), client.NewConversation()
	msg, err := cc.Step("")
	for err == nil && !sc.Done() {
		if msg, err = sc.Step(msg); err != nil {
			break
		}
		msg, err = cc.Step(msg)
	}
	return err == nil && sc.Valid()
}

func TestVerifierRoundTrip(t *testing.T) {
	v, err := NewVerifier("s3cret!")
	if err != nil {
		t.Fatal(err)
	}
	if !IsVerifier(v) {
		t.Fatalf("%q is not recognised as a verifier", v)
	}
	if !login(t, v, "s3cret!") {
		t.Error("correct password rejected")
	}
	if login(t, v, "wrong") {
		t.Error("wrong password accepted")
	}

	other, _ := NewVerifier("s3cret!")
	if other == v {
		t.Error("verifiers for the same password must use different salts")
	}
}

func TestPostgresVerifiersWork(t *testing.T) {
	if !login(t, postgresVerifier, postgresPassword) {
		t.Error("PostgreSQL verifier rejected its password")
	}
}

func TestParseVerifierRejectsJunk(t *testing.T) {
	for _, v := range []string{
		"", "password", "md5abcdef", "SCRAM-SHA-256$", "SCRAM-SHA-256$4096:salt",
		"SCRAM-SHA-256$x:c2FsdA==$a:b", "SCRAM-SHA-256$4096:c2FsdA==$YQ==:YQ==",
	} {
		if IsVerifier(v) {
			t.Errorf("%q accepted as a verifier", v)
		}
	}
}

func TestParseRole(t *testing.T) {
	for _, s := range []string{"admin", "WRITER", "reader"} {
		if _, err := ParseRole(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	if _, err := ParseRole("superuser"); err == nil {
		t.Error("unknown role accepted")
	}
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	created, err := s.EnsureAdmin(ctx, "igloo", "v")
	if err != nil || !created {
		t.Fatalf("EnsureAdmin: %v %v", created, err)
	}
	if created, _ := s.EnsureAdmin(ctx, "other", "v2"); created {
		t.Error("second admin created")
	}
	if u, err := s.Get(ctx, "igloo"); err != nil || u.Role != RoleAdmin {
		t.Errorf("Get: %+v %v", u, err)
	}
	if _, err := s.Get(ctx, "nobody"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get unknown: %v", err)
	}
	if err := s.Create(ctx, User{Name: "x"}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Create: %v", err)
	}
}

// TestPostgresStore runs against a real database when IGLOO_TEST_DATABASE_URL
// is set (make test starts one).
func TestPostgresStore(t *testing.T) {
	url := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()

	s, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_auth.users"); err != nil {
		t.Fatal(err)
	}

	// Migrations are idempotent, as several replicas run them.
	if s2, err := OpenPostgresStore(ctx, url); err != nil {
		t.Fatal(err)
	} else {
		s2.Close()
	}

	if created, err := s.EnsureAdmin(ctx, "root", "v1"); err != nil || !created {
		t.Fatalf("EnsureAdmin: %v %v", created, err)
	}
	if created, _ := s.EnsureAdmin(ctx, "root", "v2"); created {
		t.Error("EnsureAdmin replaced an existing user")
	}
	if u, _ := s.Get(ctx, "root"); u.Verifier != "v1" {
		t.Error("EnsureAdmin changed the existing password")
	}

	if err := s.Create(ctx, User{Name: "ana", Role: RoleReader, Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, User{Name: "ana", Role: RoleReader, Verifier: "v"}); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate Create: %v", err)
	}
	if err := s.SetRole(ctx, "ana", RoleWriter); err != nil {
		t.Fatal(err)
	}
	if err := s.SetVerifier(ctx, "ana", "v9"); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.Get(ctx, "ana"); u.Role != RoleWriter || u.Verifier != "v9" {
		t.Errorf("after updates: %+v", u)
	}

	users, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(users))
	for i, u := range users {
		names[i] = u.Name
	}
	if !slices.Equal(names, []string{"ana", "root"}) {
		t.Errorf("List: %v", names)
	}

	if err := s.SetRole(ctx, "root", RoleReader); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("demoting the last admin: %v", err)
	}
	if err := s.Delete(ctx, "root"); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("deleting the last admin: %v", err)
	}
	if err := s.Delete(ctx, "ana"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "ana"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleting twice: %v", err)
	}
	if err := s.SetVerifier(ctx, "ghost", "v"); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetVerifier unknown: %v", err)
	}
}
