package iddomain

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

func TestNamesAndTypes(t *testing.T) {
	for _, n := range []string{"people", "_x", "a1_b"} {
		if err := CheckName(n); err != nil {
			t.Errorf("%q: %v", n, err)
		}
	}
	for _, n := range []string{"", "People", "1a", "a-b", "a b", string(make([]byte, 64))} {
		if err := CheckName(n); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: want ErrInvalid, got %v", n, err)
		}
	}
	for in, want := range map[string]string{"varchar": "VARCHAR", " TEXT ": "VARCHAR", "uuid": "UUID", "int8": "BIGINT", "int": "INTEGER"} {
		if got, err := CheckType(in); err != nil || got != want {
			t.Errorf("%q: %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"DOUBLE", "DATE", "VARCHAR[]", ""} {
		if _, err := CheckType(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: want ErrInvalid, got %v", in, err)
		}
	}
	if HiddenColumn("person_id") != "_igloo_id_person_id" || !IsHidden("_IGLOO_ID_x") || IsHidden("igloo_id") || IsHidden("_igloo_i") {
		t.Error("hidden column names")
	}
}

// testStore checks a Store that starts empty.
func testStore(t *testing.T, s Store) {
	ctx := context.Background()
	v0, err := s.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	people := Domain{Name: "people", Type: "varchar", Tags: map[string]string{"pii": "person_id"}, Generation: 1, Created: time.Now().UTC()}
	if err := s.Update(ctx, func(c *Catalog) error { return c.AddDomain(people) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(ctx, func(c *Catalog) error { return c.AddDomain(people) }); !errors.Is(err, ErrExists) {
		t.Errorf("second CREATE: %v", err)
	}
	if err := s.Update(ctx, func(c *Catalog) error { return c.AddDomain(Domain{Name: "Bad", Type: "VARCHAR"}) }); !errors.Is(err, ErrInvalid) {
		t.Errorf("bad name: %v", err)
	}
	m := Member{Domain: "PEOPLE", Schema: "s", Table: "t", Column: "person_id"}
	if err := s.Update(ctx, func(c *Catalog) error { return c.AddMember(m) }); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Member{
		{Domain: "nope", Schema: "s", Table: "u", Column: "id"},
		{Domain: "people", Schema: "s", Table: "t", Column: "_igloo_id_person_id"},
	} {
		if err := s.Update(ctx, func(c *Catalog) error { return c.AddMember(bad) }); err == nil {
			t.Errorf("member %+v accepted", bad)
		}
	}
	if err := s.Update(ctx, func(c *Catalog) error {
		if err := c.AddDomain(Domain{Name: "devices", Type: "UUID", Generation: 1}); err != nil {
			return err
		}
		return c.AddMember(Member{Domain: "devices", Schema: "S", Table: "T", Column: "PERSON_ID"})
	}); !errors.Is(err, ErrInvalid) {
		t.Errorf("a column in two domains: %v", err)
	}
	snap, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != v0+2 {
		t.Errorf("version %d, want %d (failed changes do not count)", snap.Version, v0+2)
	}
	d := snap.Domain("People")
	if d == nil || d.Type != "VARCHAR" || d.Tags["pii"] != "person_id" || d.Generation != 1 {
		t.Fatalf("domain: %+v", d)
	}
	if snap.Domain("devices") != nil {
		t.Error("the failed change added a domain")
	}
	if got := snap.MemberOf("S", "T", "PERSON_ID"); got == nil || got.Domain != "people" || got.String() != "s.t.person_id" {
		t.Errorf("member: %+v", got)
	}
	if got := snap.TableMembers("s", "t"); len(got) != 1 {
		t.Errorf("table members: %+v", got)
	}
	if err := s.Update(ctx, func(c *Catalog) error { return c.DropDomain("people") }); !errors.Is(err, ErrInUse) {
		t.Errorf("drop in use: %v", err)
	}
	if err := s.Update(ctx, func(c *Catalog) error {
		if !c.RemoveMember("s", "t", "person_id") {
			return errors.New("not a member")
		}
		return c.DropDomain("people")
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(ctx, func(c *Catalog) error { return c.DropDomain("people") }); !errors.Is(err, ErrNotFound) {
		t.Errorf("drop again: %v", err)
	}

	// Ids: consecutive, never twice, within a generation.
	if next, gen, err := s.Next(ctx, "people"); err != nil || next != 0 || gen != 0 {
		t.Errorf("next of a new counter: %d, %d, %v", next, gen, err)
	}
	if first, err := s.Allocate(ctx, "people", 1, 5); err != nil || first != 0 {
		t.Fatalf("allocate: %d, %v", first, err)
	}
	var mu sync.Mutex
	var got [][2]int64
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				n := int64(1 + (w+i)%7)
				first, err := s.Allocate(ctx, "people", 1, n)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				got = append(got, [2]int64{first, first + n})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	slices.SortFunc(got, func(a, b [2]int64) int { return int(a[0] - b[0]) })
	at := int64(5)
	for _, r := range got {
		if r[0] != at {
			t.Fatalf("ranges overlap or leave a gap at %d: %v", at, r)
		}
		at = r[1]
	}
	if next, gen, err := s.Next(ctx, "people"); err != nil || next != at || gen != 1 {
		t.Errorf("next: %d, %d, %v; want %d, 1", next, gen, err, at)
	}
	if _, err := s.Allocate(ctx, "people", 0, 1); !errors.Is(err, ErrInvalid) {
		t.Errorf("an older generation: %v", err)
	}
	if _, err := s.Allocate(ctx, "people", 1, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("zero ids: %v", err)
	}

	// The last ids below 2^32, and none after them.
	if first, err := s.Allocate(ctx, "people", 1, MaxIDs-at-10); err != nil || first != at {
		t.Fatalf("allocate up to the limit: %d, %v", first, err)
	}
	if _, err := s.Allocate(ctx, "people", 1, 11); !errors.Is(err, ErrExhausted) {
		t.Errorf("past the limit: %v", err)
	}
	if first, err := s.Allocate(ctx, "people", 1, 10); err != nil || first != MaxIDs-10 {
		t.Errorf("the last ten: %d, %v", first, err)
	}
	if _, err := s.Allocate(ctx, "people", 1, 1); !errors.Is(err, ErrExhausted) {
		t.Errorf("after the last: %v", err)
	}
	// A new generation starts again from 0.
	if first, err := s.Allocate(ctx, "people", 2, 3); err != nil || first != 0 {
		t.Errorf("new generation: %d, %v", first, err)
	}

	// Leases.
	unlockA, err := s.Lock(ctx, "people", "a", time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lock(ctx, "people", "b", time.Minute, 150*time.Millisecond); !errors.Is(err, ErrBusy) {
		t.Errorf("a held lease: %v", err)
	}
	if _, err := s.Lock(ctx, "other", "b", time.Minute, 0); err != nil {
		t.Errorf("another domain's lease: %v", err)
	}
	again, err := s.Lock(ctx, "people", "a", time.Minute, 0)
	if err != nil {
		t.Errorf("the holder again: %v", err)
	}
	_ = again
	done := make(chan error, 1)
	go func() {
		unlock, err := s.Lock(ctx, "people", "b", time.Minute, 5*time.Second)
		if err == nil {
			unlock()
		}
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	unlockA()
	if err := <-done; err != nil {
		t.Errorf("after unlock: %v", err)
	}
	if _, err := s.Lock(ctx, "people", "c", 300*time.Millisecond, 0); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	unlock, err := s.Lock(ctx, "people", "d", time.Minute, 5*time.Second)
	if err != nil {
		t.Fatalf("an expired lease: %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Error("took a lease before it expired")
	}
	unlock()

	// Erasures, counted by domain.
	if n, err := s.Erased(ctx, "people"); err != nil || n != 0 {
		t.Errorf("erasures at first: %d, %v", n, err)
	}
	for range 2 {
		if err := s.MarkErased(ctx, "people"); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.Erased(ctx, "people"); err != nil || n != 2 {
		t.Errorf("erasures: %d, %v", n, err)
	}
	if n, err := s.Erased(ctx, "other"); err != nil || n != 0 {
		t.Errorf("another domain's erasures: %d, %v", n, err)
	}
}

func TestMemoryStore(t *testing.T) { testStore(t, NewMemoryStore()) }

func TestPostgresStore(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, q := range []string{`UPDATE igloo_iddomain.catalog SET doc = '{}'`, `DELETE FROM igloo_iddomain.counters`, `DELETE FROM igloo_iddomain.leases`,
		`DELETE FROM igloo_iddomain.erasures`} {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	testStore(t, s)
	// Opening again finds the tables there.
	s2, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
}
