package clone_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/testdb"
)

func testStore(t *testing.T, s, other clone.Store) {
	ctx := context.Background()
	a, b := uuid.New(), uuid.New()
	loc := "s3://bucket/warehouse/" + uuid.NewString()
	created := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	if err := s.Add(ctx, []clone.Share{
		{Clone: a, Location: loc + "/", Schema: "dev", Table: "t", Created: created},
		{Clone: b, Location: loc, Schema: "dev", Table: "u"},
		{Clone: b, Location: loc + "x", Schema: "dev", Table: "u"},
	}); err != nil {
		t.Fatal(err)
	}
	// Seen again under a new name: the name changes, not when it was made.
	if err := s.Add(ctx, []clone.Share{{Clone: a, Location: loc, Schema: "dev", Table: "renamed"}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Sharers(ctx, loc)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("sharers: %+v", got)
	}
	for _, sh := range got {
		if sh.Clone == a && (sh.Table != "renamed" || !sh.Created.Equal(created) || sh.Location != loc) {
			t.Errorf("share of a: %+v", sh)
		}
	}
	if of, _ := s.Of(ctx, b); len(of) != 2 {
		t.Errorf("shares of b: %+v", of)
	}
	if err := s.Remove(ctx, b, loc); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Sharers(ctx, loc); len(got) != 1 || got[0].Clone != a {
		t.Errorf("after removing b: %+v", got)
	}

	// Locks: one holder at a time, whichever store asks.
	release, ok, err := s.Lock(ctx, []string{loc, loc + "x"}, true)
	if err != nil || !ok {
		t.Fatalf("lock: %v %v", ok, err)
	}
	if _, ok, err := other.Lock(ctx, []string{loc + "/"}, false); ok || err != nil {
		t.Fatalf("a held lock was taken again: %v %v", ok, err)
	}
	waited := make(chan error, 1)
	go func() {
		r, _, err := other.Lock(ctx, []string{loc}, true)
		if err == nil {
			r()
		}
		waited <- err
	}()
	select {
	case err := <-waited:
		t.Fatalf("waited for nothing: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	release, _, _ = s.Lock(ctx, []string{loc}, true)
	if _, _, err := other.Lock(short, []string{loc}, true); err == nil {
		t.Fatal("a canceled wait took the lock")
	}
	release()
	// The canceled wait's connection closes in the background, ending any
	// lock the server granted it before it saw the cancellation.
	r, ok, err := other.Lock(ctx, []string{loc}, false)
	for deadline := time.Now().Add(5 * time.Second); err == nil && !ok && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		r, ok, err = other.Lock(ctx, []string{loc}, false)
	}
	if !ok || err != nil {
		t.Fatalf("lock after release: %v %v", ok, err)
	} else {
		r()
	}
}

func TestMemoryStore(t *testing.T) {
	s := clone.NewMemoryStore()
	testStore(t, s, s)
}

func TestPostgresStore(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := clone.OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A second replica: its own pool, the same table and locks.
	other, err := clone.OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	testStore(t, s, other)
}
