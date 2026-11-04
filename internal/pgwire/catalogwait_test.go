package pgwire

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// slowMetadata is a MetadataSource whose first read waits for read to close,
// once it has closed started.
type slowMetadata struct {
	read, started chan struct{}
	once          sync.Once
	md            atomic.Pointer[catalog.Metadata]
}

func (m *slowMetadata) Metadata() *catalog.Metadata { return m.md.Load() }

func (m *slowMetadata) RefreshMetadata(ctx context.Context) error {
	m.once.Do(func() { close(m.started) })
	select {
	case <-m.read:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.md.Store(&catalog.Metadata{})
	return nil
}

func (m *slowMetadata) TableChanged(string, string) {}

// TestFirstMetadataReadLetsGoOfTheCatalog checks that a statement waiting for
// the first read of the catalog's metadata lets go of the catalog lock it holds
// meanwhile, so that DDL, and every statement behind it, need not wait for the
// read; and that it holds the lock again once the read is done.
func TestFirstMetadataReadLetsGoOfTheCatalog(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	cs.srv.sessions.mu.Lock()
	sess := cs.srv.sessions.byPID[int32(cs.admin.PgConn().PID())]
	cs.srv.sessions.mu.Unlock()
	if sess == nil {
		t.Fatal("no session for the admin's connection")
	}
	meta := &slowMetadata{read: make(chan struct{}), started: make(chan struct{})}
	// As a statement being prepared holds it.
	release := cs.srv.shareCatalog(sess)
	done := make(chan error, 1)
	go func() { done <- readMetadata(t.Context(), cs.srv, sess, meta) }()
	<-meta.started
	exclusive := make(chan struct{})
	go func() {
		cs.srv.engine.CatalogLock()()
		close(exclusive)
	}()
	select {
	case <-exclusive:
	case <-time.After(10 * time.Second):
		close(meta.read)
		<-done
		release()
		t.Fatal("DDL waited for the first read of the catalog's metadata")
	}
	close(meta.read)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if sess.catHeld != catalogShared {
		t.Errorf("after the read the session holds the catalog lock as %v, not shared", sess.catHeld)
	}
	release()
}

// TestHistoryRecordsCatalogWait holds the catalog lock exclusively, as a
// listing or DDL in another session would, while a session's DDL waits for
// it, and checks that the history records the wait as that statement's,
// and none for the session's next statement.
func TestHistoryRecordsCatalogWait(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	unlock := cs.srv.engine.CatalogLock()
	go func() {
		time.Sleep(300 * time.Millisecond)
		unlock()
	}()
	mustExec(t, cs.admin, "CREATE SCHEMA lake.waited")
	if q := lastQuery(t, cs, "igloo", "CREATE SCHEMA lake.waited"); q.CatalogWait < 200*time.Millisecond {
		t.Errorf("DDL that waited for the catalog lock: catalog wait %s", q.CatalogWait)
	}
	mustExec(t, cs.admin, "CREATE SCHEMA lake.free")
	if q := lastQuery(t, cs, "igloo", "CREATE SCHEMA lake.free"); q.CatalogWait > 100*time.Millisecond {
		t.Errorf("DDL with the catalog lock free: catalog wait %s", q.CatalogWait)
	}
}

// TestPlanningSeesCatalogWait checks that what auto mode's planning reads of a
// session's waits for the catalog lock (sessionEnv.CatalogWaited) counts the wait
// under way, from another goroutine, and the waits before it once it is over.
func TestPlanningSeesCatalogWait(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	unlock := cs.srv.engine.CatalogLock()
	cs.srv.sessions.mu.Lock()
	sess := cs.srv.sessions.byPID[int32(cs.admin.PgConn().PID())]
	cs.srv.sessions.mu.Unlock()
	if sess == nil {
		t.Fatal("no session for the admin's connection")
	}
	env := &sessionEnv{s: cs.srv, sess: sess}
	before := env.CatalogWaited()
	done := make(chan struct{})
	go func() {
		defer close(done)
		mustExec(t, cs.admin, "CREATE SCHEMA lake.planwait")
	}()
	// From when its statement waits: under a loaded test run, it started to 15
	// ms after the sleep did.
	for deadline := time.Now().Add(10 * time.Second); sess.catWaitFrom.Load() == 0 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	if during := env.CatalogWaited() - before; during < 200*time.Millisecond {
		t.Errorf("while its statement waits for the catalog lock: waited %s", during)
	}
	unlock()
	<-done
	if after := env.CatalogWaited() - before; after < 300*time.Millisecond {
		t.Errorf("once its statement got the catalog lock: waited %s", after)
	}
}
