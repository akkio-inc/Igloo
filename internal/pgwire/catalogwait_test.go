package pgwire

import (
	"testing"
	"time"
)

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
