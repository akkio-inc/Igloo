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
