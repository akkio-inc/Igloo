package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// TestRefusedRecheckStaysRefused checks that a prepared statement that its
// recheck refuses is refused every time after, not run as before.
func TestRefusedRecheckStaysRefused(t *testing.T) {
	ctx := context.Background()
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, `CREATE MASKING POLICY hash_device AS (val VARCHAR) RETURNS VARCHAR -> 'h' || md5(val)`)
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.contacts ALTER COLUMN device_id SET MASKING POLICY hash_device")
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	const q = "SELECT device_id FROM lake.crm.contacts WHERE id = $1"
	var d string
	if err := walt.QueryRow(ctx, q, 1).Scan(&d); err != nil || d == "dev-1" || !strings.HasPrefix(d, "h") {
		t.Fatalf("masked: %q %v", d, err)
	}
	mustExec(t, walt, "CREATE TEMP MACRO md5(x) AS x")
	for i := 0; i < 3; i++ {
		if err := walt.QueryRow(ctx, q, 1).Scan(&d); pgCode(err) != "42501" {
			t.Errorf("run %d after a macro replaced md5: %q %v", i, d, err)
		}
	}
}
