package pgwire

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/policy"
)

var errMetadataDown = errors.New("the metadata database cannot be reached")

// flakyUsers is a user store whose reads fail while down is set.
type flakyUsers struct {
	*testStore
	down atomic.Bool
}

func (s *flakyUsers) Get(ctx context.Context, name string) (auth.User, error) {
	if s.down.Load() {
		return auth.User{}, errMetadataDown
	}
	return s.testStore.Get(ctx, name)
}

// flakyPolicies is a policy store whose reads fail while down is set.
type flakyPolicies struct {
	policy.Store
	down atomic.Bool
}

func (s *flakyPolicies) Version(ctx context.Context) (int64, error) {
	if s.down.Load() {
		return 0, errMetadataDown
	}
	return s.Store.Version(ctx)
}

func (s *flakyPolicies) Load(ctx context.Context) (*policy.Snapshot, error) {
	if s.down.Load() {
		return nil, errMetadataDown
	}
	return s.Store.Load(ctx)
}

// flakyGrants is a grant store whose reads fail while down is set.
type flakyGrants struct {
	auth.GrantStore
	down atomic.Bool
}

func (s *flakyGrants) UserPrivileges(ctx context.Context, user string) (auth.Privileges, error) {
	if s.down.Load() {
		return auth.Privileges{}, errMetadataDown
	}
	return s.GrantStore.UserPrivileges(ctx, user)
}

// The servers of these tests read users, grants and policies again every
// staleRefresh, and refuse statements once they could not for staleMax.
const (
	staleRefresh = 50 * time.Millisecond
	staleMax     = 2 * time.Second
)

// stillFailsClosed checks that conns' statements succeed through a blip
// of the metadata database, fail with SQLSTATE 58000 once it has been down
// for longer than staleMax, and succeed again once it is back.
func stillFailsClosed(t *testing.T, conns map[string]*pgx.Conn, down *atomic.Bool) {
	t.Helper()
	ctx := context.Background()
	for _, conn := range conns {
		mustExec(t, conn, "SELECT 1")
	}
	down.Store(true)
	time.Sleep(2 * staleRefresh)
	for name, conn := range conns {
		if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
			t.Errorf("%s, through a blip: %v", name, err)
		}
	}
	time.Sleep(staleMax)
	for name, conn := range conns {
		if _, err := conn.Exec(ctx, "SELECT 1"); pgCode(err) != "58000" {
			t.Errorf("%s, after the maximum staleness: %v", name, err)
		}
	}
	down.Store(false)
	for name, conn := range conns {
		var err error
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if _, err = conn.Exec(ctx, "SELECT 1"); err == nil {
				break
			}
		}
		if err != nil {
			t.Errorf("%s, once the metadata database is back: %v", name, err)
		}
	}
}

// A session's statements are refused once its user could not be read
// again for longer than the maximum staleness: a user disabled or a token
// revoked on another replica meanwhile used to keep their sessions here
// for as long as the metadata database could not be read.
func TestStaleUserFailsClosed(t *testing.T) {
	t.Parallel()
	var users *flakyUsers
	ts := startServer(t, func(o *Options) {
		users = &flakyUsers{testStore: o.Users.(*testStore)}
		o.Users = users
		o.RoleRefresh = staleRefresh
		o.AccessMaxStaleness = staleMax
	})
	// The extended protocol's statements are checked again as they run.
	stillFailsClosed(t, map[string]*pgx.Conn{
		"simple":   ts.admin(t, pgx.QueryExecModeSimpleProtocol),
		"prepared": ts.admin(t, pgx.QueryExecModeCacheStatement),
	}, &users.down)
}

// So are statements once the policies could not be read for that long,
// as a policy attached on another replica would not apply.
func TestStalePoliciesFailClosed(t *testing.T) {
	t.Parallel()
	policies := &flakyPolicies{Store: policy.NewMemoryStore()}
	ts := startServer(t, func(o *Options) {
		o.Policies = policies
		o.RoleRefresh = staleRefresh
		o.AccessMaxStaleness = staleMax
	})
	stillFailsClosed(t, map[string]*pgx.Conn{
		"simple":   ts.admin(t, pgx.QueryExecModeSimpleProtocol),
		"prepared": ts.admin(t, pgx.QueryExecModeCacheStatement),
	}, &policies.down)
}

// And a session's grants stay in force while they cannot be read again,
// but no longer than the maximum staleness.
func TestStaleGrantsFailClosed(t *testing.T) {
	t.Parallel()
	var grants *flakyGrants
	ts := startServer(t, func(o *Options) {
		grants = &flakyGrants{GrantStore: auth.NewMemoryGrants(o.Users)}
		o.Grants = grants
		o.RoleRefresh = staleRefresh
		o.AccessMaxStaleness = staleMax
	})
	ctx := context.Background()
	user := auth.User{Name: "igloo", Role: auth.RoleNone}
	grants.down.Store(true)
	for _, c := range []struct {
		age   time.Duration
		stale bool
	}{{staleMax / 2, false}, {staleMax, true}} {
		sess := &session{user: user, privs: privilegeSnapshot{privs: &auth.Privileges{}, loaded: time.Now().Add(-c.age)}}
		_, err := ts.srv.privileges(ctx, sess, user)
		if c.stale && pgerror.GetCode(err) != codes.System || !c.stale && err != nil {
			t.Errorf("grants loaded %s ago: %v", c.age, err)
		}
	}
}

// Delta Sharing's check of a table's policies fails the same way: a
// replica that could not read them used to go on serving shared tables
// that a policy attached on another replica meanwhile protects.
func TestStalePoliciesStopSharing(t *testing.T) {
	t.Parallel()
	policies := &flakyPolicies{Store: policy.NewMemoryStore()}
	ts := startServer(t, func(o *Options) {
		o.Policies = policies
		o.RoleRefresh = staleRefresh
		o.AccessMaxStaleness = staleMax
	})
	ctx := context.Background()
	policies.down.Store(true)
	time.Sleep(2 * staleRefresh)
	if names, err := ts.srv.ProtectedBy(ctx, "public", "campaigns"); err != nil || names != nil {
		t.Errorf("through a blip: %q, %v", names, err)
	}
	time.Sleep(staleMax)
	if names, err := ts.srv.ProtectedBy(ctx, "public", "campaigns"); pgerror.GetCode(err) != codes.System {
		t.Errorf("after the maximum staleness: %q, %v", names, err)
	}
	policies.down.Store(false)
	time.Sleep(2 * staleRefresh)
	if names, err := ts.srv.ProtectedBy(ctx, "public", "campaigns"); err != nil || names != nil {
		t.Errorf("once the metadata database is back: %q, %v", names, err)
	}
}
