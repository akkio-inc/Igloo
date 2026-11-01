package sso

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/testdb"
)

// pausedMappings is single sign-on's store. Once armed, its next read of
// the group mappings, which applying a user's roles makes after reading
// their groups, closes reading and waits until resume is closed.
type pausedMappings struct {
	*auth.PostgresStore
	armed   atomic.Bool
	reading chan struct{}
	resume  chan struct{}
}

func (p *pausedMappings) GroupRoles(ctx context.Context) ([]auth.GroupRole, error) {
	if p.armed.CompareAndSwap(true, false) {
		close(p.reading)
		<-p.resume
	}
	return p.PostgresStore.GroupRoles(ctx)
}

// TestRolesFollowTheLastGroupChange applies a user's roles on two replicas
// at once, as when SCIM changes her groups and two replicas apply the
// changes: one read her groups, then she left a group and the other
// applied that, and then the first wrote the roles it had worked out from
// her groups as they were before, so she kept the role of the group she
// had left. Now the second waits for the first, and works her roles out
// from her groups as they are once the first is done.
func TestRolesFollowTheLastGroupChange(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	open := func() *auth.PostgresStore {
		s, err := auth.OpenPostgresStore(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	a, b := open(), open()
	n := time.Now().UnixNano()
	user, role, group := fmt.Sprintf("ana_%d", n), fmt.Sprintf("analysts_%d", n), fmt.Sprintf("Analysts %d", n)
	if _, err := a.EnsureAdmin(ctx, "root", "v"); err != nil {
		t.Fatal(err)
	}
	if err := a.Create(ctx, auth.User{Name: user, Role: auth.RoleReader, Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if err := a.PutSCIMUser(ctx, auth.SCIMUser{User: user, UserName: user + "@agency.com"}); err != nil {
		t.Fatal(err)
	}
	if err := a.CreateRole(ctx, role); err != nil {
		t.Fatal(err)
	}
	if err := a.GrantGroupRole(ctx, auth.GroupRole{Group: group, Role: role}); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("g-%d", n)
	if err := a.CreateSCIMGroup(ctx, auth.SCIMGroup{ID: id, DisplayName: group, Members: []string{user}}); err != nil {
		t.Fatal(err)
	}
	replica := func(links auth.SSOStore, users *auth.PostgresStore) *Service {
		svc, err := New(Options{Config: config.OIDC{Issuer: "https://idp.example.com", ClientID: "igloo"},
			Users: users, Accounts: users, SSO: links, SCIM: users})
		if err != nil {
			t.Fatal(err)
		}
		return svc
	}
	paused := &pausedMappings{PostgresStore: a, reading: make(chan struct{}), resume: make(chan struct{})}
	first, second := replica(paused, a), replica(b, b)
	member := func() bool {
		t.Helper()
		roles, err := b.Roles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(roles, func(r auth.CustomRole) bool { return r.Name == role })
		return i >= 0 && slices.Contains(roles[i].Members, user)
	}
	if err := second.RefreshRoles(ctx, user); err != nil || !member() {
		t.Fatalf("she is in the group, but not given its role: %v", err)
	}

	// The first replica reads her groups, and then waits.
	paused.armed.Store(true)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RefreshRoles(ctx, user) }()
	<-paused.reading
	// She leaves the group, and the second replica applies that.
	if err := b.ModifySCIMGroup(ctx, id, func(g *auth.SCIMGroup) error { g.Members = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RefreshRoles(ctx, user) }()
	select {
	case err := <-secondDone:
		secondDone <- err
	case <-time.After(500 * time.Millisecond):
		// Waiting for the first.
	}
	close(paused.resume)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if member() {
		t.Error("she left the group, and kept its role")
	}
}
