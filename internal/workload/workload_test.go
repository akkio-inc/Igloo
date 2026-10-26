package workload

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestNormalize(t *testing.T) {
	ok := map[[2]string]string{
		{StatementTimeout, "300000"}:       "5min",
		{StatementTimeout, "'90s'"}:        "90s",
		{StatementTimeout, "0"}:            "0",
		{MaxConcurrentStatements, " 4 "}:   "4",
		{MemoryLimit, "8GB"}:               "8GB",
		{QueryTag, "client:acme;q:4"}:      "client:acme;q:4",
		{QueryTag, ""}:                     "",
		{MemoryLimit, "512 MiB"}:           "512 MiB",
		{StatementTimeout, "2h"}:           "2h",
		{MaxConcurrentStatements, "10000"}: "10000",
	}
	for in, want := range ok {
		if got, err := Normalize(in[0], in[1]); err != nil || got != want {
			t.Errorf("Normalize(%q, %q) = %q, %v; want %q", in[0], in[1], got, err, want)
		}
	}
	for _, in := range [][2]string{
		{StatementTimeout, "soon"}, {MaxConcurrentStatements, "0"}, {MaxConcurrentStatements, "1.5"},
		{MaxConcurrentStatements, "10001"}, {MemoryLimit, "0"}, {MemoryLimit, "lots"},
		{QueryTag, strings.Repeat("x", MaxQueryTag+1)}, {"work_mem", "4MB"},
	} {
		if _, err := Normalize(in[0], in[1]); err == nil {
			t.Errorf("Normalize(%q, %q) should fail", in[0], in[1])
		}
	}
}

func TestResolve(t *testing.T) {
	set := func(targetType, target, name, value string) auth.Setting {
		return auth.Setting{TargetType: targetType, Target: target, Name: name, Value: value}
	}
	settings := []auth.Setting{
		set(auth.TargetBuiltin, "reader", MaxConcurrentStatements, "8"),
		set(auth.TargetBuiltin, "reader", StatementTimeout, "1h"),
		set(auth.TargetBuiltin, "reader", QueryTag, "team:readers"),
		set(auth.GranteeRole, "analysts", StatementTimeout, "10min"),
		set(auth.GranteeRole, "analysts", MaxConcurrentStatements, "2"),
		set(auth.GranteeRole, "analysts", MemoryLimit, "8GB"),
		set(auth.GranteeRole, "acme", StatementTimeout, "30min"),
		set(auth.GranteeRole, "acme", MaxConcurrentStatements, "4"),
		set(auth.GranteeRole, "acme", MemoryLimit, "4GB"),
		set(auth.GranteeRole, "acme", QueryTag, "client:acme"),
		set(auth.GranteeRole, "zeta", QueryTag, "client:zeta"),
		set(auth.GranteeUser, "ana", QueryTag, "client:acme;user:ana"),
		set(auth.GranteeUser, "bo", StatementTimeout, "0"),
	}
	ana := auth.User{Name: "ana", Role: auth.RoleReader}
	// The user's own tag; the most generous of their roles' limits.
	got := Resolve(ana, []string{"acme", "analysts", "zeta"}, settings)
	want := Limits{QueryTag: "client:acme;user:ana", StatementTimeout: 30 * time.Minute,
		Slots: Slots{Key: "role acme", Max: 4}, Memory: 4_000_000_000}
	if got != want {
		t.Errorf("ana: got %+v\nwant %+v", got, want)
	}
	// Roles before the built-in role; the first role's tag by name.
	got = Resolve(auth.User{Name: "cy", Role: auth.RoleReader}, []string{"zeta", "analysts"}, settings)
	want = Limits{QueryTag: "client:zeta", StatementTimeout: 10 * time.Minute, Slots: Slots{Key: "role analysts", Max: 2}, Memory: 8_000_000_000}
	if got != want {
		t.Errorf("cy: got %+v\nwant %+v", got, want)
	}
	// Only the built-in role; a user's 0 lifts the timeout.
	got = Resolve(auth.User{Name: "bo", Role: auth.RoleReader}, nil, settings)
	want = Limits{QueryTag: "team:readers", Slots: Slots{Key: "built-in role reader", Max: 8}}
	if got != want {
		t.Errorf("bo: got %+v\nwant %+v", got, want)
	}
	if got := Resolve(auth.User{Name: "wu", Role: auth.RoleWriter}, nil, settings); got.Limited() || got != (Limits{}) {
		t.Errorf("a writer with no settings: %+v", got)
	}
}

func TestControllerLimitsSlots(t *testing.T) {
	c := NewController(0)
	l := Limits{Slots: Slots{Key: "role analysts", Max: 2}}
	ctx := context.Background()
	r1, _, err := c.Admit(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	r2, _, _ := c.Admit(ctx, l)

	// A third waits until one of the two ends.
	started := make(chan time.Duration)
	go func() {
		r3, waited, err := c.Admit(ctx, l)
		if err != nil {
			t.Error(err)
		}
		started <- waited
		r3()
	}()
	select {
	case <-started:
		t.Fatal("a third statement started while two ran")
	case <-time.After(50 * time.Millisecond):
	}
	// Other limits are not held up.
	if r, _, err := c.Admit(ctx, Limits{Slots: Slots{Key: "role acme", Max: 1}}); err != nil {
		t.Fatal(err)
	} else {
		r()
	}
	r1()
	r1() // releasing twice frees one slot
	if waited := <-started; waited < 50*time.Millisecond {
		t.Errorf("waited %v", waited)
	}
	r2()
	if n := c.Running("role analysts"); n != 0 {
		t.Errorf("%d still running", n)
	}
}

func TestControllerGivesUpWithTheContext(t *testing.T) {
	c := NewController(0)
	l := Limits{Slots: Slots{Key: "user ana", Max: 1}}
	r, _, _ := c.Admit(context.Background(), l)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := c.Admit(ctx, l); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	r()
	// The statement that gave up holds nothing.
	r, _, err := c.Admit(context.Background(), l)
	if err != nil {
		t.Fatal(err)
	}
	r()
}

func TestControllerMemoryInArrivalOrder(t *testing.T) {
	c := NewController(10)
	ctx := context.Background()
	small := Limits{Memory: 3}
	big := Limits{Memory: 8}
	r1, _, _ := c.Admit(ctx, small) // 3 of 10
	r2, _, _ := c.Admit(ctx, small) // 6 of 10

	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	start := func(name string, l Limits) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _, err := c.Admit(ctx, l)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			r()
		}()
		time.Sleep(20 * time.Millisecond)
	}
	start("big", big)     // needs 8: waits for both
	start("small", small) // would fit (9 of 10), but may not overtake big
	mu.Lock()
	if len(order) != 0 {
		t.Errorf("started too early: %v", order)
	}
	mu.Unlock()
	r1()
	r2()
	wg.Wait()
	if strings.Join(order, ",") != "big,small" {
		t.Errorf("order %v", order)
	}

	// A statement waiting only for its role's slots does not hold back
	// another role's reservation.
	analysts := Limits{Slots: Slots{Key: "role analysts", Max: 1}, Memory: 3}
	r1, _, _ = c.Admit(ctx, analysts)
	queued := make(chan struct{})
	go func() {
		r, _, err := c.Admit(ctx, analysts)
		if err != nil {
			t.Error(err)
		}
		close(queued)
		r()
	}()
	time.Sleep(20 * time.Millisecond)
	ctx1, cancel1 := context.WithTimeout(ctx, time.Second)
	defer cancel1()
	r2, waited, err := c.Admit(ctx1, Limits{Slots: Slots{Key: "role acme", Max: 1}, Memory: 3})
	if err != nil || waited != 0 {
		t.Fatalf("another role waited %v: %v", waited, err)
	}
	r2()
	r1()
	<-queued

	// A reservation larger than the replica's memory runs alone, rather
	// than never.
	r, _, err := c.Admit(ctx, Limits{Memory: 64})
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, _, err := c.Admit(ctx2, small); err == nil {
		t.Error("a reservation started next to one that takes all the memory")
	}
	r()
}

func TestControllerWithoutLimits(t *testing.T) {
	var c *Controller
	r, waited, err := c.Admit(context.Background(), Limits{Slots: Slots{Key: "x", Max: 1}})
	if err != nil || waited != 0 {
		t.Fatal(err)
	}
	r()
	// Without a known memory budget, reservations are not enforced.
	c = NewController(0)
	for range 3 {
		if _, _, err := c.Admit(context.Background(), Limits{Memory: 1 << 40}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestReplicaLimit checks the replica's limit of counted statements: they
// wait in order behind it, statements that are not counted never do, and
// one that waits for its role's slots alone does not hold the others up.
func TestReplicaLimit(t *testing.T) {
	c := NewController(0)
	c.LimitReplica(1)
	ctx := context.Background()
	counted := Limits{Counted: true}
	first, _, err := c.Admit(ctx, counted)
	if err != nil {
		t.Fatal(err)
	}
	// Not counted: starts at once.
	other, _, err := c.Admit(ctx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	other()
	// Counted: waits, in order.
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, _, err := c.Admit(ctx, counted)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			release()
		}()
		// Each joins the line before the next.
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			c.mu.Lock()
			n := len(c.queue)
			c.mu.Unlock()
			if n == i+1 {
				break
			}
		}
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, waited, err := c.Admit(short, counted); !errors.Is(err, context.DeadlineExceeded) || waited < 20*time.Millisecond {
		t.Errorf("a counted statement behind the limit: %v after %s", err, waited)
	}
	first()
	wg.Wait()
	if len(order) != 3 || order[0] != 0 || order[1] != 1 || order[2] != 2 {
		t.Errorf("admitted in order %v", order)
	}
	// Without a limit, counted statements start at once.
	free := NewController(0)
	release, waited, err := free.Admit(ctx, counted)
	if err != nil || waited != 0 {
		t.Errorf("without a limit: %v, waited %s", err, waited)
	}
	release()
}
