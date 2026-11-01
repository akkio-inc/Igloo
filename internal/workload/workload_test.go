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

	// AI caps: the most generous of the roles', 0 (none) above all.
	ai := []auth.Setting{
		set(auth.GranteeRole, "analysts", AIMaxCalls, "500"),
		set(auth.GranteeRole, "acme", AIMaxCalls, "2000"),
		set(auth.GranteeRole, "acme", AIMaxCostUSD, "2.5"),
		set(auth.GranteeRole, "zeta", AIMaxCostUSD, "0"),
		set(auth.TargetBuiltin, "reader", AIMaxCalls, "10"),
	}
	got = Resolve(ana, []string{"acme", "analysts"}, ai)
	if got.AIMaxCalls != 2000 || got.AIMaxCostUSD != 2.5 || got.AISet != [2]bool{true, true} || got.AISource[0] != "role acme" {
		t.Errorf("ana's AI caps: %+v", got)
	}
	got = Resolve(ana, []string{"acme", "zeta"}, ai)
	if got.AIMaxCostUSD != 0 || !got.AISet[1] || got.AISource[1] != "role zeta" {
		t.Errorf("a role without a cost cap: %+v", got)
	}
	if got = Resolve(ana, nil, ai); got.AIMaxCalls != 10 || got.AISet[1] {
		t.Errorf("the built-in role's: %+v", got)
	}
	for _, c := range []struct{ name, value, want string }{
		{AIMaxCalls, "100", "100"}, {AIMaxCostUSD, "0.25", "0.25"}, {AIMaxCostUSD, "0", "0"},
	} {
		if got, err := Normalize(c.name, c.value); err != nil || got != c.want {
			t.Errorf("%s = %s: %q %v", c.name, c.value, got, err)
		}
	}
	for _, c := range [][2]string{{AIMaxCalls, "-1"}, {AIMaxCalls, "1.5"}, {AIMaxCostUSD, "-2"}, {AIMaxCostUSD, "lots"}} {
		if _, err := Normalize(c[0], c[1]); err == nil {
			t.Errorf("%s = %s accepted", c[0], c[1])
		}
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

// TestEstimateFromHistory checks reservations learned from what statements
// allocated: a fraction of the latest, by shape; none for a shape not seen,
// one that allocated little, or with estimates off; and that a heavy
// statement's estimate holds it back while a light one, reserving nothing,
// starts at once.
func TestEstimateFromHistory(t *testing.T) {
	const gb = int64(1) << 30
	c := NewController(16 * gb)
	c.Observe("heavy", 40*gb)
	if got := c.Estimate("heavy"); got != 0 {
		t.Errorf("before EstimateFromHistory: %d", got)
	}
	c.EstimateFromHistory(0.5)
	c.Observe("heavy", 40*gb)
	c.Observe("medium", 4*gb)
	c.Observe("light", gb)
	for shape, want := range map[string]int64{"heavy": 20 * gb, "medium": 2 * gb, "light": 0, "unseen": 0, "": 0} {
		if got := c.Estimate(shape); got != want {
			t.Errorf("Estimate(%q) = %d, want %d", shape, got, want)
		}
	}
	c.Observe("heavy", 8*gb)
	if got := c.Estimate("heavy"); got != 4*gb {
		t.Errorf("after a lighter run: %d", got)
	}

	ctx := context.Background()
	c.Observe("heavy", 40*gb)
	running, _, _ := c.Admit(ctx, Limits{Memory: c.Estimate("medium")})
	admitted := make(chan struct{})
	go func() {
		r, _, err := c.Admit(ctx, Limits{Memory: c.Estimate("heavy")})
		if err != nil {
			t.Error(err)
		}
		close(admitted)
		r()
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case <-admitted:
		t.Error("the heavy statement started next to another's reservation")
	default:
	}
	light, _, err := c.Admit(ctx, Limits{Memory: c.Estimate("light")})
	if err != nil {
		t.Fatal(err)
	}
	light()
	running()
	<-admitted

	c.EstimateFromHistory(0)
	if got := c.Estimate("heavy"); got != 0 {
		t.Errorf("with estimates off: %d", got)
	}
}

// TestPeaks checks what statements are seen holding: the most of what
// DuckDB held while each ran, less what the others beside it reserved; none
// for one no sample fell within; FootprintOf, the smaller of that and what
// it allocated; and runs that ended early, which only raise their shape's
// footprint.
func TestPeaks(t *testing.T) {
	const gb = int64(1) << 30
	c := NewController(22 * gb)
	c.LimitReplica(8)
	ctx := context.Background()
	heavy, _, _ := c.Admit(ctx, Limits{Counted: true, Memory: 10 * gb})
	light, _, _ := c.Admit(ctx, Limits{Counted: true})
	c.sample(12 * gb)
	c.sample(15 * gb)
	if peak, ok := light(); !ok || peak != 5*gb {
		t.Errorf("the light statement's peak %d (%v), want 5 GB", peak, ok)
	}
	c.sample(9 * gb)
	if peak, ok := heavy(); !ok || peak != 15*gb {
		t.Errorf("the heavy statement's peak %d (%v), want 15 GB", peak, ok)
	}
	short, _, _ := c.Admit(ctx, Limits{Counted: true})
	if _, ok := short(); ok {
		t.Error("a statement no sample fell within has a peak")
	}

	for _, f := range []struct {
		allocated, peak int64
		sampled         bool
		want            int64
	}{{84 * gb, 12 * gb, true, 12 * gb}, {gb, 12 * gb, true, gb}, {3 * gb, 0, false, 3 * gb}, {3 * gb, -2 * gb, true, 0}} {
		if got := FootprintOf(f.allocated, f.peak, f.sampled); got != f.want {
			t.Errorf("FootprintOf(%d, %d, %v) = %d, want %d", f.allocated, f.peak, f.sampled, got, f.want)
		}
	}
	c.Observe("s", 8*gb)
	c.ObserveUnfinished("s", 5*gb)
	if got, _ := c.Footprint("s"); got != 8*gb {
		t.Errorf("a run that held less before its end lowered the footprint to %d", got)
	}
	// What a completed run held proves the shape fits; a run cut short
	// that held more proves nothing (see Completed).
	if got, ok := c.Completed("s"); !ok || got != 8*gb {
		t.Errorf("completed, then cut short holding less: %d (%v)", got, ok)
	}
	c.ObserveUnfinished("s", 10*gb)
	c.ObserveUnfinished("new", 3*gb)
	if got, _ := c.Footprint("s"); got != 10*gb {
		t.Errorf("a run that held more before its end left the footprint at %d", got)
	}
	if got, ok := c.Footprint("new"); !ok || got != 3*gb {
		t.Errorf("an unfinished run of a new shape: %d (%v)", got, ok)
	}
	for _, shape := range []string{"s", "new", "never"} {
		if got, ok := c.Completed(shape); ok {
			t.Errorf("%s, latest cut short or never run: completed at %d", shape, got)
		}
	}
	c.Observe("s", 0)
	if got, ok := c.Footprint("s"); !ok || got != 0 {
		t.Errorf("a run that held nothing: %d (%v)", got, ok)
	}
	if got, ok := c.Completed("s"); !ok || got != 0 {
		t.Errorf("a completed run that held nothing: %d (%v)", got, ok)
	}

	// SampleMemory reads until its context ends.
	sctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		c.SampleMemory(sctx, time.Millisecond, func(context.Context) (int64, error) { return 3 * gb, nil })
		close(done)
	}()
	running, _, _ := c.Admit(ctx, Limits{Counted: true})
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		c.mu.Lock()
		sampled := false
		for w := range c.active {
			sampled = w.sampled
		}
		c.mu.Unlock()
		if sampled {
			break
		}
	}
	if peak, ok := running(); !ok || peak != 3*gb {
		t.Errorf("sampled peak %d (%v), want 3 GB", peak, ok)
	}
	stop()
	<-done
}
