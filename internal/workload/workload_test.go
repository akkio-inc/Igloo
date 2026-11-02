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
	// The median of its latest runs: one lighter run changes nothing, most do.
	c.Observe("heavy", 8*gb)
	if got := c.Estimate("heavy"); got != 20*gb {
		t.Errorf("after a lighter run: %d", got)
	}
	c.Observe("heavy", 8*gb)
	if got := c.Estimate("heavy"); got != 4*gb {
		t.Errorf("after lighter runs: %d", got)
	}

	ctx := context.Background()
	for range footprintRuns {
		c.Observe("heavy", 40*gb)
	}
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

// TestPeaks checks what statements are seen holding: the most of their share
// of what DuckDB held while each ran, by what they reserved; none for one no
// sample fell within; FootprintOf, two thirds of what it allocated or what it
// was seen holding if more; and runs that ended early, which only raise their
// shape's footprint.
func TestPeaks(t *testing.T) {
	const gb = int64(1) << 30
	c := NewController(22 * gb)
	c.LimitReplica(8)
	ctx := context.Background()
	heavy, _, _ := c.Admit(ctx, Limits{Counted: true, Memory: 16 * gb})
	light, _, _ := c.Admit(ctx, Limits{Counted: true})
	c.sample(12 * gb)
	// 22 GB over 16: a share of 16 and of 1.375 GB.
	held := 15 * gb
	c.sample(held)
	smallest := 22 * gb / minReservationShare
	want := int64(float64(held) * float64(smallest) / float64(16*gb+smallest))
	if peak, ok := light(); !ok || peak != want {
		t.Errorf("the light statement's peak %d (%v), want %d", peak, ok, want)
	}
	// Alone, a statement is charged with all of it.
	c.sample(20 * gb)
	if peak, ok := heavy(); !ok || peak != 20*gb {
		t.Errorf("the heavy statement's peak %d (%v), want 20 GB", peak, ok)
	}
	short, _, _ := c.Admit(ctx, Limits{Counted: true})
	if _, ok := short(); ok {
		t.Error("a statement no sample fell within has a peak")
	}

	for _, f := range []struct {
		allocated, peak int64
		sampled         bool
		want            int64
	}{{84 * gb, 12 * gb, true, 56 * gb}, {3 * gb, 12 * gb, true, 3 * gb}, {3 * gb, 0, false, 2 * gb}, {3 * gb, -2 * gb, true, 2 * gb},
		{12 * gb, 10 * gb, true, 10 * gb}} {
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

// TestFlattenForms checks the form each statement shape's FLATTENs take:
// the estimate's for the first statement, asked for once; the other for
// the next; either while neither has ended; and from then on the one whose
// latest runs took the less CPU time at best.
func TestFlattenForms(t *testing.T) {
	f := NewController(0)
	asked := 0
	estimate := func() bool { asked++; return true }
	for i, want := range []bool{true, false, true} {
		if got := f.ChooseForm("a", estimate); got != want {
			t.Errorf("statement %d: lists %v, want %v", i, got, want)
		}
	}
	if asked != 1 {
		t.Errorf("asked DuckDB %d times", asked)
	}
	f.ObserveForm("a", false, 2*time.Second)
	if f.ChooseForm("a", estimate) {
		t.Error("with only the lateral form's run ended, not the lateral form")
	}
	f.ObserveForm("a", true, 5*time.Second)
	if f.ChooseForm("a", estimate) {
		t.Error("the slower form")
	}
	f.ObserveForm("a", true, time.Second)
	if !f.ChooseForm("a", estimate) {
		t.Error("not the form that became faster")
	}
	// A run beside others takes longer: it changes nothing until the faster
	// run is no longer among the latest.
	f.ObserveForm("a", true, 5*time.Second)
	if !f.ChooseForm("a", estimate) {
		t.Error("a slower run of the faster form moved the shape off it")
	}
	for range formRuns {
		f.ObserveForm("a", true, 5*time.Second)
	}
	if f.ChooseForm("a", estimate) {
		t.Error("the form that became slower")
	}
	f.ObserveForm("unseen", true, time.Second)
	if f.ChooseForm("b", func() bool { return false }) || !f.ChooseForm("b", estimate) {
		t.Error("another shape: not the estimate's, then the other form")
	}
}

// TestFormsFromTheHistory checks what a restart keeps of a shape's forms: both
// seen, the faster; one seen, the other next, and then the faster.
func TestFormsFromTheHistory(t *testing.T) {
	never := func() bool { t.Error("asked DuckDB of a shape in the history"); return false }
	f := NewController(0)
	f.ObserveForm("both", false, 5*time.Second)
	f.ObserveForm("both", true, 2*time.Second)
	if !f.ChooseForm("both", never) {
		t.Error("with both seen, not the faster")
	}
	f.ObserveForm("one", true, 2*time.Second)
	if f.ChooseForm("one", never) {
		t.Error("with one seen, not the other next")
	}
	f.ObserveForm("one", false, 5*time.Second)
	if !f.ChooseForm("one", never) {
		t.Error("with both seen by then, not the faster")
	}
	var none *Controller
	none.ObserveForm("x", true, time.Second)
}

// TestControllerLineByExpected checks that statements wait in the order they
// would end if each ran alone from when it came: a short one goes before a
// long one that came just before it, a long one before a short one that comes
// after it would have ended, and those with no history in arrival order.
func TestControllerLineByExpected(t *testing.T) {
	c := NewController(0)
	c.LimitReplica(1)
	ctx := context.Background()
	first, _, err := c.Admit(ctx, Limits{Counted: true})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	start := func(name string, expect time.Duration) {
		before := func() int {
			c.mu.Lock()
			defer c.mu.Unlock()
			return len(c.queue)
		}
		n := before()
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, _, err := c.Admit(ctx, Limits{Counted: true, Expect: expect})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			release()
		}()
		for deadline := time.Now().Add(5 * time.Second); before() == n && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		}
	}
	start("long", time.Hour)
	start("short", time.Second)
	start("unknown", 0)
	start("unknown too", 0)
	start("medium", time.Minute)
	first()
	wg.Wait()
	if got := strings.Join(order, ","); got != "unknown,unknown too,short,medium,long" {
		t.Errorf("order %s", got)
	}

	// A statement that comes long after a long one's start does not pass it:
	// the long one is due first.
	c = NewController(0)
	c.LimitReplica(1)
	first, _, _ = c.Admit(ctx, Limits{Counted: true})
	order = nil
	start("long", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	start("short", time.Millisecond)
	first()
	wg.Wait()
	if got := strings.Join(order, ","); got != "long,short" {
		t.Errorf("order %s", got)
	}
}

// TestCosts checks what the controller learns of what statements cost: the
// least of a shape's latest runs, over the cores, and which are light.
func TestCosts(t *testing.T) {
	c := NewController(0)
	if c.Expect("a") != 0 || c.Light("a") {
		t.Error("costs before WeighCosts")
	}
	c.ObserveCost("a", time.Second)
	if c.Expect("a") != 0 || c.Light("a") {
		t.Error("costs without WeighCosts")
	}
	c.WeighCosts(8, 8*time.Second)
	if c.Expect("b") != 0 || c.Light("b") {
		t.Error("a shape not seen has a cost")
	}
	c.ObserveCost("b", 160*time.Second)
	if c.Expect("b") != 20*time.Second || c.Light("b") {
		t.Errorf("b: expected %s, light %v", c.Expect("b"), c.Light("b"))
	}
	// A run beside others takes more than it would alone: the least counts.
	c.ObserveCost("b", 4*time.Second)
	c.ObserveCost("b", 160*time.Second)
	if c.Expect("b") != 500*time.Millisecond || !c.Light("b") {
		t.Errorf("b: expected %s, light %v", c.Expect("b"), c.Light("b"))
	}
	// Until it is no longer among the latest.
	for range costRuns {
		c.ObserveCost("b", 160*time.Second)
	}
	if c.Expect("b") != 20*time.Second || c.Light("b") {
		t.Errorf("b later: expected %s, light %v", c.Expect("b"), c.Light("b"))
	}
	c.WeighCosts(8, 0)
	c.ObserveCost("c", 0)
	if c.Light("c") {
		t.Error("light with no threshold")
	}
}

// TestUnseenWaitsWhileMemoryIsFull checks that a statement nothing is known of
// waits while DuckDB holds nearly all the memory for those running, and that
// one known to hold little does not.
func TestUnseenWaitsWhileMemoryIsFull(t *testing.T) {
	const gb = int64(1) << 30
	c := NewController(8 * gb)
	c.LimitReplica(8)
	ctx := context.Background()
	// Nothing runs yet: however much DuckDB held, an unseen one starts.
	c.sample(7 * gb)
	first, waited, err := c.Admit(ctx, Limits{Counted: true, Unseen: true})
	if err != nil || waited != 0 {
		t.Fatalf("with nothing running: waited %s: %v", waited, err)
	}
	// Below the mark, another starts too.
	c.sample(gb)
	second, waited, err := c.Admit(ctx, Limits{Counted: true, Unseen: true})
	if err != nil || waited != 0 {
		t.Fatalf("below the mark: waited %s: %v", waited, err)
	}
	second()
	c.sample(7 * gb)
	// A known one starts whatever is held.
	known, waited, err := c.Admit(ctx, Limits{Counted: true})
	if err != nil || waited != 0 {
		t.Fatalf("a known statement waited %s: %v", waited, err)
	}
	known()
	started := make(chan struct{})
	go func() {
		r, _, err := c.Admit(ctx, Limits{Counted: true, Unseen: true})
		if err != nil {
			t.Error(err)
		}
		close(started)
		r()
	}()
	select {
	case <-started:
		t.Fatal("an unseen statement started with the memory full")
	case <-time.After(50 * time.Millisecond):
	}
	// The next sample that finds room lets it start.
	c.sample(2 * gb)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("an unseen statement did not start once there was room")
	}
	first()
	// Without a replica's limit, it waits all the same.
	free := NewController(8 * gb)
	r, _, _ := free.Admit(ctx, Limits{Memory: gb})
	free.sample(7 * gb)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, _, err := free.Admit(short, Limits{Counted: true, Unseen: true}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("without a limit, with the memory full: %v", err)
	}
	r()
}

// TestHeldBehindSecond checks when statements wait for what the second DuckDB's
// holds: with one running there, none in the first, and some in line.
func TestHeldBehindSecond(t *testing.T) {
	const gb = int64(1) << 30
	c := NewController(16 * gb)
	ctx := context.Background()
	if c.HeldBehindSecond() {
		t.Error("with nothing running")
	}
	report, _, err := c.Admit(ctx, Limits{Apart: true, Memory: 16 * gb})
	if err != nil {
		t.Fatal(err)
	}
	if c.HeldBehindSecond() {
		t.Error("with nothing in line")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, _, err := c.Admit(ctx, Limits{Memory: 4 * gb})
		if err != nil {
			t.Error(err)
			return
		}
		r()
	}()
	for deadline := time.Now().Add(5 * time.Second); !c.HeldBehindSecond() && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
	}
	if !c.HeldBehindSecond() {
		t.Error("with a statement in line behind the report")
	}
	report()
	<-done
	if c.HeldBehindSecond() {
		t.Error("after the report")
	}
	// One running in the first keeps its caches.
	first, _, _ := c.Admit(ctx, Limits{Memory: gb})
	report, _, _ = c.Admit(ctx, Limits{Apart: true, Memory: gb})
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	go func() {
		if r, _, err := c.Admit(short, Limits{Memory: 16 * gb}); err == nil {
			r()
		}
	}()
	time.Sleep(20 * time.Millisecond)
	if c.HeldBehindSecond() {
		t.Error("with one running in the first")
	}
	first()
	report()
	var none *Controller
	if none.HeldBehindSecond() {
		t.Error("nil controller")
	}
}
