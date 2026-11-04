package workload

import (
	"slices"
	"sync"
	"time"
)

// Reservations from statements' history. A replica's statements share one
// memory pool, so several heavy ones at once spill to disk, and on a disk
// that is busy enough they take many times as long as one after the other:
// a dashboard's burst of queries over billions of rows took 548 s with 8
// running at once on 8 cores and 22 GB, against 365 s with 2. A fixed limit
// holds back the light statements with the heavy ones, though. The
// controller instead learns what each statement held at once (see Observe),
// by shape, the text with its literals masked, which a dashboard sends
// again and again with other audiences or dates, and gives the next
// statement of that shape a reservation of a fraction of it (see Estimate):
// a statement that held more than the replica's memory waits until the
// others' are released, while one that held little reserves nothing and
// starts at once.
//
// DuckDB 1.5 measures what a statement allocated over its run, which counts
// memory freed and allocated again each time, and the process's peak, not
// a statement's. Alone, a statement holds at its most between two fifths and
// all of what it allocated, two thirds on most shapes of a dashboard, and that
// holds whatever runs beside it, unlike anything sampled: what DuckDB holds
// for all the statements running cannot be told apart, and less what the
// others reserved it said next to nothing for a statement beside one that
// reserved much and held little yet. Four in ten of a large dashboard's
// statements learned under a twentieth of what they allocated, started beside
// the heavy ones next time and spilled, and the next run's footprints and
// times differed from the last's by a fifth. So a run's footprint is
// heldShare of what it allocated, or what it was seen holding if more (see
// FootprintOf); and a shape's is the median of its latest runs, since DuckDB
// counts some of what other statements allocate on the same threads, up to
// twenty times a small statement's own. It learns from runs that did not end
// too (see ObserveUnfinished): heavy statements that never ended, cancelled at
// their timeout, were never learned and started together, six or seven at
// once, until all spilled.

// heldShare is the share of what a run allocated that it is taken to hold at
// its most.
const heldShare = 2.0 / 3

// footprintRuns is how many of a shape's latest runs its footprint looks at.
const footprintRuns = 8

// LongUnfinished is how long a run that ended early must have run to be taken
// for one that needed all the memory it ran with.
const LongUnfinished = 10 * time.Second

// minReservationShare is the smallest reservation, as a share of the
// budget, that an estimate makes; a smaller one is none.
const minReservationShare = 16

// maxFootprints bounds the shapes kept; they are forgotten all at once.
const maxFootprints = 1 << 16

// footprints are what statements of each shape held at once, as of their
// latest runs.
type footprints struct {
	mu       sync.Mutex
	fraction float64
	held     map[string]*footprint
	// onWorkers says the latest run of each key, of those that have run, ran on
	// workers rather than here.
	onWorkers map[string]bool
}

// footprint is what statements of a shape held at once: what its latest
// completed runs held, and the most a run seen since, which ended before its
// end, held.
type footprint struct {
	runs       []int64
	unfinished int64
}

// localTimes are how long the latest completed runs of each shape took (see
// ObserveTook).
type localTimes struct {
	mu   sync.Mutex
	runs map[string][]time.Duration
}

// held is what statements of the shape hold, and whether a run that ended
// before its end says so: the median of its latest runs, the lower one of two,
// or more, what a run that ended early held.
func (f *footprint) held() (int64, bool) {
	var median int64
	if len(f.runs) > 0 {
		sorted := slices.Sorted(slices.Values(f.runs))
		median = sorted[(len(sorted)-1)/2]
	}
	return max(median, f.unfinished), f.unfinished > median
}

// ReadNothing says that a run read no bytes and took next to no CPU, as a statement
// does when DuckDB plans it to no rows for the values of its literals. That says
// nothing of what statements of its shape, the text with its literals masked, use
// otherwise: a shape whose runs for some values take a minute of all the cores, once
// so learned from a run for a value with no rows, reserved nothing, waited for nothing
// and started all together. So such a run is only ever what a shape has until one
// that read something (see ObserveEmpty).
func ReadNothing(bytesRead int64, cpu time.Duration) bool {
	return bytesRead == 0 && cpu < LightCPU
}

// ObserveEmpty records a run of key that ReadNothing says of: as holding and taking
// nothing, if the key has no other runs, and until it has one. It is for the same text
// with the same literals (see Alike), which plan to no rows again; those alike but for
// their literals may take a minute of all the cores.
func (c *Controller) ObserveEmpty(shape string) {
	if c == nil || shape == "" {
		return
	}
	fp := c.history.of(shape)
	if len(fp.runs) == 0 {
		fp.runs = append(fp.runs, 0)
	}
	c.history.mu.Unlock()
	k := &c.costs
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.runs == nil || len(k.runs) >= maxFootprints {
		k.runs = make(map[string][]time.Duration)
	}
	if len(k.runs[shape]) == 0 {
		k.runs[shape] = []time.Duration{0}
	}
}

// most is the most any of its latest runs held, or one that ended early held.
func (f *footprint) most() int64 {
	m := f.unfinished
	for _, r := range f.runs {
		m = max(m, r)
	}
	return m
}

// pick returns which of a statement's keys to go by (see Alike), and whether by the
// most its runs took, not what they took most of the time: a statement whose text
// with its literals has not run may be any of those alike but for them.
func (c *Controller) pick(exact, shape string) (key string, most bool) {
	if key = c.Alike(exact, shape); key != exact && exact != "" {
		return key, true
	}
	return key, false
}

// Reserve returns what a statement is to reserve (see Estimate) and whether its runs
// say so (seen), by those of the same text with the same literals if there are any,
// and otherwise by the most any run alike but for its literals held. Those alike can
// differ by as much as their literals do: over the empty audience or the value nothing
// has, a dashboard's statements took seconds, and others of them with no runs of
// their own, so estimated, reserved nothing, started together and spilled.
func (c *Controller) Reserve(exact, shape string) (memory int64, seen bool) {
	key, most := c.pick(exact, shape)
	if !most {
		_, seen = c.Footprint(key)
		return c.Estimate(key), seen
	}
	f := &c.history
	f.mu.Lock()
	fp, ok := f.held[key]
	var held int64
	if ok {
		held = fp.most()
	}
	f.mu.Unlock()
	return c.estimate(held), ok
}

// Alike returns which of a statement's two keys to go by, what it reserves and how
// long it takes: exact, for the same text with the same literals, if statements of it
// have run, and otherwise shape, for those alike but for their literals. A dashboard's
// statements over a large audience allocated 3.7 times, at the median, and up to 12
// times, what the same statements over a small one did, so once it had run over a small
// one the statements over a large one reserved next to nothing, started together and
// spilled: a burst of 324 queries took 2,207 s after one over small audiences, and with
// their own runs to go by, 1,708 s.
func (c *Controller) Alike(exact, shape string) string {
	if c == nil || exact == "" {
		return shape
	}
	if _, _, ok := c.footprint(exact); ok {
		return exact
	}
	return shape
}

// setOnWorkers records where key's latest run ran. f.mu is held.
func (f *footprints) setOnWorkers(key string, on bool) {
	if f.onWorkers == nil || len(f.onWorkers) >= maxFootprints {
		f.onWorkers = make(map[string]bool)
	}
	f.onWorkers[key] = on
}

// ObserveOnWorkers records a run of key that ran on workers: it says nothing of what
// statements of it hold here.
func (c *Controller) ObserveOnWorkers(key string) {
	if c == nil || key == "" {
		return
	}
	f := &c.history
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setOnWorkers(key, true)
}

// RunsOnWorkers says whether a statement of these keys is likely to run on workers:
// its latest run with the same literals did, or with none of those, its latest run
// alike. It then has nothing to wait for here: auto mode looks at where a statement
// runs only once it is admitted, and such statements waited in line for memory they
// would not use, ahead of what would.
func (c *Controller) RunsOnWorkers(exact, shape string) bool {
	if c == nil {
		return false
	}
	f := &c.history
	f.mu.Lock()
	defer f.mu.Unlock()
	if on, ok := f.onWorkers[exact]; ok && exact != "" {
		return on
	}
	return f.onWorkers[shape]
}

// EstimateFromHistory makes the controller reserve, for a statement that
// reserves nothing of its own, fraction of what the latest statement of its
// shape held (see Estimate). Zero or less turns that off. It is set
// once, before statements run.
func (c *Controller) EstimateFromHistory(fraction float64) {
	c.history.fraction = max(fraction, 0)
}

// Observe records what a statement of shape held at once on this replica,
// in a run it completed (see FootprintOf); the server seeds them from the
// query history when it starts, as its statements had them under the same
// memory limit.
func (c *Controller) Observe(shape string, footprint int64) {
	if c == nil || shape == "" || footprint < 0 {
		return
	}
	fp := c.history.of(shape)
	defer c.history.mu.Unlock()
	if footprint > 0 {
		// A run that read nothing is only ever all it has (see ObserveEmpty).
		fp.runs = slices.DeleteFunc(fp.runs, func(n int64) bool { return n == 0 })
	}
	c.history.setOnWorkers(shape, false)
	fp.runs = append(fp.runs, footprint)
	if len(fp.runs) > footprintRuns {
		fp.runs = fp.runs[len(fp.runs)-footprintRuns:]
	}
	fp.unfinished = 0
}

// ObserveTook records how long a statement of shape took on this replica in a
// run it completed, past what it waited to start; the server seeds them from
// the query history when it starts. Auto mode's history keeps such times by
// plan key, which a statement has only if it was planned for workers: one run
// here with distributed_execution off, or whose planning gave up, took 714 s
// and left auto mode to go on estimating it at 15 s (see
// distributed.LocalRuns).
func (c *Controller) ObserveTook(shape string, took time.Duration) {
	if c == nil || shape == "" || took <= 0 {
		return
	}
	k := &c.took
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.runs == nil || len(k.runs) >= maxFootprints {
		k.runs = make(map[string][]time.Duration)
	}
	runs := append(k.runs[shape], took)
	if len(runs) > footprintRuns {
		runs = runs[len(runs)-footprintRuns:]
	}
	k.runs[shape] = runs
}

// Took is how long statements of shape take on this replica, the median of
// its latest completed runs, the lower one of two; ok is false when none of
// its shape has completed here.
func (c *Controller) Took(shape string) (took time.Duration, ok bool) {
	if c == nil || shape == "" {
		return 0, false
	}
	k := &c.took
	k.mu.Lock()
	defer k.mu.Unlock()
	runs := k.runs[shape]
	if len(runs) == 0 {
		return 0, false
	}
	sorted := slices.Sorted(slices.Values(runs))
	return sorted[(len(sorted)-1)/2], true
}

// ObserveUnfinished records what a statement of shape was seen holding in a
// run that ended before its end, cancelled or failed: at least that much,
// so it raises what a shape's runs held and never lowers it.
func (c *Controller) ObserveUnfinished(shape string, held int64) {
	if c == nil || shape == "" || held < 0 {
		return
	}
	fp := c.history.of(shape)
	defer c.history.mu.Unlock()
	fp.unfinished = max(fp.unfinished, held)
}

// of returns shape's footprint, a new one if it has none, with f.mu held.
func (f *footprints) of(shape string) *footprint {
	f.mu.Lock()
	if f.held == nil || len(f.held) >= maxFootprints {
		f.held = make(map[string]*footprint)
	}
	fp := f.held[shape]
	if fp == nil {
		fp = &footprint{}
		f.held[shape] = fp
	}
	return fp
}

// FootprintOf is what Observe learns of a run that allocated allocated over
// its whole run and was seen holding what seen says: heldShare of what it
// allocated, or what it was seen holding if more, since a run allocates at
// least what it holds at once; and never more than its DuckDB held in all for
// the statements running in it while it ran, where a sample says so. A run that
// allocates and frees again and again holds far less than two thirds of what it
// allocated: a dashboard's query at ten times its data allocated 82 GB and its
// DuckDB held at most 35 GB while it ran. Reserving 54.5 GB, more than the
// replica's 49 GB, it waited each time for every statement that reserved memory
// to end, and the 8 to 10 that came after it waited until it ended, while one
// statement ran on 16 cores.
func FootprintOf(allocated int64, seen Seen) int64 {
	held := int64(heldShare * float64(allocated))
	if seen.Sampled {
		held = max(held, min(seen.Peak, allocated))
		if seen.Most > 0 {
			held = min(held, seen.Most)
		}
	}
	return max(held, 0)
}

// Estimate returns the memory a statement of shape is to reserve: the
// fraction of what the latest one held, or none for a shape not seen
// or whose reservation would be under a sixteenth of the replica's memory.
// Reservations larger than the memory are capped when the statement is
// admitted (see reservation).
func (c *Controller) Estimate(shape string) int64 {
	if c == nil || shape == "" || c.history.fraction == 0 || c.budget <= 0 {
		return 0
	}
	held, _, _ := c.footprint(shape)
	return c.estimate(held)
}

// estimate is the reservation for what statements held: none under a sixteenth of the
// replica's memory.
func (c *Controller) estimate(held int64) int64 {
	if c == nil || c.history.fraction == 0 || c.budget <= 0 {
		return 0
	}
	r := int64(float64(held) * c.history.fraction)
	if r < c.budget/minReservationShare {
		return 0
	}
	return r
}

// Footprint returns what the latest statement of shape held at once on
// this replica, in a run it completed or, if more, in one that ended
// before its end (see Observe and ObserveUnfinished); ok is false when
// none of its shape has run here.
func (c *Controller) Footprint(shape string) (held int64, ok bool) {
	held, _, ok = c.footprint(shape)
	return held, ok
}

// Completed is Footprint where the run it was learned from completed; ok
// is false otherwise. Auto mode takes it for proof that one machine is not
// short of memory for the statement, whatever its estimate says. A run cut
// short says only that the statement needed at least what it held: a
// report the guard stopped on a replica of 53 GB was learned at all of
// it, from the query history, which auto mode then took for a working set
// that fit, though DuckDB's plan put it at about three times that; it was
// tried on the replica once a day, estimated at 47 s, and spilled 54 GB.
func (c *Controller) Completed(shape string) (held int64, ok bool) {
	held, unfinished, ok := c.footprint(shape)
	if !ok || unfinished {
		return 0, false
	}
	return held, true
}

func (c *Controller) footprint(shape string) (held int64, unfinished, ok bool) {
	if c == nil || shape == "" {
		return 0, false, false
	}
	f := &c.history
	f.mu.Lock()
	defer f.mu.Unlock()
	fp, ok := f.held[shape]
	if !ok {
		return 0, false, false
	}
	held, unfinished = fp.held()
	return held, unfinished, true
}
