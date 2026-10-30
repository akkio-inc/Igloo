package tasks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/schedule"
)

const (
	// maxConcurrentRuns bounds the scheduled runs one replica runs at once.
	// Further due jobs wait for a free slot, or for another replica.
	maxConcurrentRuns = 4
	// pollInterval is how often the scheduler looks for work when none is
	// due sooner: jobs created or changed on other replicas show up
	// within it.
	pollInterval = 10 * time.Second
	// pruneInterval is how often old runs are deleted.
	pruneInterval = time.Hour
	// nextDueTTL is how long NextDue's answer is reused.
	nextDueTTL = 5 * time.Second
	// writeTimeout bounds recording a run after it ends, even when
	// Igloo is shutting down.
	writeTimeout = 10 * time.Second
	// heldInterval is how often a run checks that it still holds its
	// job's lock.
	heldInterval = 2 * time.Second
)

// errLockLost is the cause of a run canceled because it lost its lock.
var errLockLost = errors.New("the run lost its lock, as when its connection to the metadata database drops, so another run of the job could start")

// Runner runs a job as its owner, with the owner's privileges at the time.
// It returns the rows the run loaded, wrote or returned, or -1;
// ErrOwnerGone when the owner has been dropped; ErrIdle for a pipe that
// found nothing to load; an error wrapping ErrSkipped for a run it did not
// carry out. RunOf(ctx) is the run.
type Runner interface {
	RunJob(ctx context.Context, j Job) (rows int64, err error)
}

// Options configure a Scheduler.
type Options struct {
	// Retention is how long runs are kept. Zero keeps them.
	Retention time.Duration
	// Ended, if set, is told about every run recorded once it has ended,
	// as for alerts on failed runs. It must not block.
	Ended func(Job, Run)
	Log   *slog.Logger
}

// Scheduler runs jobs when they are due. Every replica runs one; each due
// run is claimed by one replica, and a job never runs twice at once.
type Scheduler struct {
	store     Store
	retention time.Duration
	ended     func(Job, Run)
	log       *slog.Logger
	kick      chan struct{}
	slots     chan struct{}
	running   sync.WaitGroup
	// runs is the context scheduled runs run in, which cancelRuns ends.
	runs       context.Context
	cancelRuns context.CancelFunc

	dueMu   sync.Mutex
	dueAt   time.Time // when due was read
	due     time.Time
	dueErr  error
	hasDue  bool
	nowFunc func() time.Time
	// heldEvery is how often a run checks its lock (heldInterval).
	heldEvery time.Duration
}

// NewScheduler schedules the jobs in store.
func NewScheduler(store Store, opts Options) *Scheduler {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	runs, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		runs:       runs,
		cancelRuns: cancel,
		store:      store,
		retention:  opts.Retention,
		ended:      opts.Ended,
		log:        opts.Log.With("component", "tasks"),
		kick:       make(chan struct{}, 1),
		slots:      make(chan struct{}, maxConcurrentRuns),
		nowFunc:    time.Now,
		heldEvery:  heldInterval,
	}
}

// Store is where jobs and history are kept.
func (s *Scheduler) Store() Store { return s.store }

// Kick makes the scheduler look at the schedule again at once, after a
// job was created or changed on this replica.
func (s *Scheduler) Kick() {
	s.dueMu.Lock()
	s.dueAt = time.Time{}
	s.dueMu.Unlock()
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// NextDue is when the soonest started job is next due; ok is false when
// none is scheduled. The scale-to-zero gateway starts Igloo then.
func (s *Scheduler) NextDue(ctx context.Context) (time.Time, bool, error) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	if time.Since(s.dueAt) < nextDueTTL {
		return s.due, s.hasDue, s.dueErr
	}
	jobs, err := s.store.Scheduled(ctx)
	s.dueAt, s.dueErr, s.hasDue, s.due = time.Now(), err, false, time.Time{}
	if err == nil && len(jobs) > 0 {
		s.due, s.hasDue = jobs[0].NextRun, true
	}
	return s.due, s.hasDue, s.dueErr
}

// Run starts due jobs with r until ctx is canceled. The runs it started
// go on; Wait waits for them.
func (s *Scheduler) Run(ctx context.Context, r Runner) {
	// The first prune comes soon after a start, at a random point, so
	// replicas starting together do not all try at once.
	prune := time.NewTimer(time.Minute + rand.N(time.Minute))
	defer prune.Stop()
	for {
		wait := s.fireDue(ctx, r)
		select {
		case <-ctx.Done():
			return
		case <-s.kick:
		case <-prune.C:
			s.prune(ctx)
			prune.Reset(pruneInterval)
		case <-time.After(wait):
		}
	}
}

// fireDue starts every due job there is a slot for, and returns how long
// to wait before looking again.
func (s *Scheduler) fireDue(ctx context.Context, r Runner) time.Duration {
	jobs, err := s.store.Scheduled(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("could not read the schedule", "err", err)
		}
		return pollInterval
	}
	now := s.nowFunc()
	for _, j := range jobs {
		if j.NextRun.After(now) {
			return min(max(j.NextRun.Sub(now), 50*time.Millisecond), pollInterval)
		}
		select {
		case s.slots <- struct{}{}:
		default:
			// Every slot is busy; look again soon.
			return time.Second
		}
		s.running.Add(1)
		go func() {
			defer s.running.Done()
			defer func() { <-s.slots }()
			s.fire(s.runs, r, j, now)
		}()
	}
	return pollInterval
}

// Wait waits for the scheduled runs under way to end, as at shutdown, and
// cancels them when ctx ends first. Canceled runs are recorded as failed.
func (s *Scheduler) Wait(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.running.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.cancelRuns()
		<-done
	}
}

// fire claims a due run of j and runs it, unless another replica claimed
// it first.
func (s *Scheduler) fire(ctx context.Context, r Runner, j Job, now time.Time) {
	sched, err := schedule.Parse(j.Schedule, j.Timezone)
	next := time.Time{}
	if err == nil {
		next = sched.Next(j.NextRun, now)
	}
	claimed, cerr := s.store.Claim(ctx, j.Kind, j.Name, j.NextRun, next)
	if cerr != nil || !claimed {
		if cerr != nil && ctx.Err() == nil {
			s.log.Warn("could not claim a run", "kind", j.Kind, "name", j.Name, "err", cerr)
		}
		return
	}
	if err != nil {
		// Schedules are checked when they are set, so this means the
		// stored one was changed by hand.
		s.suspend(ctx, j, fmt.Errorf("invalid schedule: %w", err))
		return
	}
	s.Kick()
	if _, err := s.execute(ctx, r, j, TriggerSchedule, j.NextRun); err != nil && !errors.Is(err, ErrIdle) && ctx.Err() == nil {
		s.log.Info("scheduled run failed", "kind", j.Kind, "name", j.Name, "owner", j.Owner, "err", err)
	}
}

// RunNow runs j at once, as EXECUTE TASK and REFRESH MATERIALIZED VIEW
// do, and records the run. ErrRunning means it is running already.
func (s *Scheduler) RunNow(ctx context.Context, r Runner, j Job) (Run, error) {
	return s.execute(ctx, r, j, TriggerManual, time.Time{})
}

// execute runs j unless it is running already, and records the run. A
// scheduled run that finds j running is recorded as skipped.
//
// The job's lock is a session advisory lock, which Postgres releases when
// its connection drops; another replica can then take it and run the job
// while this run goes on. So the run checks that it still holds the lock
// right before it starts, and every heldEvery while it runs, and is
// canceled once it does not.
func (s *Scheduler) execute(ctx context.Context, r Runner, j Job, trigger string, scheduled time.Time) (Run, error) {
	run := Run{ID: audit.NewID(s.nowFunc()), Kind: j.Kind, Name: j.Name, User: j.Owner, Trigger: trigger,
		Scheduled: scheduled, Start: s.nowFunc(), Rows: -1}
	lease, ok, err := s.store.TryLease(ctx, JobKey(j.Kind, j.Name))
	if err != nil {
		return run, fmt.Errorf("lock %s %s: %w", j.Kind, j.Name, err)
	}
	if !ok {
		if trigger != TriggerSchedule {
			return run, fmt.Errorf("%s %s %w", j.Kind, j.Name, ErrRunning)
		}
		run.Outcome, run.Error, run.End = OutcomeSkipped, "the previous run was still running", run.Start
		s.write(ctx, run)
		s.end(j, run)
		return run, nil
	}
	defer lease.Release()
	// Holding the lock, any run still marked running was left by a
	// replica that stopped.
	if err := s.store.AbandonRuns(ctx, j.Kind, j.Name); err != nil {
		s.log.Warn("could not update abandoned runs", "kind", j.Kind, "name", j.Name, "err", err)
	}
	// A pipe's check is recorded only if it loaded something or failed.
	if j.Kind != KindPipe {
		run.Outcome = OutcomeRunning
		s.write(ctx, run)
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if err = lease.Held(ctx); err != nil {
		cancel(fmt.Errorf("%w: %w", errLockLost, err))
	} else {
		stop := s.watch(runCtx, lease, cancel)
		run.Rows, err = r.RunJob(context.WithValue(runCtx, runKey{}, run), j)
		stop()
	}
	if cause := context.Cause(runCtx); errors.Is(cause, errLockLost) && err != nil && !errors.Is(err, ErrIdle) {
		err = fmt.Errorf("canceled: %w", cause)
	}
	run.End = s.nowFunc()
	switch {
	case errors.Is(err, ErrIdle):
		return run, err
	case err == nil:
		run.Outcome = OutcomeSucceeded
	case errors.Is(err, ErrSkipped):
		run.Outcome, run.Error = OutcomeSkipped, err.Error()
	default:
		run.Outcome, run.Error = OutcomeFailed, err.Error()
		if ctx.Err() != nil && trigger == TriggerSchedule {
			run.Error = "canceled: Igloo was shutting down"
		}
	}
	s.write(ctx, run)
	s.end(j, run)
	if errors.Is(err, ErrOwnerGone) {
		s.suspend(ctx, j, err)
	}
	return run, err
}

// watch checks every heldEvery that lease is still held, until stop is
// called, and cancels the run, whose context is ctx, once it is not. stop
// returns once the checks have, so the lease can be released.
func (s *Scheduler) watch(ctx context.Context, lease Lease, cancel context.CancelCauseFunc) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(s.heldEvery)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			hctx, hcancel := context.WithTimeout(ctx, writeTimeout)
			err := lease.Held(hctx)
			hcancel()
			if err != nil && ctx.Err() == nil {
				cancel(fmt.Errorf("%w: %w", errLockLost, err))
				return
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// end tells Options.Ended about a run that ended.
func (s *Scheduler) end(j Job, run Run) {
	if s.ended != nil {
		s.ended(j, run)
	}
}

func (s *Scheduler) write(ctx context.Context, run Run) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	if err := s.store.WriteRun(ctx, run); err != nil {
		s.log.Warn("could not record a run", "kind", run.Kind, "name", run.Name, "err", err)
	}
}

// suspend stops scheduling j, because of cause.
func (s *Scheduler) suspend(ctx context.Context, j Job, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	if err := s.store.SetState(ctx, j.Kind, j.Name, StateSuspended, time.Time{}); err != nil {
		s.log.Warn("could not suspend", "kind", j.Kind, "name", j.Name, "err", err)
		return
	}
	s.log.Warn("suspended", "kind", j.Kind, "name", j.Name, "owner", j.Owner, "reason", cause)
	s.Kick()
}

func (s *Scheduler) prune(ctx context.Context) {
	if s.retention <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := s.store.Prune(ctx, time.Now().Add(-s.retention)); err != nil && ctx.Err() == nil {
		s.log.Warn("could not delete old task history", "err", err)
	}
}
