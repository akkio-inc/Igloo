package memlimit

import (
	"context"
	"log/slog"
	"time"
)

// Under heavy work DuckDB keeps the memory its buffer manager tracks at its
// limit and allocates more besides, which it does not track: 8 GiB more,
// with the rest of the process, for one statement over ten times the data
// on a 61 GiB machine, where Watch then had to cancel it. That memory is
// not the allocator's free pages (purging them changed nothing); it is
// work in progress, and grows with the work. So Adapt gives it room:
// while what the process can still allocate is below High(total), it
// lowers DuckDB's memory limit, which makes DuckDB write buffers to its
// spill files and give their memory back, and once memory has stayed
// plentiful it raises the limit back toward the configured one. Statements
// then run slower, spilling more, rather than fail.

// High is the memory left, of total, below which Adapt lowers DuckDB's
// memory limit: an eighth of it, at least 512 MiB, twice Low, so that it
// acts well before Watch would shed work.
func High(total int64) int64 {
	return max(total/8, 512<<20)
}

const (
	// adaptEvery is how often Adapt checks memory, and adaptCalm how long
	// memory must stay plentiful, above twice High, before it raises the
	// limit, by a step at a time.
	adaptEvery = 500 * time.Millisecond
	adaptCalm  = 30 * time.Second
	// minChange is the least change of the limit worth making.
	minChange = 256 << 20
)

// Adapt keeps DuckDB's memory limit, base as configured, low enough that
// the memory DuckDB uses besides fits beside it (see High), never below
// half of base. set changes the limit. It returns when ctx ends, and does
// nothing when total or base is 0.
func Adapt(ctx context.Context, total, base int64, set func(int64) error, log *slog.Logger) {
	if total <= 0 || base <= 0 {
		return
	}
	a := &adapter{total: total, base: base, limit: base, available: Available, set: set, log: log}
	t := time.NewTicker(adaptEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			a.step(now)
		}
	}
}

type adapter struct {
	total, base, limit int64
	available          func() (int64, bool)
	set                func(int64) error
	log                *slog.Logger
	// plentySince is when memory last became plentiful, zero while it is
	// not; failedAt when a change last failed.
	plentySince, failedAt time.Time
}

// step makes one check, and changes the limit if memory calls for it.
func (a *adapter) step(now time.Time) {
	left, ok := a.available()
	if !ok {
		return
	}
	high, floor := High(a.total), a.base/2
	switch {
	case left < high:
		a.plentySince = time.Time{}
		// Down by what is missing, and a step more, so that DuckDB frees
		// enough at once rather than a little at every check.
		if want := max(floor, a.limit-(high-left)-a.total/64); a.limit-want >= minChange {
			a.change(now, want, left)
		}
	case left > 2*high && a.limit < a.base:
		if a.plentySince.IsZero() {
			a.plentySince = now
			return
		}
		if now.Sub(a.plentySince) < adaptCalm {
			return
		}
		// Up by half of what is to spare, and wait as long again before
		// the next step.
		if want := min(a.base, a.limit+(left-2*high)/2); want-a.limit >= minChange {
			a.change(now, want, left)
		}
		a.plentySince = now
	default:
		a.plentySince = time.Time{}
	}
}

func (a *adapter) change(now time.Time, want, left int64) {
	if err := a.set(want); err != nil {
		// DuckDB could not free enough for the new limit, its memory being
		// held by work it cannot spill; the next check tries again.
		if now.Sub(a.failedAt) > time.Minute {
			a.log.Warn("could not change DuckDB's memory limit", "from", Format(a.limit), "to", Format(want),
				"available", Format(left), "err", err)
		}
		a.failedAt = now
		return
	}
	switch {
	case want == a.base:
		a.log.Info("DuckDB's memory limit is back to what it was", "limit", Format(want), "available", Format(left))
	case a.limit == a.base:
		a.log.Info("memory is running low; lowered DuckDB's memory limit so that it spills", "from", Format(a.limit),
			"to", Format(want), "available", Format(left))
	default:
		a.log.Debug("changed DuckDB's memory limit", "from", Format(a.limit), "to", Format(want), "available", Format(left))
	}
	a.limit = want
}
