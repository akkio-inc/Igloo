package pgwire

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// prepPhases is how long the steps of preparing a statement, or of
// readying a prepared one to run, took: for the debug log, which shows
// where the time a client sees as a whole goes (see timePhases).
type prepPhases struct {
	start time.Time
	names []string
	took  []time.Duration
	times []int
}

// timePhases times the steps the session's statement goes through until
// the returned function, which logs them as what (at debug), as "took"
// and "phases". It does nothing when the log is not at debug, or within
// steps already timed, whose log has these steps too.
func (s *Server) timePhases(ctx context.Context, sess *session, what string) func() {
	if sess.phases != nil || !s.log.Enabled(ctx, slog.LevelDebug) {
		return func() {}
	}
	p := &prepPhases{start: time.Now()}
	sess.phases = p
	return func() {
		sess.phases = nil
		s.log.Debug(what, "took", time.Since(p.start).Round(time.Millisecond), "phases", p.String())
	}
}

// phase starts timing a step (see timePhases), and returns what ends it.
// Steps of the same name add up, and a step may be within another.
func (sess *session) phase(name string) func() {
	p := sess.phases
	if p == nil {
		return func() {}
	}
	t := time.Now()
	return func() {
		d := time.Since(t)
		for i, n := range p.names {
			if n == name {
				p.took[i] += d
				p.times[i]++
				return
			}
		}
		p.names = append(p.names, name)
		p.took = append(p.took, d)
		p.times = append(p.times, 1)
	}
}

// String lists the steps as name=duration, in the order they first
// ended, with how many times, after a slash, those taken more than once.
func (p *prepPhases) String() string {
	var b strings.Builder
	for i, n := range p.names {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(n + "=" + p.took[i].Round(time.Millisecond).String())
		if p.times[i] > 1 {
			b.WriteString("/" + strconv.Itoa(p.times[i]))
		}
	}
	return b.String()
}
