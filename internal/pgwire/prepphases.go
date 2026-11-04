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

// timePhases times the steps the session's statement, text, goes through
// until the returned function, which logs them as what (at debug), as
// "took" and "phases" (see phaseAttrs). It does nothing when the log is not
// at debug, or within steps already timed, whose log has these steps too.
func (s *Server) timePhases(ctx context.Context, sess *session, what, text string) func() {
	if sess.phases != nil || !s.log.Enabled(ctx, slog.LevelDebug) {
		return func() {}
	}
	p := &prepPhases{start: time.Now()}
	sess.phases = p
	return func() {
		sess.phases = nil
		s.log.Debug(what, phaseAttrs(time.Since(p.start), p, sess.pid, text)...)
	}
}

// slowPhases is how long a statement's steps take before their log line
// says which statement it was.
const slowPhases = time.Second

// phaseAttrs are the log attributes of steps that took took: how long, the
// steps, and for steps of slowPhases or more the session's pid and the
// statement's first words, redacted as the history has it. Dashboards'
// prepares took 2 to 8 s at once, inside DuckDB, beside refreshes and ELT,
// and their lines said nothing of what they bound.
func phaseAttrs(took time.Duration, p *prepPhases, pid int32, text string) []any {
	attrs := []any{"took", took.Round(time.Millisecond), "phases", p.String()}
	if took < slowPhases {
		return attrs
	}
	q := strings.Join(strings.Fields(redactQuery(text)), " ")
	if len(q) > 200 {
		q = q[:200] + "..."
	}
	return append(attrs, "pid", pid, "statement", q)
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
