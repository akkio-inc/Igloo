package notify

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Built-in operational alerts. Igloo itself notices failed runs, load
// errors, failed-login bursts and maintenance failures as they happen, and
// looks for idle pipes and long queries every checkInterval. Each goes to
// every enabled integration that has it switched on, once per problem in
// a while (see the repeat windows below): the same task failing every five
// minutes notifies once an hour, not twelve times.
const (
	checkInterval = time.Minute
	// cacheTTL is how long the list of integrations is reused; changes on
	// other replicas reach built-in alerts within it.
	cacheTTL = 30 * time.Second
	// eventQueue bounds the events waiting to be sent; more are dropped.
	eventQueue = 256
	// pruneInterval is how often old history is deleted, and claimKeep how
	// long claims are kept: longer than any repeat window.
	pruneInterval = time.Hour
	claimKeep     = 31 * 24 * time.Hour
	// recordTimeout bounds writing history.
	recordTimeout = 10 * time.Second

	repeatFailedRun   = time.Hour
	repeatLoadErrors  = time.Hour
	repeatMaintenance = 24 * time.Hour
	repeatLongQuery   = 7 * 24 * time.Hour
)

// event is a built-in alert's notification, before it goes out.
type event struct {
	builtin string
	// key names the problem; one notification per key goes out per
	// repeat window, from any replica.
	key    string
	repeat time.Duration
	msg    Message
}

// Statement is a statement running on this replica, for the long query
// alert. Text must have its secrets removed.
type Statement struct {
	PID     int32
	User    string
	Text    string
	Started time.Time
}

// emit queues e for the integrations that have its built-in alert on.
func (s *Service) emit(e event) {
	e.msg.Type, e.msg.Name = RecordBuiltin, e.builtin
	if e.msg.Time.IsZero() {
		e.msg.Time = s.nowFunc()
	}
	select {
	case s.events <- e:
	default:
		s.log.Warn("too many operational alerts waiting; dropped one", "alert", e.builtin, "key", e.key)
	}
}

// RunEnded is tasks.Options.Ended: scheduled runs of tasks, pipes and
// materialized views that failed raise the failed_runs alert. Alerts'
// failures are in the alert history, and do not raise it: an integration
// that cannot be reached would otherwise be told so through itself.
func (s *Service) RunEnded(j tasks.Job, r tasks.Run) {
	if r.Outcome != tasks.OutcomeFailed || r.Trigger != tasks.TriggerSchedule || j.Kind == tasks.KindAlert {
		return
	}
	s.emit(event{
		builtin: BuiltinFailedRuns, key: "failed_runs/" + j.Kind + "/" + strings.ToLower(j.Name), repeat: repeatFailedRun,
		msg: Message{
			Title: "Igloo: a scheduled " + j.Kind + " failed",
			Body: []Part{Text("The " + j.Kind + " "), Value(j.Name), Text(", owned by "), Value(j.Owner),
				Text(", failed at " + r.Start.UTC().Format(time.RFC3339) + ": "), Value(r.Error)},
			Details: map[string]string{"kind": j.Kind, "name": j.Name, "owner": j.Owner, "run_id": r.ID, "error": r.Error},
		},
	})
}

// LoadFailed raises load_errors for a COPY INTO, or a pipe's, that could not
// load some files: failed of total, the first with firstError.
func (s *Service) LoadFailed(table, user, pipe string, failed, total int, file, firstError string) {
	by := []Part{Text(" by "), Value(user)}
	if pipe != "" {
		by = []Part{Text(" by the pipe "), Value(pipe)}
	}
	body := append([]Part{Text("COPY INTO "), Value(table)}, by...)
	body = append(body, Text(fmt.Sprintf(" could not load %d of %d files. The first, ", failed, total)), Value(file),
		Text(": "), Value(firstError))
	s.emit(event{
		builtin: BuiltinLoadErrors, key: "load_errors/" + strings.ToLower(table), repeat: repeatLoadErrors,
		msg: Message{
			Title: "Igloo: files failed to load",
			Body:  body,
			Details: map[string]string{"table": table, "user": user, "pipe": pipe, "failed": strconv.Itoa(failed),
				"files": strconv.Itoa(total), "file": file, "error": firstError},
		},
	})
}

// LoginsBlocked raises failed_logins when the login limiter starts refusing
// password logins for a user (byUser) or from an address.
func (s *Service) LoginsBlocked(who string, byUser bool, failures int, window time.Duration) {
	what, key := "user", "failed_logins/user/"+who
	if !byUser {
		what, key = "address", "failed_logins/address/"+who
	}
	s.emit(event{
		builtin: BuiltinFailedLogins, key: key, repeat: window,
		msg: Message{
			Title: "Igloo: password logins refused after failed logins",
			Body: []Part{Text(fmt.Sprintf("%d failed logins in %s for the %s ", failures, window, what)), Value(who),
				Text(": this replica refuses its password logins for a while. See igloo.login_history.")},
			Details: map[string]string{what: who, "failures": strconv.Itoa(failures)},
		},
	})
}

// MaintenanceFailed is maintenance.Options.Failed: it raises
// maintenance_failures.
func (s *Service) MaintenanceFailed(table string, err error) {
	what := []Part{Text("Maintenance of the table "), Value(table)}
	if table == "" {
		what = []Part{Text("Table maintenance could not list the tables")}
	}
	s.emit(event{
		builtin: BuiltinMaintenance, key: "maintenance/" + strings.ToLower(table), repeat: repeatMaintenance,
		msg: Message{
			Title:   "Igloo: table maintenance failed",
			Body:    append(what, Text(" failed: "), Value(err.Error())),
			Details: map[string]string{"table": table, "error": err.Error()},
		},
	})
}

// Run sends built-in alerts, looks for idle pipes and long queries, and
// prunes the alert history, until ctx is canceled.
func (s *Service) Run(ctx context.Context) {
	check := time.NewTicker(checkInterval)
	defer check.Stop()
	prune := time.NewTicker(pruneInterval)
	defer prune.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-s.events:
			s.deliver(ctx, e)
		case <-check.C:
			s.check(ctx)
		case <-prune.C:
			s.prune(ctx)
		}
	}
}

// integrations are the enabled integrations with built-in alerts.
func (s *Service) integrations(ctx context.Context) []Integration {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if !s.cacheAt.IsZero() && s.nowFunc().Sub(s.cacheAt) < s.cacheTTL {
		return s.cache
	}
	all, err := s.store.Integrations(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("could not read the notification integrations", "err", err)
		}
		return s.cache
	}
	// A new slice: callers may still hold the last one.
	var fresh []Integration
	for _, in := range all {
		if in.Enabled && !in.Builtin.Empty() {
			fresh = append(fresh, in)
		}
	}
	s.cache, s.cacheAt = fresh, s.nowFunc()
	return s.cache
}

// Changed makes built-in alerts read the integrations again, after one
// changed on this replica.
func (s *Service) Changed() {
	s.cacheMu.Lock()
	s.cacheAt = time.Time{}
	s.cacheMu.Unlock()
}

// deliver sends e to each integration that has its alert on.
func (s *Service) deliver(ctx context.Context, e event) {
	for _, in := range s.integrations(ctx) {
		if in.Builtin.Has(e.builtin) {
			s.deliverTo(ctx, in, e)
		}
	}
}

// check raises idle_pipes and long_queries for the integrations that have
// them on.
func (s *Service) check(ctx context.Context) {
	idle, long := false, time.Duration(0)
	for _, in := range s.integrations(ctx) {
		idle = idle || in.Builtin.IdlePipes > 0
		if d := in.Builtin.LongQueries; d > 0 && (long == 0 || d < long) {
			long = d
		}
	}
	if idle && s.jobs != nil {
		s.checkPipes(ctx)
	}
	if long > 0 && s.statements != nil {
		s.checkStatements(ctx, long)
	}
}

// checkPipes looks for started pipes that loaded nothing for longer than
// some integration's IDLE_PIPES, and tells those integrations.
func (s *Service) checkPipes(ctx context.Context) {
	pipes, err := s.jobs.Jobs(ctx, tasks.KindPipe)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("could not list pipes for the idle pipe alert", "err", err)
		}
		return
	}
	now := s.nowFunc()
	for _, p := range pipes {
		if p.State != tasks.StateStarted {
			continue
		}
		last, err := s.jobs.LastLoad(ctx, p.Name)
		if err != nil {
			s.log.Warn("could not read a pipe's runs", "pipe", p.Name, "err", err)
			continue
		}
		since := "it was created"
		if last.IsZero() {
			last = p.CreatedAt
		} else {
			since = "its last load"
		}
		for _, in := range s.integrations(ctx) {
			d := in.Builtin.IdlePipes
			if d == 0 || now.Sub(last) < d {
				continue
			}
			hours := int(now.Sub(last) / time.Hour)
			e := event{
				builtin: BuiltinIdlePipes, key: "idle_pipes/" + strings.ToLower(p.Name), repeat: d,
				msg: Message{
					Type: RecordBuiltin, Name: BuiltinIdlePipes, Time: now,
					Title: "Igloo: a pipe has loaded no files",
					Body: []Part{Text("The pipe "), Value(p.Name), Text(", owned by "), Value(p.Owner),
						Text(fmt.Sprintf(", has loaded no new files in the %d hours since %s (at %s). Check that its files are arriving.",
							hours, since, last.UTC().Format(time.RFC3339)))},
					Details: map[string]string{"pipe": p.Name, "owner": p.Owner, "since": last.UTC().Format(time.RFC3339)},
				},
			}
			s.deliverTo(ctx, in, e)
		}
	}
}

// checkStatements looks for statements running on this replica for longer
// than some integration's LONG_QUERIES.
func (s *Service) checkStatements(ctx context.Context, shortest time.Duration) {
	now := s.nowFunc()
	for _, st := range s.statements() {
		age := now.Sub(st.Started)
		if st.Started.IsZero() || age < shortest {
			continue
		}
		for _, in := range s.integrations(ctx) {
			d := in.Builtin.LongQueries
			if d == 0 || age < d {
				continue
			}
			text := st.Text
			if len(text) > MaxValueBytes {
				text = text[:MaxValueBytes]
			}
			e := event{
				builtin: BuiltinLongQueries, repeat: repeatLongQuery,
				key: fmt.Sprintf("long_queries/%d/%d", st.PID, st.Started.UnixNano()),
				msg: Message{
					Type: RecordBuiltin, Name: BuiltinLongQueries, Time: now,
					Title: "Igloo: a statement is running long",
					Body: []Part{Text("A statement by "), Value(st.User),
						Text(fmt.Sprintf(" has run for %d minutes (session %d): ", int(age/time.Minute), st.PID)), Value(text)},
					Details: map[string]string{"user": st.User, "pid": strconv.Itoa(int(st.PID)),
						"started": st.Started.UTC().Format(time.RFC3339), "statement": text},
				},
			}
			s.deliverTo(ctx, in, e)
		}
	}
}

// deliverTo sends e to in, unless this problem was sent to it within the
// repeat window, and records the notification.
func (s *Service) deliverTo(ctx context.Context, in Integration, e event) {
	ok, err := s.store.Claim(ctx, in.Name+"/"+e.key, e.repeat)
	if err != nil || !ok {
		if err != nil {
			s.log.Warn("could not record an operational alert", "alert", e.builtin, "integration", in.Name, "err", err)
		}
		return
	}
	rec := Record{ID: audit.NewID(s.nowFunc()), Type: RecordBuiltin, Name: e.builtin, Trigger: TriggerEvent,
		Start: s.nowFunc(), Rows: -1, Integration: in.Name}
	rec.Attempts, err = s.Send(ctx, in, e.msg)
	rec.State, rec.End = StateTriggered, s.nowFunc()
	if err != nil {
		rec.State, rec.Error = StateActionFailed, err.Error()
	}
	s.Record(ctx, rec)
}

// Record adds r to the alert history, even while Igloo shuts down.
func (s *Service) Record(ctx context.Context, r Record) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()
	if err := s.store.WriteRecord(ctx, r); err != nil {
		s.log.Warn("could not record in the alert history", "alert", r.Name, "err", err)
	}
}

func (s *Service) prune(ctx context.Context) {
	if s.retention <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	now := s.nowFunc()
	if err := s.store.Prune(ctx, now.Add(-s.retention), now.Add(-claimKeep)); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn("could not delete old alert history", "err", err)
	}
}
