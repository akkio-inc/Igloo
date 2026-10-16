package tasks

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
)

// Bounds on the history a MemoryStore keeps.
const (
	MemoryRuns  = 10_000
	MemoryLoads = 100_000
)

// MemoryStore keeps everything in memory, for one replica without a
// metadata database. It is lost on restart.
type MemoryStore struct {
	mu    sync.Mutex
	jobs  map[[2]string]Job
	runs  []Run
	loads []Load
	locks map[string]chan struct{}
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: map[[2]string]Job{}, locks: map[string]chan struct{}{}}
}

func mapKey(kind, name string) [2]string { return [2]string{kind, strings.ToLower(name)} }

func (m *MemoryStore) CreateJob(_ context.Context, j Job, replace bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mapKey(j.Kind, j.Name)
	if _, ok := m.jobs[k]; ok && !replace {
		return ErrExists
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	j.LastRun = nil
	m.jobs[k] = j
	return nil
}

func (m *MemoryStore) Job(_ context.Context, kind, name string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[mapKey(kind, name)]
	if !ok {
		return Job{}, ErrNotFound
	}
	j.LastRun = m.lastRun(j)
	return j, nil
}

func (m *MemoryStore) lastRun(j Job) *Run {
	for i := len(m.runs) - 1; i >= 0; i-- {
		if r := m.runs[i]; r.Kind == j.Kind && strings.EqualFold(r.Name, j.Name) {
			return &r
		}
	}
	return nil
}

func (m *MemoryStore) Jobs(_ context.Context, kind string) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		if j.Kind == kind {
			j.LastRun = m.lastRun(j)
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out, nil
}

func (m *MemoryStore) update(kind, name string, fn func(*Job)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mapKey(kind, name)
	j, ok := m.jobs[k]
	if !ok {
		return ErrNotFound
	}
	fn(&j)
	m.jobs[k] = j
	return nil
}

func (m *MemoryStore) SetState(_ context.Context, kind, name, state string, next time.Time) error {
	return m.update(kind, name, func(j *Job) { j.State, j.NextRun = state, next })
}

func (m *MemoryStore) SetSchedule(_ context.Context, kind, name, schedule, timezone string, next time.Time) error {
	return m.update(kind, name, func(j *Job) { j.Schedule, j.Timezone, j.NextRun = schedule, timezone, next })
}

func (m *MemoryStore) DropJob(_ context.Context, kind, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mapKey(kind, name)
	if _, ok := m.jobs[k]; !ok {
		return ErrNotFound
	}
	delete(m.jobs, k)
	return nil
}

func (m *MemoryStore) Scheduled(context.Context) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		if j.State == StateStarted && !j.NextRun.IsZero() {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].NextRun.Before(out[b].NextRun) })
	return out, nil
}

func (m *MemoryStore) Claim(_ context.Context, kind, name string, due, next time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mapKey(kind, name)
	j, ok := m.jobs[k]
	if !ok || j.State != StateStarted || !j.NextRun.Equal(due) {
		return false, nil
	}
	j.NextRun = next
	m.jobs[k] = j
	return true, nil
}

func (m *MemoryStore) lock(key string) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.locks[key]
	if !ok {
		ch = make(chan struct{}, 1)
		m.locks[key] = ch
	}
	return ch
}

func (m *MemoryStore) TryLock(_ context.Context, key string) (func(), bool, error) {
	ch := m.lock(key)
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true, nil
	default:
		return nil, false, nil
	}
}

func (m *MemoryStore) Lock(ctx context.Context, key string) (func(), error) {
	ch := m.lock(key)
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *MemoryStore) WriteRun(_ context.Context, r Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i := slices.IndexFunc(m.runs, func(x Run) bool { return x.ID == r.ID }); i >= 0 {
		m.runs[i] = r
		return nil
	}
	m.runs = append(m.runs, r)
	if len(m.runs) > MemoryRuns {
		m.runs = slices.Delete(m.runs, 0, len(m.runs)-MemoryRuns)
	}
	return nil
}

func (m *MemoryStore) AbandonRuns(_ context.Context, kind, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, r := range m.runs {
		if r.Kind == kind && strings.EqualFold(r.Name, name) && r.Outcome == OutcomeRunning {
			m.runs[i].Outcome, m.runs[i].Error, m.runs[i].End = OutcomeFailed, abandoned, time.Now()
		}
	}
	return nil
}

// abandoned explains a run that never finished.
const abandoned = "Igloo stopped during the run"

func (m *MemoryStore) Runs(_ context.Context, v audit.Viewer, p audit.Page) ([]Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return page(m.runs, v, p, func(r Run) (time.Time, string, string) { return r.Start, r.ID, r.User }), nil
}

func (m *MemoryStore) Prune(_ context.Context, before time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs = slices.DeleteFunc(m.runs, func(r Run) bool { return r.Start.Before(before) })
	m.loads = slices.DeleteFunc(m.loads, func(l Load) bool { return l.Status == LoadFailed && l.Start.Before(before) })
	return nil
}

func (m *MemoryStore) LastLoad(_ context.Context, pipe string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var last time.Time
	for _, r := range m.runs {
		if r.Kind == KindPipe && strings.EqualFold(r.Name, pipe) && (r.Outcome == OutcomeSucceeded || r.Rows > 0) &&
			r.Start.After(last) {
			last = r.Start
		}
	}
	return last, nil
}

func (m *MemoryStore) PreviousLoads(_ context.Context, table string, files []string) (map[string]Load, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	want := make(map[string]bool, len(files))
	for _, f := range files {
		want[f] = true
	}
	out := map[string]Load{}
	for _, l := range m.loads {
		if strings.EqualFold(l.Table, table) && want[l.File] {
			out[l.File] = l // later loads come later
		}
	}
	return out, nil
}

func (m *MemoryStore) WriteLoads(_ context.Context, ls []Load) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range ls {
		l.Modified = Truncate(l.Modified)
		if i := slices.IndexFunc(m.loads, func(x Load) bool { return x.ID == l.ID }); i >= 0 {
			m.loads[i] = l
			continue
		}
		m.loads = append(m.loads, l)
	}
	if len(m.loads) > MemoryLoads {
		m.loads = slices.Delete(m.loads, 0, len(m.loads)-MemoryLoads)
	}
	return nil
}

func (m *MemoryStore) DeleteLoads(_ context.Context, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loads = slices.DeleteFunc(m.loads, func(l Load) bool { return slices.Contains(ids, l.ID) })
	return nil
}

func (m *MemoryStore) Loads(_ context.Context, v audit.Viewer, p audit.Page) ([]Load, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return page(m.loads, v, p, func(l Load) (time.Time, string, string) { return l.Start, l.ID, l.User }), nil
}

func (m *MemoryStore) ForgetTable(_ context.Context, table string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loads = slices.DeleteFunc(m.loads, func(l Load) bool { return strings.EqualFold(l.Table, table) })
	delete(m.jobs, mapKey(KindMaterializedView, mvName(table)))
	return nil
}

func (m *MemoryStore) RenameTable(_ context.Context, from, to string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, l := range m.loads {
		if strings.EqualFold(l.Table, from) {
			m.loads[i].Table = to
		}
	}
	if j, ok := m.jobs[mapKey(KindMaterializedView, mvName(from))]; ok {
		delete(m.jobs, mapKey(KindMaterializedView, mvName(from)))
		j.Name = mvName(to)
		m.jobs[mapKey(KindMaterializedView, j.Name)] = j
	}
	return nil
}

func (m *MemoryStore) Close() {}

// page returns what v may see of records, newest first, as p selects.
// key gives a record's time, ID and user.
func page[T any](records []T, v audit.Viewer, p audit.Page, key func(T) (time.Time, string, string)) []T {
	var out []T
	for _, r := range records {
		t, id, user := key(r)
		switch {
		case !viewerSees(v, user),
			!p.Since.IsZero() && t.Before(p.Since),
			!p.Until.IsZero() && !t.Before(p.Until),
			!p.AfterTime.IsZero() && !(t.Before(p.AfterTime) || t.Equal(p.AfterTime) && id < p.AfterID):
			continue
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(a, b int) bool {
		ta, ida, _ := key(out[a])
		tb, idb, _ := key(out[b])
		if !ta.Equal(tb) {
			return ta.After(tb)
		}
		return ida > idb
	})
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}
