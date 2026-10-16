package erasure

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Catalog is the Iceberg catalog; *catalog.Client is one.
type Catalog interface {
	Tables(ctx context.Context) ([]table.Identifier, error)
	LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error)
	CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error)
}

// History keeps history records and can redact identifiers from them:
// the query history (package audit) and the load and task history
// (package tasks).
type History interface {
	Redact(ctx context.Context, r audit.Redactor) (int64, error)
}

// Lineage tells which statements wrote tables from others: the query
// history.
type Lineage interface {
	Writes(ctx context.Context, tables, names []string) ([]audit.Query, error)
}

// Options configure a Service.
type Options struct {
	Store   Store
	Catalog Catalog
	// Policies returns the current tags.
	Policies func(ctx context.Context) (*policy.Snapshot, error)
	Engine   Engine
	// CatalogName is the catalog's name in DuckDB, and Schema its default
	// schema, which unqualified names in history resolve to.
	CatalogName string
	Schema      string
	// Warehouse is the warehouse's root, with a trailing slash. Requests
	// delete orphan files only of tables under it.
	Warehouse string
	// Jobs runs requests. Without it they run only when Run is called.
	Jobs *tasks.Scheduler
	// History are the stores whose records requests redact; nil ones are
	// left out.
	History []History
	// Lineage, if set, finds tables written from the tables a request
	// erases from.
	Lineage Lineage
	// Purger is this replica's, which a request has drop files at once.
	Purger *Purger
	// Rows deletes and counts rows. Nil means statements in DuckDB on the
	// catalog's tables.
	Rows Rows
	Log  *slog.Logger
}

// Service accepts and carries out erasure requests.
type Service struct {
	opts   Options
	engine Engine
	rows   Rows
	log    *slog.Logger
	now    func() time.Time
	// mu runs one request at a time without a scheduler, as its lock does
	// with one.
	mu sync.Mutex
}

// New returns a Service.
func New(opts Options) *Service {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Schema == "" {
		opts.Schema = "public"
	}
	s := &Service{opts: opts, engine: opts.Engine, rows: opts.Rows, log: opts.Log.With("component", "erasure"), now: time.Now}
	if s.rows == nil {
		s.rows = catalogRows{engine: opts.Engine, catalog: opts.CatalogName}
	}
	return s
}

const (
	// runSchedule is how often a request's job runs until it completes,
	// so a run that failed is tried again soon.
	runSchedule = "1 minute"
	// lockKey serializes requests across replicas: two at once would
	// rewrite and expire the same tables.
	lockKey = "erasure"
	// maxRounds bounds how often verification sends a request back.
	maxRounds = 5
)

// Wanted are the identifiers of one tag value a request erases.
type Wanted struct {
	Tag, Value  string
	Identifiers []string
}

// Submit records a request by requester to erase the wanted identifiers,
// and schedules it. It refuses tag values no column has, so a request for
// data nobody tagged does not silently erase nothing.
func (s *Service) Submit(ctx context.Context, requester string, wanted []Wanted) (*Request, error) {
	wanted, err := s.check(ctx, wanted)
	if err != nil {
		return nil, err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	now := s.now()
	r := &Request{ID: audit.NewID(now), Requester: requester, Created: now, Updated: now, Status: StatusQueued,
		Step: StepFind, Salt: hex.EncodeToString(b[:])}
	for _, w := range wanted {
		r.Subjects = append(r.Subjects, newSubject(r.Salt, w.Tag, w.Value, w.Identifiers))
	}
	// The job comes first: one whose request is missing ends itself,
	// while a request without a job would never run.
	if err := s.schedule(ctx, r); err != nil {
		return nil, fmt.Errorf("schedule the request: %w", err)
	}
	if err := s.opts.Store.Create(ctx, r); err != nil {
		s.unschedule(ctx, r.ID)
		return nil, err
	}
	s.log.Info("erasure requested", "request", r.ID, "by", requester, "subjects", subjectNames(r.Subjects))
	return r, nil
}

// check validates and merges what a request wants erased.
func (s *Service) check(ctx context.Context, wanted []Wanted) ([]Wanted, error) {
	snap, err := s.opts.Policies(ctx)
	if err != nil {
		return nil, err
	}
	var out []Wanted
	total := 0
	for _, w := range wanted {
		tag, ok := snap.Catalog.Tag(w.Tag)
		if !ok {
			return nil, fmt.Errorf("%w: tag %q does not exist", ErrInvalid, w.Tag)
		}
		if len(tag.AllowedValues) > 0 && !slices.ContainsFunc(tag.AllowedValues, func(v string) bool { return held(v, w.Value) }) {
			return nil, fmt.Errorf("%w: tag %s allows no value %q", ErrInvalid, w.Tag, w.Value)
		}
		if !slices.ContainsFunc(snap.Catalog.ColumnTags, func(ct policy.ColumnTag) bool { return ct.Tag == w.Tag && held(ct.Value, w.Value) }) {
			return nil, fmt.Errorf("%w: no column is tagged %s = '%s'; tag the columns that hold these identifiers first",
				ErrInvalid, w.Tag, w.Value)
		}
		i := slices.IndexFunc(out, func(o Wanted) bool { return o.Tag == w.Tag && o.Value == w.Value })
		if i < 0 {
			out = append(out, Wanted{Tag: w.Tag, Value: w.Value})
			i = len(out) - 1
		}
		for _, id := range w.Identifiers {
			switch {
			case normalize(id) == "":
				return nil, fmt.Errorf("%w: an identifier is empty", ErrInvalid)
			case len(id) > maxIdentifierLength:
				return nil, fmt.Errorf("%w: an identifier is longer than %d bytes", ErrInvalid, maxIdentifierLength)
			}
			if !slices.ContainsFunc(out[i].Identifiers, func(o string) bool { return normalize(o) == normalize(id) }) {
				out[i].Identifiers = append(out[i].Identifiers, id)
				total++
			}
		}
	}
	switch {
	case total == 0:
		return nil, fmt.Errorf("%w: no identifiers", ErrInvalid)
	case total > maxIdentifiers:
		return nil, fmt.Errorf("%w: at most %d identifiers per request", ErrInvalid, maxIdentifiers)
	}
	return out, nil
}

// holds reports whether a column tagged tagValue holds identifiers of
// value, and which digest of them: none when they are as they are. A
// request for a hashed value, as a partner's list of hashes asks, finds
// the columns of that value only.
func holds(tagValue, value string) (digest string, ok bool) {
	if tagValue == value {
		return "", true
	}
	v, digest, hashed := strings.Cut(tagValue, ":")
	return digest, hashed && v == value && digests[digest] != nil
}

func held(tagValue, value string) bool {
	_, ok := holds(tagValue, value)
	return ok
}

func subjectNames(subjects []Subject) string {
	names := make([]string, len(subjects))
	for i, sub := range subjects {
		names[i] = fmt.Sprintf("%s.%s (%d)", sub.Tag, sub.Value, sub.Count)
	}
	return strings.Join(names, ", ")
}

// List returns every request, newest first.
func (s *Service) List(ctx context.Context) ([]*Request, error) { return s.opts.Store.List(ctx) }

// Run carries the request id as far as it can go now. The scheduler runs it
// every minute until it completes, as an erasure job.
func (s *Service) Run(ctx context.Context, id string) error {
	release, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer release()
	r, err := s.opts.Store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		s.unschedule(ctx, id)
		return nil
	}
	if err != nil {
		return err
	}
	if r.Status == StatusCompleted {
		s.unschedule(ctx, id)
		return nil
	}
	r.Status, r.Error, r.WaitUntil = StatusRunning, "", time.Time{}
	r.Attempts++
	if err := s.save(ctx, r); err != nil {
		return err
	}

	runErr := s.advance(ctx, r)
	switch {
	case runErr != nil:
		r.Status, r.Error = StatusRetrying, runErr.Error()
		s.log.Warn("erasure request failed; it runs again in a minute", "request", id, "step", r.Step, "err", runErr)
	case r.Status == StatusWaiting:
		s.wake(ctx, id, r.WaitUntil)
	default:
		r.Status, r.Completed = StatusCompleted, s.now()
		r.forget()
		s.log.Info("erasure request completed", "request", id, "tables", len(r.Tables), "rows", r.rows())
	}
	if err := s.save(context.WithoutCancel(ctx), r); err != nil {
		return errors.Join(runErr, err)
	}
	if r.Status == StatusCompleted {
		s.unschedule(ctx, id)
	}
	return runErr
}

// advance runs r's steps until it is done, waits, or a step fails.
func (s *Service) advance(ctx context.Context, r *Request) error {
	for r.Step != StepDone {
		step, started := r.Step, s.now()
		next, err := s.step(ctx, r)
		if err != nil {
			return fmt.Errorf("%s: %w", step, err)
		}
		if r.Status == StatusWaiting {
			return nil
		}
		r.ran(step, started, s.now())
		r.Step = next
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// step runs r's current step and returns the one to run next.
func (s *Service) step(ctx context.Context, r *Request) (string, error) {
	next := nextStep(r.Step)
	var err error
	switch r.Step {
	case StepFind:
		err = s.find(ctx, r)
	case StepDelete:
		err = s.deleteRows(ctx, r)
	case StepRewrite:
		err = s.rewrite(ctx, r)
	case StepExpire:
		err = s.expire(ctx, r)
	case StepPurge:
		err = s.purge(ctx, r)
	case StepSharing:
		err = s.sharing(ctx, r)
	case StepHistory:
		err = s.history(ctx, r)
	case StepCache:
		err = s.cache(ctx, r)
	case StepVerify:
		return s.verify(ctx, r)
	default:
		err = fmt.Errorf("unknown step %q", r.Step)
	}
	return next, err
}

func (s *Service) save(ctx context.Context, r *Request) error {
	r.Updated = s.now()
	return s.opts.Store.Save(ctx, r)
}

// wait has r wait until at before it goes on.
func (r *Request) wait(at time.Time) {
	r.Status, r.WaitUntil = StatusWaiting, at
}

// ran records that step ran from started to finished.
func (r *Request) ran(step string, started, finished time.Time) {
	i := slices.IndexFunc(r.Steps, func(sr StepRecord) bool { return sr.Step == step })
	if i < 0 {
		r.Steps = append(r.Steps, StepRecord{Step: step, Started: started})
		i = len(r.Steps) - 1
	}
	r.Steps[i].Finished = finished
	r.Steps[i].Runs++
}

// forget deletes what could tell whose data r erased.
func (r *Request) forget() {
	r.Salt = ""
	for i := range r.Subjects {
		r.Subjects[i].Plain, r.Subjects[i].Digests, r.Subjects[i].Prefixes = nil, nil, nil
	}
}

func (r *Request) rows() int64 {
	var n int64
	for _, t := range r.Tables {
		n += t.RowsDeleted
	}
	return n
}

func (t *Table) done(step string, at time.Time) {
	if t.Steps == nil {
		t.Steps = map[string]time.Time{}
	}
	t.Steps[step] = at
}

// lock lets one request run at a time, on any replica.
func (s *Service) lock(ctx context.Context) (func(), error) {
	if s.opts.Jobs == nil {
		s.mu.Lock()
		return s.mu.Unlock, nil
	}
	return s.opts.Jobs.Store().Lock(ctx, lockKey)
}

func (s *Service) schedule(ctx context.Context, r *Request) error {
	if s.opts.Jobs == nil {
		return nil
	}
	err := s.opts.Jobs.Store().CreateJob(ctx, tasks.Job{Kind: JobKind, Name: r.ID, Owner: r.Requester,
		Statement: "ERASE SUBJECT: request " + r.ID, Schedule: runSchedule, State: tasks.StateStarted, NextRun: s.now()}, false)
	if err == nil {
		s.opts.Jobs.Kick()
	}
	return err
}

func (s *Service) unschedule(ctx context.Context, id string) {
	if s.opts.Jobs == nil {
		return
	}
	err := s.opts.Jobs.Store().DropJob(context.WithoutCancel(ctx), JobKind, id)
	if err != nil && !errors.Is(err, tasks.ErrNotFound) {
		s.log.Warn("could not end the job of an erasure request", "request", id, "err", err)
	}
}

// wake has the request's job run next at at.
func (s *Service) wake(ctx context.Context, id string, at time.Time) {
	if s.opts.Jobs == nil {
		return
	}
	if err := s.opts.Jobs.Store().SetState(context.WithoutCancel(ctx), JobKind, id, tasks.StateStarted, at); err != nil {
		s.log.Warn("could not reschedule an erasure request", "request", id, "err", err)
	}
}

// Columns name what Row returns, as igloo.erasure_requests and SHOW
// ERASURE REQUESTS list requests.
var Columns = []string{"request_id", "requester", "status", "step", "created_at", "updated_at", "completed_at",
	"subjects", "tables", "downstream", "history_redacted", "steps", "verification", "error"}

// Row is a request as igloo.erasure_requests lists it. Its JSON columns
// say what was done where, without hashes.
func Row(r *Request) []any {
	type subject struct {
		Tag         string `json:"tag"`
		Value       string `json:"value"`
		Identifiers int    `json:"identifiers"`
	}
	subjects := make([]subject, len(r.Subjects))
	for i, s := range r.Subjects {
		subjects[i] = subject{s.Tag, s.Value, s.Count}
	}
	text := func(v any) any {
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return string(b)
	}
	row := []any{r.ID, r.Requester, r.Status, r.Step, r.Created, r.Updated, nil, text(subjects),
		text(nonNil(r.Tables)), text(nonNil(r.Downstream)), r.HistoryRedacted, text(nonNil(r.Steps)), nil, nil}
	if !r.Completed.IsZero() {
		row[6] = r.Completed
	}
	if r.Verification != nil {
		row[12] = text(r.Verification)
	}
	if r.Error != "" {
		row[13] = r.Error
	}
	return row
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
