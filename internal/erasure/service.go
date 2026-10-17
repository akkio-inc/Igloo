package erasure

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/deletefiles"
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

// Locks are locks of table locations, shared by every replica.
type Locks interface {
	// Lock takes the lock of every location, or with wait false gives
	// up (ok false) if another holds one. Call release once ok.
	Lock(ctx context.Context, locations []string, wait bool) (release func(), ok bool, err error)
}

// Options configure a Service.
type Options struct {
	Store   Store
	Catalog Catalog
	// Policies returns the current tags.
	Policies func(ctx context.Context) (*policy.Snapshot, error)
	Engine   Engine
	// Key is the erasure key, which seals open requests' identifiers and
	// salts their hashes; it must not be kept in the metadata database,
	// and must be the same on every replica. Without it, requests can be
	// previewed but not made.
	Key []byte
	// CatalogName is the catalog's name in DuckDB, and Schema its default
	// schema, which unqualified names in history resolve to.
	CatalogName string
	Schema      string
	// Warehouse is the warehouse's root, with a trailing slash. Requests
	// delete only files under it.
	Warehouse string
	// Jobs runs requests. Without it they run only when Run is called.
	Jobs *tasks.Scheduler
	// History are the stores whose records requests redact; nil ones are
	// left out.
	History []History
	// Lineage, if set, finds tables written from the tables a request
	// erases from.
	Lineage Lineage
	// Purger is this replica's, which a request has empty its cache at
	// once.
	Purger *Purger
	// Locks, if set, are the locks of tables' locations that making a
	// zero-copy clone holds (clone.Store), which a request holds while it
	// deletes files: a clone made meanwhile could reference one.
	Locks Locks
	// Rows deletes and counts rows. Nil means statements in DuckDB on the
	// catalog's tables.
	Rows    Rows
	Metrics *Metrics
	// Failed, if set, is told about a request that failed for good, which
	// someone must make again: its ID, the step it stopped at, and why,
	// without identifiers. It must not block.
	Failed func(id, step, err string)
	Log    *slog.Logger
}

// Service accepts and carries out erasure requests.
type Service struct {
	opts   Options
	engine Engine
	rows   Rows
	sealer *sealer
	log    *slog.Logger
	now    func() time.Time
	// lease takes the lock that runs one request at a time, on any
	// replica.
	lease func(ctx context.Context) (tasks.Lease, bool, error)
	// mu stands in for the scheduler's locks without one.
	mu sync.Mutex
	// targets are what position delete files delete rows of.
	targets *deletefiles.Targets
	// cacheMu guards dirs.
	cacheMu sync.Mutex
	// dirs are the directories of the files tables reference, by
	// metadata file (see dirsOf).
	dirs map[string]*tableDirs
}

// New returns a Service.
func New(opts Options) *Service {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Schema == "" {
		opts.Schema = "public"
	}
	s := &Service{opts: opts, engine: opts.Engine, rows: opts.Rows, log: opts.Log.With("component", "erasure"), now: time.Now,
		targets: deletefiles.NewTargets(), dirs: map[string]*tableDirs{}}
	if s.rows == nil {
		s.rows = catalogRows{engine: opts.Engine, catalog: opts.CatalogName}
	}
	if len(opts.Key) > 0 {
		s.sealer = newSealer(opts.Key)
	}
	s.lease = func(ctx context.Context) (tasks.Lease, bool, error) {
		if s.opts.Jobs != nil {
			return s.opts.Jobs.Store().TryLease(ctx, lockKey)
		}
		if !s.mu.TryLock() {
			return nil, false, nil
		}
		return memLease(s.mu.Unlock), true, nil
	}
	return s
}

// memLease is a lock of this process, which it cannot lose.
type memLease func()

func (memLease) Held(context.Context) error { return nil }
func (l memLease) Release()                 { l() }

const (
	// runSchedule is how often a request's job runs until it completes.
	runSchedule = "1 minute"
	// lockKey serializes requests across replicas: two at once would
	// rewrite and expire the same tables.
	lockKey = "erasure"
	// maxRounds bounds how often verification sends a request back.
	maxRounds = 5
	// maxFailures is how many runs in a row may fail before a request
	// fails for good, waiting twice as long after each, up to maxBackoff.
	maxFailures = 10
	maxBackoff  = time.Hour
)

// Metrics count requests that need someone's attention. A nil *Metrics
// counts nothing.
type Metrics struct {
	failed, held prometheus.Counter
}

// NewMetrics registers the erasure metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_erasure_requests_failed_total",
			Help: "Erasure requests that failed too often to go on, which someone must look at and make again.",
		}),
		held: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_erasure_requests_held_total",
			Help: "Erasure requests held because they would delete much of a table, until CONFIRM ERASURE REQUEST.",
		}),
	}
	reg.MustRegister(m.failed, m.held)
	return m
}

// Wanted are the identifiers of one tag value a request erases.
type Wanted struct {
	Tag, Value  string
	Identifiers []string
}

// Submit records a request by requester to erase the wanted identifiers,
// and schedules it. It refuses tag values no column has, so a request for
// data nobody tagged does not silently erase nothing, and identifiers
// that stand for no one.
func (s *Service) Submit(ctx context.Context, requester string, wanted []Wanted) (*Request, error) {
	if s.sealer == nil {
		return nil, fmt.Errorf("%w: requests need the erasure key, IGLOO_ERASURE_KEY", ErrInvalid)
	}
	if s.opts.Warehouse == "" {
		return nil, fmt.Errorf("%w: requests need the warehouse's location, IGLOO_WAREHOUSE_LOCATION", ErrInvalid)
	}
	subjects, err := s.prepare(ctx, wanted)
	if err != nil {
		return nil, err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	now := s.now()
	r := &Request{ID: audit.NewID(now), Requester: requester, Created: now, Updated: now, Status: StatusQueued,
		Step: StepFind, Salt: hex.EncodeToString(b[:]), Subjects: subjects}
	ids := make([][]identifier, len(subjects))
	for i, sub := range subjects {
		ids[i] = sub.ids
	}
	if r.Sealed, err = s.sealer.sealIDs(r.ID, ids); err != nil {
		return nil, err
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

// Preview finds what a request for the wanted identifiers would erase
// from, and how many rows match in each table, without making one.
func (s *Service) Preview(ctx context.Context, wanted []Wanted) (*Request, error) {
	subjects, err := s.prepare(ctx, wanted)
	if err != nil {
		return nil, err
	}
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	r := &Request{Subjects: subjects, salt: hex.EncodeToString(b[:])}
	if r.fold, err = s.folding(ctx); err != nil {
		return nil, err
	}
	for i := range r.Subjects {
		r.Subjects[i].hash(r.salt)
	}
	return r, s.discover(ctx, r)
}

// prepare checks what a request wants erased, and returns its subjects
// with their identifiers as given and as DuckDB normalizes them.
func (s *Service) prepare(ctx context.Context, wanted []Wanted) ([]Subject, error) {
	snap, err := s.opts.Policies(ctx)
	if err != nil {
		return nil, err
	}
	var out []Subject
	total := 0
	for _, w := range wanted {
		tag, ok := snap.Catalog.Tag(w.Tag)
		if !ok {
			return nil, fmt.Errorf("%w: tag %q does not exist", ErrInvalid, w.Tag)
		}
		if len(tag.AllowedValues) > 0 && !slices.ContainsFunc(tag.AllowedValues, func(v string) bool { return held(v, w.Value) }) {
			return nil, fmt.Errorf("%w: tag %s allows no value %q", ErrInvalid, w.Tag, w.Value)
		}
		if !slices.ContainsFunc(snap.Catalog.ColumnTags, func(ct policy.ColumnTag) bool {
			return ct.Tag == w.Tag && ct.Catalog == "" && held(ct.Value, w.Value)
		}) {
			return nil, fmt.Errorf("%w: no column of the %s catalog's tables is tagged %s = '%s'; tag the columns that hold these identifiers first",
				ErrInvalid, s.opts.CatalogName, w.Tag, w.Value)
		}
		i := slices.IndexFunc(out, func(o Subject) bool { return o.Tag == w.Tag && o.Value == w.Value })
		if i < 0 {
			out = append(out, Subject{Tag: w.Tag, Value: w.Value})
			i = len(out) - 1
		}
		for _, id := range w.Identifiers {
			if len(id) > maxIdentifierLength {
				return nil, fmt.Errorf("%w: an identifier is longer than %d bytes", ErrInvalid, maxIdentifierLength)
			}
			normal, err := s.normalize(ctx, id)
			if err != nil {
				return nil, err
			}
			switch {
			case normal == "":
				return nil, fmt.Errorf("%w: an identifier is empty", ErrInvalid)
			case sentinel(normal) || sentinelDigest(normal):
				return nil, fmt.Errorf("%w: identifier %d of %s.%s stands for no one in particular, as a placeholder or an opted-out device's "+
					"all-zero ID does; it would erase the rows of everyone it stands for", ErrInvalid, len(out[i].ids)+1, w.Tag, w.Value)
			}
			if !slices.ContainsFunc(out[i].ids, func(o identifier) bool { return o.Normal == normal }) {
				out[i].ids = append(out[i].ids, identifier{Given: id, Normal: normal})
				out[i].Count++
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
	// The tagged tables must exist, and their columns hold values that
	// compare as identifiers.
	tables, err := s.tagged(snap, out)
	if err != nil {
		return nil, err
	}
	for i := range tables {
		t := &tables[i]
		if t.Remote {
			continue
		}
		tbl, err := s.opts.Catalog.LoadTable(ctx, identOf(t))
		if err != nil {
			return nil, fmt.Errorf("%w: %s is tagged, but cannot be loaded (%v): if it was renamed, its tags stayed with its old name; "+
				"tag it under its new name, or unset its tags if it was dropped", ErrInvalid, t.Name, err)
		}
		flds := fields(tbl, t)
		if err := resolved(t, flds); err != nil {
			return nil, err
		}
		if err := checkFields(t, flds); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// normalize is id as DuckDB's lower(trim(...)) leaves it, which is how
// column values compare.
func (s *Service) normalize(ctx context.Context, id string) (string, error) {
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck
	rows, err := conn.QueryContext(ctx, "SELECT lower(trim(CAST(? AS VARCHAR)))", []driver.NamedValue{{Ordinal: 1, Value: id}})
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	normal, _ := dest[0].(string)
	return normal, nil
}

// holds reports whether a column tagged tagValue holds identifiers of
// value, and which digest of them: none when they are as they are; and
// whether the column is to be nulled. A request for a hashed value, as a
// partner's list of hashes asks, finds the columns of that value only.
func holds(tagValue, value string) (digest string, null, ok bool) {
	tv, null := strings.CutSuffix(tagValue, nullSuffix)
	if tv == value {
		return "", null, true
	}
	v, digest, hashed := strings.Cut(tv, ":")
	return digest, null, hashed && v == value && digests[digest] != nil
}

func held(tagValue, value string) bool {
	_, _, ok := holds(tagValue, value)
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
// every minute until it completes, as an erasure job. While another
// request runs, it returns tasks.ErrIdle at once.
func (s *Service) Run(ctx context.Context, id string) error {
	lease, ok, err := s.lease(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return tasks.ErrIdle
	}
	defer lease.Release()
	r, err := s.opts.Store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		s.unschedule(ctx, id)
		return nil
	}
	if err != nil {
		return err
	}
	switch r.Status {
	case StatusCompleted, StatusCanceled, StatusFailed:
		s.unschedule(ctx, id)
		return nil
	case StatusHeld:
		s.suspend(ctx, id)
		return nil
	}
	r.Status, r.Error, r.WaitUntil = StatusRunning, "", time.Time{}
	r.Attempts++
	runErr := s.open(ctx, r)
	if runErr == nil {
		if runErr = s.save(ctx, r); runErr != nil {
			return runErr
		}
		runErr = s.advance(ctx, r, lease)
	}
	switch {
	case runErr != nil:
		s.failed(ctx, r, runErr)
	case r.Status == StatusWaiting:
		s.wake(ctx, id, r.WaitUntil)
	case r.Status == StatusHeld:
		s.opts.Metrics.countHeld()
		s.suspend(ctx, id)
	default:
		r.Status, r.Completed = StatusCompleted, s.now()
		s.dropBranches(ctx, r)
		s.end(ctx, r)
		s.log.Info("erasure request completed", "request", id, "tables", len(r.Tables), "rows", r.rows())
	}
	if err := s.save(context.WithoutCancel(ctx), r); err != nil {
		return errors.Join(runErr, err)
	}
	if r.Status == StatusCompleted || r.Status == StatusFailed {
		s.unschedule(ctx, id)
	}
	if runErr != nil {
		// The scheduler records, logs and alerts on what Run returns: only
		// the scrubbed error goes there.
		return errors.New(r.Error)
	}
	return nil
}

// terminalError is an error a request cannot get past by trying again.
type terminalError struct{ error }

func (e terminalError) Unwrap() error { return e.error }

func terminal(err error) error { return terminalError{err} }

// contentionError is a step that lost to other writers' commits more often
// than it tries in one run: it tries again later, which does not count as
// a failure, however long it takes.
type contentionError struct{ error }

func (e contentionError) Unwrap() error { return e.error }

func contended(err error) error { return contentionError{err} }

const (
	// contentionWait is how long a request that lost to other writers
	// waits before it tries again.
	contentionWait = time.Minute
	// keyWait is how long a request whose identifiers this replica cannot
	// unseal waits: until a replica with the right key runs it.
	keyWait = 10 * time.Minute
)

// failed records that a run of r failed, and has it run again later, or,
// after too many failures or one it cannot get past, fail for good. A run
// that lost to other writers, or on a replica whose erasure key is not the
// one r's identifiers were sealed under, does not count: neither says
// anything is wrong with the request, and failing it would forget its
// identifiers for good.
func (s *Service) failed(ctx context.Context, r *Request, err error) {
	r.Error = scrub(r, err.Error())
	var c contentionError
	switch {
	case errors.Is(err, errKey):
		r.Status, r.WaitUntil = StatusRetrying, s.now().Add(keyWait)
		s.log.Error("this replica cannot unseal an erasure request's identifiers: its IGLOO_ERASURE_KEY is not the key they were sealed under; "+
			"set the same key on every replica", "request", r.ID, "in", keyWait)
		s.wake(ctx, r.ID, r.WaitUntil)
		return
	case errors.As(err, &c):
		r.Status, r.WaitUntil = StatusRetrying, s.now().Add(contentionWait)
		s.log.Warn("erasure request lost to other writers' commits; it tries again", "request", r.ID, "step", r.Step,
			"in", contentionWait, "err", r.Error)
		s.wake(ctx, r.ID, r.WaitUntil)
		return
	}
	r.Failures++
	var t terminalError
	if errors.As(err, &t) || r.Failures >= maxFailures {
		r.Status = StatusFailed
		s.dropBranches(ctx, r)
		s.end(ctx, r)
		s.opts.Metrics.countFailed()
		if s.opts.Failed != nil {
			s.opts.Failed(r.ID, r.Step, r.Error)
		}
		s.log.Error("erasure request failed; what it erased stays erased, but it will not go on: make a new request once the cause is fixed",
			"request", r.ID, "step", r.Step, "failures", r.Failures, "err", r.Error)
		return
	}
	backoff := min(time.Minute<<(r.Failures-1), maxBackoff)
	r.Status, r.WaitUntil = StatusRetrying, s.now().Add(backoff)
	s.log.Warn("erasure request failed; it runs again later", "request", r.ID, "step", r.Step, "in", backoff, "err", r.Error)
	s.wake(ctx, r.ID, r.WaitUntil)
}

// open unseals r's identifiers and computes their hashes, for a run.
func (s *Service) open(ctx context.Context, r *Request) error {
	if s.sealer == nil {
		return fmt.Errorf("%w (this replica has none)", errKey)
	}
	ids, err := s.sealer.unsealIDs(r.ID, r.Sealed)
	if err != nil {
		return err
	}
	if len(ids) != len(r.Subjects) {
		return errKey
	}
	if r.fold, err = s.folding(ctx); err != nil {
		return err
	}
	r.salt, r.written = s.sealer.hashSalt(r.Salt), map[string]bool{}
	for i := range r.Subjects {
		r.Subjects[i].ids = ids[i]
		r.Subjects[i].hash(r.salt)
	}
	return nil
}

// folding returns DuckDB's normalization tables, read once.
func (s *Service) folding(ctx context.Context) (*folding, error) {
	folds.mu.Lock()
	defer folds.mu.Unlock()
	if folds.f == nil {
		f, err := loadFolding(ctx, s.engine)
		if err != nil {
			return nil, fmt.Errorf("read DuckDB's lower() and trim(): %w", err)
		}
		folds.f = f
	}
	return folds.f, nil
}

// folds are DuckDB's normalization tables, the same for every connection
// of the DuckDB this process links.
var folds struct {
	mu sync.Mutex
	f  *folding
}

// dropBranches drops the branches r rewrote tags on, which a run that
// stopped may have left: the request is over.
func (s *Service) dropBranches(ctx context.Context, r *Request) {
	ctx = context.WithoutCancel(ctx)
	for i := range r.Tables {
		t := &r.Tables[i]
		if t.Remote {
			continue
		}
		if err := s.sweep(ctx, r, t, true); err != nil {
			s.log.Warn("could not drop an erasure request's branches; drop them by hand", "request", r.ID, "table", t.Name,
				"prefix", tagBranch+strings.ToLower(r.ID), "err", err)
		}
	}
}

// end deletes what could tell whose data r erased: its sealed identifiers
// and salt, and the paths of the files it deleted, which can hold them as
// partition values.
func (s *Service) end(ctx context.Context, r *Request) {
	r.Salt, r.Sealed, r.salt = "", "", ""
	for i := range r.Subjects {
		r.Subjects[i].ids, r.Subjects[i].plain, r.Subjects[i].digests, r.Subjects[i].prefixes = nil, nil, nil, nil
	}
	if err := s.opts.Store.Forget(context.WithoutCancel(ctx), r.ID); err != nil {
		s.log.Warn("could not remove an erasure request's file paths", "request", r.ID, "err", err)
	}
}

// advance runs r's steps until it is done, waits, or a step fails. Before
// each it checks that the run still holds its lock: another replica may
// have taken it if its connection dropped.
func (s *Service) advance(ctx context.Context, r *Request, lease tasks.Lease) error {
	for r.Step != StepDone {
		if err := lease.Held(ctx); err != nil {
			return fmt.Errorf("stopped before %s: %w", r.Step, err)
		}
		step, started := r.Step, s.now()
		next, err := s.step(ctx, r)
		if err != nil {
			return fmt.Errorf("%s: %w", step, err)
		}
		if r.Status == StatusWaiting {
			return nil
		}
		r.ran(step, started, s.now())
		r.Step, r.Failures = next, 0
		if err := s.save(ctx, r); err != nil {
			return err
		}
		if r.Status == StatusHeld {
			return nil
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

// Cancel cancels a request that has not begun deleting.
func (s *Service) Cancel(ctx context.Context, id, by string) (*Request, error) {
	return s.change(ctx, id, func(r *Request) error {
		if deleting(r) {
			return fmt.Errorf("%w: request %s has begun deleting, and must finish", ErrInvalid, id)
		}
		r.Status, r.Completed, r.Error = StatusCanceled, s.now(), ""
		s.end(ctx, r)
		s.unschedule(ctx, id)
		s.log.Info("erasure request canceled", "request", id, "by", by)
		return nil
	})
}

// Confirm lets a held request go on. Someone other than its requester
// must: the hold is a second pair of eyes.
func (s *Service) Confirm(ctx context.Context, id, by string) (*Request, error) {
	return s.change(ctx, id, func(r *Request) error {
		if r.Status != StatusHeld {
			return fmt.Errorf("%w: request %s is %s, not held", ErrInvalid, id, r.Status)
		}
		if strings.EqualFold(by, r.Requester) {
			return fmt.Errorf("%w: request %s was made by %s, who cannot confirm it too; another admin or privacy officer must",
				ErrInvalid, id, r.Requester)
		}
		r.Status, r.ConfirmedBy = StatusQueued, by
		s.log.Info("erasure request confirmed", "request", id, "by", by)
		if s.opts.Jobs != nil {
			if err := s.opts.Jobs.Store().SetState(ctx, JobKind, id, tasks.StateStarted, s.now()); err != nil {
				return err
			}
			defer s.opts.Jobs.Kick()
		}
		return nil
	})
}

// change applies fn to request id while no run of it is under way.
func (s *Service) change(ctx context.Context, id string, fn func(r *Request) error) (*Request, error) {
	release := s.mu.Unlock
	if s.opts.Jobs != nil {
		unlock, ok, err := s.opts.Jobs.Store().TryLock(ctx, tasks.JobKey(JobKind, id))
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrBusy
		}
		release = unlock
	} else if !s.mu.TryLock() {
		return nil, ErrBusy
	}
	defer release()
	r, err := s.opts.Store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	switch r.Status {
	case StatusCompleted, StatusCanceled, StatusFailed:
		return nil, fmt.Errorf("%w: request %s is %s", ErrInvalid, id, r.Status)
	}
	if err := fn(r); err != nil {
		return nil, err
	}
	return r, s.save(ctx, r)
}

// deleting reports whether r has begun deleting rows: once it sent a
// DELETE, whose commit may have landed though its answer was lost.
func deleting(r *Request) bool {
	if r.DeleteStarted || r.Step != StepFind && r.Step != StepDelete {
		return true
	}
	return slices.ContainsFunc(r.Tables, func(t Table) bool { _, ok := t.Steps[StepDelete]; return ok })
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

// suspend stops the request's job until it is confirmed.
func (s *Service) suspend(ctx context.Context, id string) {
	if s.opts.Jobs == nil {
		return
	}
	if err := s.opts.Jobs.Store().SetState(context.WithoutCancel(ctx), JobKind, id, tasks.StateSuspended, time.Time{}); err != nil {
		s.log.Warn("could not suspend a held erasure request", "request", id, "err", err)
	}
}

func (m *Metrics) countFailed() {
	if m != nil {
		m.failed.Inc()
	}
}

func (m *Metrics) countHeld() {
	if m != nil {
		m.held.Inc()
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
