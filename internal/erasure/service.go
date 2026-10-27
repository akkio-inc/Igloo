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
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/deletefiles"
	"github.com/akkio-inc/igloo/internal/iddomain"
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

// Derived keeps what was derived from tables' rows and kept apart from
// them, tagged with the tables each entry was derived from: the AI
// functions' cache (package ai), whose answers can hold what a provider
// read in the rows, as a summary of a person's notes. Coarse: every entry
// derived from a table a request erases from goes, the person's or not.
type Derived interface {
	// Purge deletes what was derived from any of tables, or from what
	// Igloo could not name, and keeps statements that began before it,
	// which may have read the person's rows, from keeping anything more;
	// it returns how many entries it deleted.
	Purge(ctx context.Context, tables []string) (int64, error)
	// Remaining counts what is derived from any of tables by statements
	// that began before the last purge.
	Remaining(ctx context.Context, tables []string) (int64, error)
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
	// Storage, if set, opens the object store that holds a location in
	// the warehouse with Igloo's own credentials, which a request reaches
	// files through when none of its tables exists any more, as when
	// every one was dropped.
	Storage func(ctx context.Context, location string) (iceio.IO, error)
	// Locations, if set, returns the locations of every table and view of
	// the catalog, which own the files under them: a request deletes the
	// warehouse's files under none (see unowned). It must fail if one
	// cannot be loaded. Without it, the tables' locations stand in.
	Locations func(ctx context.Context) ([]string, error)
	// Jobs runs requests. Without it they run only when Run is called.
	Jobs *tasks.Scheduler
	// History are the stores whose records requests redact; nil ones are
	// left out.
	History []History
	// Derived are what requests purge of what was derived from their
	// tables, in their history step, once no snapshot holds the person's
	// rows; nil ones are left out.
	Derived []Derived
	// Models, if set, are the models trained from tables' rows, which
	// requests train again (see Models).
	Models Models
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
	Rows Rows
	// IDDomains keeps ID domains' leases, which a request holds while it
	// deletes from a domain's dictionary for the last time (see
	// deleteRows). Nil means no domains.
	IDDomains iddomain.Store
	// IDSets are what Igloo derives from ID domains' ids, such as bitmap
	// indexes, which a request has erase the ids it erases (see IDSets).
	// Nil means none.
	IDSets  IDSets
	Metrics *Metrics
	// Memory is what rewriting a table's files may hold at once (see
	// package filerewrite), as compaction's IGLOO_MAINTENANCE_MEMORY; zero
	// means filerewrite.DefaultMemory.
	Memory int64
	// Failed, if set, is told about a request that failed for good, which
	// someone must make again: its ID, the step it stopped at, and why,
	// without identifiers. It must not block.
	Failed func(id, step, err string)
	// Blocked, if set, is told the same about a request that has begun
	// deleting and cannot go on until someone fixes what err says, each
	// time it tries again. It must not block.
	Blocked func(id, step, err string)
	Log     *slog.Logger
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
	failed, held, blocked prometheus.Counter
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
		blocked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_erasure_requests_blocked_runs_total",
			Help: "Runs of erasure requests that had begun deleting and could not go on until someone fixes what their error says.",
		}),
	}
	reg.MustRegister(m.failed, m.held, m.blocked)
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
		if err := s.erasable(ctx, t, tbl); err != nil {
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

// Erasing reports whether a request that is not over (not completed,
// canceled or failed) erases from the catalog's table schema.name, as its
// find step found it: tagged, or sharing files with one that is. Such a
// table must stay one the request can rewrite (see erasable): Igloo's SQL
// and its Iceberg REST endpoint refuse to upgrade its format version.
func (s *Service) Erasing(ctx context.Context, schema, name string) (bool, error) {
	all, err := s.opts.Store.List(ctx)
	if err != nil {
		return false, err
	}
	want := schema + "." + name
	for _, r := range all {
		if !r.open() {
			continue
		}
		for i := range r.Tables {
			if t := &r.Tables[i]; !t.Remote && strings.EqualFold(tableName(identOf(t)), want) {
				return true, nil
			}
		}
	}
	return false, nil
}

// ErasingIn reports whether a request that is not over erases from a
// table of the catalog's schema, which may then not be dropped.
func (s *Service) ErasingIn(ctx context.Context, schema string) (bool, error) {
	all, err := s.opts.Store.List(ctx)
	if err != nil {
		return false, err
	}
	for _, r := range all {
		if !r.open() {
			continue
		}
		for i := range r.Tables {
			t := &r.Tables[i]
			if ident := identOf(t); !t.Remote && len(ident) > 1 && strings.EqualFold(strings.Join(ident[:len(ident)-1], "."), schema) {
				return true, nil
			}
		}
	}
	return false, nil
}

// open reports whether r is not over: not completed, canceled, failed or
// closed.
func (r *Request) open() bool {
	switch r.Status {
	case StatusCompleted, StatusCanceled, StatusFailed, StatusClosed:
		return false
	}
	return true
}

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
	case StatusCompleted, StatusCanceled, StatusFailed, StatusClosed:
		s.unschedule(ctx, id)
		return nil
	case StatusHeld:
		s.suspend(ctx, id)
		return nil
	case StatusBlocked:
		r.takeUp()
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
		s.wake(ctx, id, r.nextRun())
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

// contentionError is a step that cannot go on now for reasons outside the
// request: it lost to other writers' commits more often than it tries in
// one run, or the catalog could not answer. It tries again later, which
// does not count as a failure, however long it takes.
type contentionError struct{ error }

func (e contentionError) Unwrap() error { return e.error }

func contended(err error) error { return contentionError{err} }

func unavailable(err error) error { return contentionError{err} }

const (
	// contentionWait is how long a request that lost to other writers
	// waits before it tries again.
	contentionWait = time.Minute
	// keyWait is how long a request whose identifiers this replica cannot
	// unseal waits: until a replica with the right key runs it.
	keyWait = 10 * time.Minute
	// blockedWait is how often a blocked request tries again.
	blockedWait = time.Hour
)

// failed records that a run of r failed, and has it run again later, or,
// after too many failures or one it cannot get past, fail for good if it
// has not sent a DELETE yet, and be blocked if it has. A run that lost to
// other writers, or on a replica whose erasure key is not the one r's
// identifiers were sealed under, does not count: neither says anything is
// wrong with the request, and failing it would forget its identifiers for
// good.
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
		s.log.Warn("erasure request cannot go on now: other writers' commits came first, or the catalog could not answer; it tries again",
			"request", r.ID, "step", r.Step, "in", contentionWait, "err", r.Error)
		s.wake(ctx, r.ID, r.WaitUntil)
		return
	}
	var t terminalError
	var te *tableError
	if deleting(r) && errors.As(err, &te) {
		// A run that failed with one table counts against that table
		// alone: once its runs have failed as often as a request's may, it
		// is set aside (see setAside), and the others go on at once.
		if i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == te.table && t.active() }); i >= 0 {
			tb := &r.Tables[i]
			tb.Failures++
			wait := min(time.Minute<<(tb.Failures-1), maxBackoff)
			if errors.As(err, &t) || tb.Failures >= maxFailures {
				if serr := s.setAside(ctx, r, tb, te.err); serr != nil {
					s.log.Warn("could not save an erasure request", "request", r.ID, "err", serr)
				}
				tb.Failures, wait = 0, contentionWait
			}
			r.Status, r.WaitUntil = StatusRetrying, s.now().Add(wait)
			s.log.Warn("erasure request failed with a table; it runs again later", "request", r.ID, "table", tb.Name, "step", r.Step,
				"failures", tb.Failures, "in", wait, "err", r.Error)
			s.wake(ctx, r.ID, r.WaitUntil)
			return
		}
	}
	r.Failures++
	final := errors.As(err, &t) || r.Failures >= maxFailures
	if final && deleting(r) {
		// Once it sent a DELETE, a request never fails: failing would
		// forget whose rows it deleted while their files may still hold
		// them. What stops it now arose since its checks before each
		// DELETE, as another engine changing a table, or is for someone
		// to fix: it waits, alerts, and goes on once that is done.
		r.Status, r.WaitUntil = StatusBlocked, s.now().Add(blockedWait)
		s.opts.Metrics.countBlocked()
		if s.opts.Blocked != nil {
			s.opts.Blocked(r.ID, r.Step, r.Error)
		}
		s.log.Error("erasure request cannot go on until what its error says is fixed; it has begun deleting, so it keeps its identifiers "+
			"and tries again", "request", r.ID, "step", r.Step, "failures", r.Failures, "in", blockedWait, "err", r.Error)
		s.wake(ctx, r.ID, r.WaitUntil)
		return
	}
	if final {
		r.Status = StatusFailed
		s.dropBranches(ctx, r)
		s.end(ctx, r)
		s.opts.Metrics.countFailed()
		if s.opts.Failed != nil {
			s.opts.Failed(r.ID, r.Step, r.Error)
		}
		s.log.Error("erasure request failed before it deleted anything, and will not go on: make a new request once the cause is fixed",
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
	r.domainIDs = nil
	if r.SealedIDs != "" {
		if err := s.sealer.unsealJSON(domainIDsAAD(r.ID), r.SealedIDs, &r.domainIDs); err != nil {
			return err
		}
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
	r.Salt, r.Sealed, r.salt, r.SealedIDs, r.domainIDs = "", "", "", "", nil
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
		if aside := r.setAside(); next == StepDone && len(aside) > 0 {
			// The other tables went through every step; the request is
			// blocked, and takes those set aside up again (see takeUp).
			r.ran(step, started, s.now())
			r.Step = r.resume()
			return terminal(fmt.Errorf("the request went through every step with its other tables, but set aside %s; it takes them up again "+
				"every hour, and an admin can close it (CLOSE ERASURE REQUEST) if what stops it will not be fixed", strings.Join(aside, "; ")))
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
	case StepModels:
		err = s.retrainModels(ctx, r)
	case StepCache:
		err = s.cache(ctx, r)
	case StepVerify:
		return s.verify(ctx, r)
	default:
		err = fmt.Errorf("unknown step %q", r.Step)
	}
	return next, err
}

// Close ends a blocked request without finishing it: the last resort, for
// an admin who decides that what blocks it will not be fixed, as a table
// another engine upgraded to format version 3, and that keeping the
// person's identifiers sealed for it serves no one.
//
// Before it forgets the identifiers, it does what needs no table: it
// deletes the sharing copies that hold the person's rows, redacts the
// identifiers from the history, and has every replica empty its read
// cache of what the request deleted; and it deletes the blobs the models
// it trained again had. If one of those cannot be done now, it is not
// closed, and the error says why. The models trained from its tables that
// it did not train again stay, and it records them (ModelsLeft), with why. The files the request
// recorded of a table set aside that was dropped since go, unless a table
// references them; each table it did not finish with records why, and
// the files it leaves that may hold the person's rows (Left), with the
// identifiers scrubbed from their paths. What it erased stays erased; who
// closed it is recorded; it forgets its identifiers, as a completed
// request does, and the log says what it left. Its callers allow admins
// only; requesters and holders of ERASE cannot close a request, as they
// cannot cancel one that has begun deleting.
func (s *Service) Close(ctx context.Context, id, by string) (*Request, error) {
	return s.change(ctx, id, func(r *Request) error {
		if r.Status != StatusBlocked {
			return fmt.Errorf("%w: request %s is %s; only a blocked request can be closed", ErrInvalid, id, r.Status)
		}
		why := r.Error
		if err := s.open(ctx, r); err != nil {
			return fmt.Errorf("%w: %v", ErrNotYet, err)
		}
		// What it deletes here, replicas empty their caches of too (see
		// finish). Refused, the request keeps what it did meanwhile.
		files, err := s.leave(ctx, r)
		if err == nil {
			err = s.finish(ctx, r)
		}
		var models, workers []string
		if err == nil {
			models, err = s.leaveModels(ctx, r)
		}
		if err == nil {
			workers, err = s.workersLeft(ctx, r)
		}
		if err != nil {
			err = fmt.Errorf("%w: %s; it can be closed once that is done", ErrNotYet, scrub(r, err.Error()))
			return errors.Join(err, s.save(ctx, r))
		}
		var left []string
		for i := range r.Tables {
			t := &r.Tables[i]
			var why2 string
			switch {
			case t.Remote:
				continue
			case t.Blocked != "":
				why2 = t.Blocked
			case t.Gone && len(files[t.Name]) == 0:
				continue
			case t.Gone:
				why2 = "it was dropped while the request ran"
			case t.Steps[StepVerify].IsZero():
				why2 = fmt.Sprintf("the request was closed at its %s step before it finished with the table: %s", r.Step, why)
			case len(files[t.Name]) == 0:
				continue
			default:
				why2 = "the request finished with the table, but not with files it recorded"
			}
			if fs := files[t.Name]; len(fs) > 0 {
				why2 += fmt.Sprintf("; it leaves %d files that may hold the person's rows: %s", len(fs), strings.Join(fs, ", "))
			}
			t.Left = scrub(r, why2)
			left = append(left, t.Name)
		}
		r.Status, r.ClosedBy, r.Completed = StatusClosed, by, s.now()
		r.Error = "closed by " + by + " while blocked: " + why
		if len(models) > 0 {
			// Models it did not train again, which may hold what they
			// learned of the person's rows: why, if it tried.
			r.ModelsLeft = nil
			for _, m := range models {
				w := m + ": not trained again since its tables' rows were erased"
				for _, f := range r.ModelsFailed {
					if strings.HasPrefix(f, m+": ") {
						w = f
					}
				}
				r.ModelsLeft = append(r.ModelsLeft, scrub(r, w))
			}
			r.Error += "; it leaves " + strconv.Itoa(len(models)) + " models trained from its tables, which may hold what they learned of " +
				"the person: " + strings.Join(r.ModelsLeft, "; ")
		}
		if len(workers) > 0 {
			// Each purges before it serves again, but its disk holds what
			// it read until then.
			r.WorkersLeft = workers
			r.Error += "; it leaves " + strconv.Itoa(len(workers)) + " workers that do not run and had not purged what it deleted from " +
				"their read caches, which they do when they start again: " + strings.Join(workers, "; ")
		}
		if fs := files[""]; len(fs) > 0 {
			// Under no table's location: the request records them itself,
			// the first maxListed by name. A later request deletes them
			// once three days old.
			shown, more := fs, ""
			if len(fs) > maxListed {
				shown, more = fs[:maxListed], fmt.Sprintf(" and %d more", len(fs)-maxListed)
			}
			r.Error += scrub(r, fmt.Sprintf("; it leaves %d recent files in the warehouse under no table's location, which may hold the person's rows: %s%s",
				len(fs), strings.Join(shown, ", "), more))
		}
		s.dropBranches(ctx, r)
		s.end(ctx, r)
		s.unschedule(ctx, id)
		s.log.Warn("erasure request closed while blocked: it forgot its identifiers, and what it did not erase stays",
			"request", id, "by", by, "step", r.Step, "tables_left", strings.Join(left, ", "), "models_left", strings.Join(models, ", "),
			"workers_left", strings.Join(workers, "; "), "err", why)
		return nil
	})
}

// maxListed bounds how many files under no table's location a closed
// request names (see Close).
const maxListed = 100

// finish runs, for a request that is to be closed, the steps that need no
// table: sharing, history and cache. It fails if one cannot be done now,
// as when a replica has yet to empty its read cache.
func (s *Service) finish(ctx context.Context, r *Request) error {
	if err := s.sharing(ctx, r); err != nil {
		return fmt.Errorf("the request could not delete the Delta Sharing copies that hold the person's rows: %w", err)
	}
	if err := s.history(ctx, r); err != nil {
		return fmt.Errorf("the request could not redact the identifiers from the history: %w", err)
	}
	status, until := r.Status, r.WaitUntil
	err := s.cache(ctx, r)
	waiting := r.Status == StatusWaiting
	r.Status, r.WaitUntil = status, until
	var t terminalError
	switch {
	case err != nil && errors.As(err, &t):
		// Workers that do not run and have not purged stay as they are;
		// Close records them.
	case err != nil:
		return fmt.Errorf("the request could not have the replicas empty their read caches: %w", err)
	case waiting:
		return errors.New("replicas or workers have yet to empty their read caches of the files the request deleted; try again in a minute")
	}
	return nil
}

// leave settles, for a request that is to be closed, the files it
// recorded and has not deleted, and the warehouse's files under no table's
// or view's location (see unowned). A table set aside that was dropped
// since, and not renamed, holds nothing any more: the files recorded for
// it go, unless a table references them, as purge would delete them; so
// do the old files under no table's location. It returns, by table, the
// paths of the files left that may hold the person's rows: those recorded
// and not deleted, and, under "", the recent ones under no table's
// location, which purge would wait for.
func (s *Service) leave(ctx context.Context, r *Request) (map[string][]string, error) {
	for i := range r.Tables {
		t := &r.Tables[i]
		if t.Remote || t.Blocked == "" || t.UUID == "" {
			continue
		}
		_, err := s.opts.Catalog.LoadTable(ctx, identOf(t))
		if !errors.Is(err, icecat.ErrNoSuchTable) {
			continue
		}
		ident, err := s.renamedTo(ctx, t.UUID)
		if err != nil {
			return nil, err
		}
		if ident == nil {
			s.log.Info("an erasure request being closed found a table it set aside dropped; it deletes the files it recorded for it",
				"request", r.ID, "table", t.Name)
			t.Blocked, t.BlockedAt, t.Gone = "", "", true
		}
	}
	files, err := s.opts.Store.Files(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	recorded := func() {
		for _, f := range files {
			if f.Deleted.IsZero() && f.Kind != fileOutside {
				out[f.Table] = append(out[f.Table], f.Path)
			}
		}
	}
	var c contentionError
	tables, referenced, kept, err := s.referenced(ctx, r)
	switch {
	case errors.As(err, &c):
		return nil, err
	case err != nil:
		// Which files a table references cannot be told: none is deleted,
		// and every one recorded, and any under no table's location, is
		// left.
		s.log.Warn("an erasure request being closed could not read its tables; it lists every file it did not delete as left",
			"request", r.ID, "err", scrub(r, err.Error()))
		recorded()
		out[""] = append(out[""], "any in the warehouse under no table's location")
		return out, nil
	}
	var gone []File
	going := pathSet{}
	for _, f := range files {
		i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == f.Table })
		if f.Deleted.IsZero() && f.Kind != fileOutside && i >= 0 && r.Tables[i].Gone && !referenced.has(f.Path) {
			gone = append(gone, f)
			going.add(f.Path)
		}
	}
	u := &unownedFiles{young: map[string]time.Time{}}
	sw, err := s.unowned(ctx, s.newFileIO(tables), referenced)
	if err == nil {
		u = sw.files(going)
	}
	switch {
	case errors.As(err, &c):
		return nil, err
	case err != nil:
		s.log.Warn("an erasure request being closed could not sweep the warehouse; it lists the files under no table's location as left",
			"request", r.ID, "err", scrub(r, err.Error()))
		out[""] = append(out[""], fmt.Sprintf("any in the warehouse under no table's location (%v)", err))
		u = &unownedFiles{young: map[string]time.Time{}}
	}
	check := pathSet{}
	for _, f := range gone {
		check.add(f.Path)
	}
	for _, p := range u.all() {
		check.add(p)
	}
	if len(check) > 0 {
		release, err := s.lockLocations(ctx, r)
		if err != nil {
			return nil, err
		}
		refs, more, err := s.othersReference(ctx, r, tables, check)
		if err == nil {
			kept = append(kept, more...)
			// Files another table references stay, recorded: they are
			// listed as left.
			gone = slices.DeleteFunc(gone, func(f File) bool { _, ok := refs[canonical(f.Path)]; return ok })
			u.leave(refs, kept)
			for _, p := range u.old {
				gone = append(gone, File{Path: p, Kind: fileUnowned})
			}
			err = s.delete(ctx, r, tables, keep(gone, kept))
		}
		release()
		if err != nil {
			s.log.Warn("an erasure request being closed could not delete the files it found no table needs; it lists them as left",
				"request", r.ID, "err", scrub(r, err.Error()))
		}
		if files, err = s.opts.Store.Files(ctx, r.ID); err != nil {
			return nil, err
		}
	}
	recorded()
	out[""] = append(out[""], slices.Sorted(maps.Keys(u.young))...)
	return out, nil
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
	if !r.open() {
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

// setAside describes the tables r set aside (see Service.block): each
// one's name, the step it stopped at and why.
func (r *Request) setAside() []string {
	var out []string
	for _, t := range r.Tables {
		if t.Blocked != "" {
			out = append(out, fmt.Sprintf("%s (at its %s step: %s)", t.Name, t.BlockedAt, t.Blocked))
		}
	}
	return out
}

// resume is the step r goes on from to take up the tables it set aside:
// the earliest one of them stopped at, or r's own step if that is earlier.
func (r *Request) resume() string {
	at := slices.Index(steps, r.Step)
	for _, t := range r.Tables {
		if j := slices.Index(steps, t.BlockedAt); t.Blocked != "" && j >= 0 && (at < 0 || j < at) {
			at = j
		}
	}
	if at < 0 {
		return r.Step
	}
	return steps[at]
}

// takeUp has a blocked request take up the tables it set aside again,
// from the step it goes on from (see resume), every blockedWait, so that
// it goes on once someone fixed what blocked them. They get one try each
// time: one that fails again is set aside again at once (see failed); the
// others' failures count as before. Every table goes through the steps
// from there again (see redo).
func (r *Request) takeUp() {
	r.Step = r.resume()
	for i := range r.Tables {
		t := &r.Tables[i]
		if t.Blocked != "" {
			t.Failures = maxFailures - 1
		}
		t.Blocked, t.BlockedAt = "", ""
	}
	r.redo(r.Step)
}

// redo forgets that r's tables went through step and the steps after it,
// which they are to go through again: a table's verify step then says it
// was verified since.
func (r *Request) redo(step string) {
	at := slices.Index(steps, step)
	if at < 0 {
		return
	}
	if at <= slices.Index(steps, StepPurge) {
		// Rows or files of its tables go again: models trained since the
		// models step moved the epoch on may have read them, and it moves
		// it on again.
		r.ModelEpoch = 0
	}
	for i := range r.Tables {
		for _, s := range steps[at:] {
			delete(r.Tables[i].Steps, s)
		}
	}
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
	t.Failures = 0
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

func (m *Metrics) countBlocked() {
	if m != nil {
		m.blocked.Inc()
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
