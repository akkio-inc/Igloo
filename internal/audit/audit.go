// Package audit keeps Igloo's query history and login history: every
// statement a session runs and every login attempt. Records are written in
// the background, in batches, so recording never slows a query down; when
// the queue is full or the store fails, records are dropped with a warning
// and a metric rather than failing or blocking anything.
package audit

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Statement outcomes, matching igloo_queries_total.
const (
	OutcomeOK       = "ok"
	OutcomeError    = "error"
	OutcomeDenied   = "denied"
	OutcomeCanceled = "canceled"
)

// Login results.
const (
	LoginSuccess = "success"
	LoginFailure = "failure"
)

// Query is one statement a session ran, or tried to.
type Query struct {
	ID        string
	SessionID string
	User      string
	// Role is the user's built-in role when the statement ran.
	Role     string
	Start    time.Time
	Duration time.Duration
	Outcome  string
	// SQLState is the error's SQLSTATE; empty when the statement succeeded.
	SQLState string
	// Rows is how many rows the statement returned or changed, or -1 when
	// that is not known.
	Rows          int64
	StatementType string
	// Text is the statement with passwords and secrets removed.
	Text string
	// TablesRead and TablesWritten are set when Igloo worked out which
	// tables the statement uses; nil otherwise.
	TablesRead    []string
	TablesWritten []string
	// LocationsRead are the locations outside the catalog that the
	// statement read files from, when known: COPY INTO's.
	LocationsRead   []string
	ClientAddress   string
	ApplicationName string
	// Policies names the masking and row access policies that applied to
	// the statement; nil when none did.
	Policies []string
	// QueryTag is the tag the statement ran under (SET query_tag or a
	// default its user's roles set), if any.
	QueryTag string
	// Pool is the compute pool of the replica that ran it.
	Pool string
	// Queued is how long it waited for its role's limits before starting.
	Queued time.Duration
	// Resources is what DuckDB measured while it ran; nil when it did not.
	Resources *Resources
	// Job is the task, pipe or materialized view the statement ran for,
	// such as "task nightly"; empty for a session's own statements.
	Job string
}

// Resources is what DuckDB's profiler measured while a statement ran.
type Resources struct {
	// CPUTime is the time its operators ran, summed over DuckDB's threads.
	CPUTime time.Duration
	// BytesRead is what DuckDB counted of the statement's file reads.
	BytesRead int64
	// MemoryAllocated is the memory DuckDB's buffer pool allocated for it
	// over its whole run: memory freed and allocated again counts again.
	MemoryAllocated int64
}

// Login is one login attempt.
type Login struct {
	ID              string
	Time            time.Time
	User            string
	Result          string
	Reason          string
	ClientAddress   string
	ApplicationName string
	// SessionID is the session a successful login opened.
	SessionID string
	// Method is how the user signed in, or tried to: one of the
	// Method constants, or empty when that is not known.
	Method string
	// Token names the access token a login used.
	Token string
}

// Login methods.
const (
	MethodPassword = "password"
	MethodToken    = "token"
	MethodSSO      = "sso"
	// MethodAccessToken is an identity provider's access token sent as a
	// SQL password.
	MethodAccessToken = "access_token"
)

// Viewer is who reads the history: admins see every record, everyone else
// their own.
type Viewer struct {
	User string
	All  bool
}

// Page selects records, newest first.
type Page struct {
	// Since and Until bound the records' times, if set: Since <= t < Until.
	Since, Until time.Time
	// AfterTime and AfterID continue a listing: only records that sort
	// after them (older, or as old with a smaller ID) are returned.
	AfterTime time.Time
	AfterID   string
	Limit     int
}

// Store persists and reads back records. Implementations must be safe for
// concurrent use.
type Store interface {
	WriteQueries(ctx context.Context, qs []Query) error
	WriteLogins(ctx context.Context, ls []Login) error
	// Queries and Logins return what v may see, newest first.
	Queries(ctx context.Context, v Viewer, p Page) ([]Query, error)
	Logins(ctx context.Context, v Viewer, p Page) ([]Login, error)
	// Lineage returns the edges that successful statements since since
	// made, and TableUsage how often they read each table and changed its
	// data (see lineage.go). Both cover every user's statements.
	Lineage(ctx context.Context, since time.Time) ([]Edge, error)
	TableUsage(ctx context.Context, since time.Time) ([]TableUsage, error)
	// Prune deletes records older than before. Stores shared by several
	// replicas let one of them do it at a time.
	Prune(ctx context.Context, before time.Time) error
	// WriteCompute adds to the time replicas ran (see usage.go).
	WriteCompute(ctx context.Context, cs []Compute) error
	// Usage sums the statements v may see by day, pool, user, role and
	// query tag, from since (inclusive) to until, either of which may be
	// zero.
	Usage(ctx context.Context, v Viewer, since, until time.Time) ([]Usage, error)
	Close()
}

const (
	// queueSize bounds the records waiting to be written. Beyond it,
	// records are dropped.
	queueSize = 10_000
	// batchSize and flushInterval decide when a batch is written.
	batchSize     = 1_000
	flushInterval = time.Second
	// writeTimeout bounds one batch write.
	writeTimeout = 10 * time.Second
	// writeAttempts is how often a batch is written before it is dropped,
	// retryDelay doubling between attempts (about a minute in all), so a
	// slow or briefly unavailable metadata database loses nothing.
	writeAttempts = 6
	retryDelay    = time.Second
	// pruneInterval is how often old records are deleted.
	pruneInterval = time.Hour
	// warnInterval spaces out warnings about dropped records.
	warnInterval = time.Minute
)

// Log records history into a Store. A nil *Log records nothing.
type Log struct {
	store     Store
	retention time.Duration
	metrics   *Metrics
	log       *slog.Logger
	// direct stores are written synchronously: they are in memory and
	// fast, and their readers then see a statement as soon as it ends.
	direct bool
	queue  chan any

	// compute records how long this replica runs, for the cost estimates
	// of igloo.usage; see usage.go.
	compute computeClock

	dropMu   sync.Mutex
	dropped  int
	lastWarn time.Time

	// retries are batches whose write failed, oldest first. Only Run
	// touches them.
	retries []retry

	tableUsage usageCache
}

// retry is a batch of one kind waiting to be written again.
type retry struct {
	queries  []Query
	logins   []Login
	attempts int
	next     time.Time
}

func (r retry) kind() string {
	if len(r.queries) > 0 {
		return "query"
	}
	return "login"
}

func (r retry) len() int { return len(r.queries) + len(r.logins) }

// Options configures a Log.
type Options struct {
	// Retention is how long records are kept.
	Retention time.Duration
	// Metrics may be nil.
	Metrics *Metrics
	// Pool is the compute pool this replica belongs to, and PricePerHour
	// what its machine costs an hour, or zero if unknown.
	Pool         string
	PricePerHour float64
}

// New records into store. Run must be running for records to reach a
// store other than a MemoryStore.
func New(store Store, opts Options, log *slog.Logger) *Log {
	_, direct := store.(*MemoryStore)
	return &Log{
		store:     store,
		retention: opts.Retention,
		metrics:   opts.Metrics,
		log:       log.With("component", "audit"),
		direct:    direct,
		queue:     make(chan any, queueSize),
		compute:   newComputeClock(opts.Pool, opts.PricePerHour),
	}
}

// Immediate reports whether records are written as they come, so that
// readers see a statement as soon as it ends: true for a MemoryStore.
func (l *Log) Immediate() bool { return l != nil && l.direct }

// RecordQuery queues q for writing. It never blocks.
func (l *Log) RecordQuery(q Query) {
	if l == nil {
		return
	}
	if q.ID == "" {
		q.ID = NewID(q.Start)
	}
	l.enqueue(q)
}

// RecordLogin queues x for writing. It never blocks.
func (l *Log) RecordLogin(x Login) {
	if l == nil {
		return
	}
	if x.ID == "" {
		x.ID = NewID(x.Time)
	}
	l.enqueue(x)
}

func (l *Log) enqueue(r any) {
	if l.direct {
		l.write(context.Background(), []any{r})
		return
	}
	select {
	case l.queue <- r:
	default:
		l.drop(kindOf(r), 1, "queue full")
	}
}

// Queries returns the query history v may see.
func (l *Log) Queries(ctx context.Context, v Viewer, p Page) ([]Query, error) {
	if l == nil {
		return nil, nil
	}
	return l.store.Queries(ctx, v, p)
}

// Logins returns the login history v may see.
func (l *Log) Logins(ctx context.Context, v Viewer, p Page) ([]Login, error) {
	if l == nil {
		return nil, nil
	}
	return l.store.Logins(ctx, v, p)
}

// Run writes queued records in batches and deletes ones older than the
// retention, until ctx is canceled. It then writes what is still queued.
func (l *Log) Run(ctx context.Context) {
	if l == nil {
		return
	}
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()
	// The first prune comes soon after a start, at a random point, so
	// replicas starting together do not all try at once.
	prune := time.NewTimer(time.Minute + rand.N(time.Minute))
	defer prune.Stop()
	compute := time.NewTicker(computeInterval)
	defer compute.Stop()

	var batch []any
	write := func() {
		if len(batch) > 0 {
			l.write(context.Background(), batch)
			batch = batch[:0]
		}
		l.retryDue(context.Background(), time.Now(), false)
	}
	for {
		select {
		case r := <-l.queue:
			batch = append(batch, r)
			if len(batch) >= batchSize {
				write()
			}
		case <-flush.C:
			write()
		case <-prune.C:
			l.prune(ctx)
			prune.Reset(pruneInterval)
		case <-compute.C:
			l.writeCompute(time.Now())
		case <-ctx.Done():
			for drained := false; !drained; {
				select {
				case r := <-l.queue:
					batch = append(batch, r)
				default:
					drained = true
				}
			}
			write()
			l.retryDue(context.Background(), time.Now(), true)
			l.writeCompute(time.Now())
			return
		}
	}
}

// write stores a batch of queries and logins. A part that fails is retried
// by Run, or dropped for a store written directly.
func (l *Log) write(ctx context.Context, batch []any) {
	var (
		queries []Query
		logins  []Login
	)
	for _, r := range batch {
		switch r := r.(type) {
		case Query:
			queries = append(queries, r)
		case Login:
			logins = append(logins, r)
		}
	}
	if len(queries) > 0 {
		l.attempt(ctx, retry{queries: queries}, time.Now())
	}
	if len(logins) > 0 {
		l.attempt(ctx, retry{logins: logins}, time.Now())
	}
}

// attempt writes r once. On failure it keeps r for a later attempt, or
// drops it once it has had writeAttempts.
func (l *Log) attempt(ctx context.Context, r retry, now time.Time) {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	var err error
	if len(r.queries) > 0 {
		err = l.store.WriteQueries(ctx, r.queries)
	} else {
		err = l.store.WriteLogins(ctx, r.logins)
	}
	if err == nil {
		l.metrics.written(r.kind(), r.len())
		return
	}
	r.attempts++
	if l.direct || r.attempts >= writeAttempts {
		l.drop(r.kind(), r.len(), err.Error())
		return
	}
	r.next = now.Add(retryDelay << (r.attempts - 1))
	l.retries = append(l.retries, r)
	// Keep the backlog within the queue's size, dropping the oldest.
	for total := l.backlog(); total > queueSize && len(l.retries) > 0; total = l.backlog() {
		l.drop(l.retries[0].kind(), l.retries[0].len(), "retry backlog full")
		l.retries = l.retries[1:]
	}
}

func (l *Log) backlog() int {
	n := 0
	for _, r := range l.retries {
		n += r.len()
	}
	return n
}

// retryDue writes the batches due for another attempt, or every waiting
// batch once more when last is set (Igloo is stopping), dropping what still
// fails then.
func (l *Log) retryDue(ctx context.Context, now time.Time, last bool) {
	if len(l.retries) == 0 {
		return
	}
	waiting := l.retries
	l.retries = nil
	for _, r := range waiting {
		switch {
		case last:
			r.attempts = writeAttempts - 1
			l.attempt(ctx, r, now)
		case now.Before(r.next):
			l.retries = append(l.retries, r)
		default:
			l.attempt(ctx, r, now)
		}
	}
}

func (l *Log) prune(ctx context.Context) {
	if l.retention <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := l.store.Prune(ctx, time.Now().Add(-l.retention)); err != nil && ctx.Err() == nil {
		l.log.Warn("could not delete old history", "err", err)
	}
}

// drop counts dropped records and warns about them, at most once per
// warnInterval.
func (l *Log) drop(kind string, n int, reason string) {
	l.metrics.dropped(kind, n)
	l.dropMu.Lock()
	defer l.dropMu.Unlock()
	l.dropped += n
	if time.Since(l.lastWarn) < warnInterval {
		return
	}
	l.log.Warn("dropped history records", "count", l.dropped, "reason", reason)
	l.dropped, l.lastWarn = 0, time.Now()
}

func kindOf(r any) string {
	if _, ok := r.(Login); ok {
		return "login"
	}
	return "query"
}

// NewID returns a random UUID (version 7) whose first bits are the time t,
// so IDs sort roughly by time.
func NewID(t time.Time) string {
	var b [16]byte
	_, _ = cryptoRead(b[:])
	ms := uint64(t.UnixMilli())
	binary.BigEndian.PutUint16(b[0:2], uint16(ms>>32))
	binary.BigEndian.PutUint32(b[2:6], uint32(ms))
	b[6] = 0x70 | b[6]&0x0f
	b[8] = 0x80 | b[8]&0x3f
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

var cryptoRead = crand.Read
