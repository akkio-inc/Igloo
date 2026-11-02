package pgwire

import (
	"errors"
	"os"
	"time"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the server's Prometheus metrics. A nil *Metrics records
// nothing, which keeps tests simple.
type Metrics struct {
	sessions *prometheus.GaugeVec
	queries  *prometheus.CounterVec
	duration *prometheus.HistogramVec
	logins   *prometheus.CounterVec
	reruns   prometheus.Counter
	// ids, filled and skipped count ID domains' assignments (see
	// idassign.go).
	ids, filled, skipped *prometheus.CounterVec
	// reports counts the report API's calls by how they were answered, and
	// recognized and fallbacks what recognizing reports did (see
	// reportapi.go, reportrecognize.go).
	reports               *prometheus.CounterVec
	recognized, fallbacks prometheus.Counter
	// indexStatements counts the statements whose counts in SQL read
	// bitmap indexes (see bitmaprewrite.go).
	indexStatements prometheus.Counter
	// memoryExits counts the times Igloo gave up on a statement that did
	// not stop when canceled for memory (see memexit.go).
	memoryExits prometheus.Counter
	// implicitRetries counts the statements in Snowflake's dialect that
	// bound only with its implicit conversions (see parseTranslated).
	implicitRetries prometheus.Counter
	// prepares counts the queries prepared under the shared catalog lock,
	// and those prepared again under the exclusive one (see
	// sharedprepare.go).
	prepares *prometheus.CounterVec
	// sortedWrites counts the writes of rows in order sorted a range at a
	// time (see sortedwrite.go).
	sortedWrites prometheus.Counter
	// largeSorts counts the reads of rows in order sorted in runs and
	// merged (see largesort.go).
	largeSorts prometheus.Counter
	// resultCache counts the result cache's lookups and the results it
	// kept, and resultBytes is the memory its results take (see
	// resultcache.go).
	resultCache *prometheus.CounterVec
	resultBytes prometheus.Gauge
	// processThreads is the threads of the process.
	processThreads prometheus.GaugeFunc
	// backgrounds counts the statements run in the second DuckDB (see
	// background.go).
	backgrounds prometheus.Counter
	// aggPasses counts the tables CREATE TABLE ... AS filled in passes
	// over their aggregate's groups (see aggpasses.go).
	aggPasses prometheus.Counter
}

// NewMetrics registers the server's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		sessions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "igloo_sessions",
			Help: "Open client sessions.",
		}, []string{"role"}),
		queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_queries_total",
			Help: "Statements run, by outcome: ok, error, denied or canceled.",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "igloo_query_duration_seconds",
			Help:    "Time to run a statement and send its results.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10), // 1ms to ~4.4min
		}, []string{"outcome"}),
		logins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_logins_total",
			Help: "Login attempts, by result: success or failure.",
		}, []string{"result"}),
		reruns: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_bitmap_reruns_total",
			Help: "Statements counting distinct IDs in roaring bitmaps that ran out of memory for them and ran again with DuckDB's hash.",
		}),
		ids: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_id_domain_ids_assigned_total",
			Help: "Ids ID domains gave new values, by domain.",
		}, []string{"domain"}),
		filled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_id_domain_rows_filled_total",
			Help: "Rows of member tables whose ids were filled in after they were written, by domain.",
		}, []string{"domain"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_id_domain_assignments_skipped_total",
			Help: "Assignments of ids left to maintenance, by reason: busy (another held the domain's lease), exhausted (every id below 2^32 handed out) or erased (an erasure deleted from the dictionary meanwhile).",
		}, []string{"reason"}),
		reports: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_report_calls_total",
			Help: "Report API calls prepared, by how they count: index (bitmap indexes), index+scan (people from bitmap indexes, the rest from a scan), ids (ID domains' ids) or scan (the tables' values).",
		}, []string{"path"}),
		recognized: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_report_ctes_recognized_total",
			Help: "Common table expressions of reports that IGLOO_REPORT_REWRITE answered through the report API.",
		}),
		fallbacks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_report_rewrite_fallbacks_total",
			Help: "Statements whose recognized reports failed to prepare, which were prepared as written instead.",
		}),
		indexStatements: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_bitmap_index_statements_total",
			Help: "Statements prepared whose counts of distinct members in SQL read bitmap indexes instead of the tables (the report API's calls count in igloo_report_calls_total).",
		}),
		memoryExits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_memory_exits_total",
			Help: "Times Igloo exited because a statement it canceled for running out of memory did not stop, or DuckDB went on with its work after it did, and memory was nearly gone.",
		}),
		implicitRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_snowflake_implicit_conversions_total",
			Help: "Statements in Snowflake's dialect that DuckDB could bind only with Snowflake's implicit conversions, which Igloo then made.",
		}),
		prepares: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_shared_prepares_total",
			Help: "Queries prepared alongside other statements, under the shared catalog lock (shared), and those that failed to prepare that way, or met a refused catalog listing meanwhile, and were prepared again under the exclusive lock (retried).",
		}, []string{"lock"}),
		largeSorts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_large_sorts_total",
			Help: "Reads of more rows in order than memory holds, sorted in runs and merged.",
		}),
		sortedWrites: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_sorted_writes_total",
			Help: "Writes of rows in order (CREATE TABLE ... AS and INSERT with ORDER BY) too large to sort at once within IGLOO_SORT_SPILL_BUDGET, which Igloo sorted a range of the order at a time.",
		}),
		resultCache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_result_cache_total",
			Help: "Queries the result cache answered (hit), answered with the result of the same query another session was running (joined) or could not answer (missed), and results it kept (kept).",
		}, []string{"outcome"}),
		resultBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_result_cache_bytes",
			Help: "Memory the results the result cache keeps take, as Igloo estimates it.",
		}),
		processThreads: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "igloo_process_threads",
			Help: "Threads of the process, Go's and DuckDB's; Go's own go_threads counts only Go's.",
		}, processThreads),
		backgrounds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_background_statements_total",
			Help: "Heavy statements run in the second DuckDB, whose threads give way to the first's.",
		}),
		aggPasses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_aggregate_passes_total",
			Help: "Tables CREATE TABLE ... AS filled in passes over its aggregate's groups, whose list, string or DISTINCT aggregates would not have fit in memory in one.",
		}),
	}
	reg.MustRegister(m.processThreads, m.sessions, m.queries, m.duration, m.logins, m.reruns, m.ids, m.filled, m.skipped, m.reports, m.recognized, m.fallbacks,
		m.indexStatements, m.memoryExits, m.implicitRetries, m.prepares, m.sortedWrites, m.largeSorts, m.resultCache, m.resultBytes, m.aggPasses, m.backgrounds)
	return m
}

func (m *Metrics) reportCall(path string) {
	if m != nil {
		m.reports.WithLabelValues(path).Inc()
	}
}

func (m *Metrics) reportsRecognized(n int) {
	if m != nil && n > 0 {
		m.recognized.Add(float64(n))
	}
}

func (m *Metrics) indexStatement() {
	if m != nil {
		m.indexStatements.Inc()
	}
}

func (m *Metrics) memoryExit() {
	if m != nil {
		m.memoryExits.Inc()
	}
}

func (m *Metrics) implicitRetry() {
	if m != nil {
		m.implicitRetries.Inc()
	}
}

func (m *Metrics) prepare(lock string) {
	if m != nil {
		m.prepares.WithLabelValues(lock).Inc()
	}
}

func (m *Metrics) sortedWrite() {
	if m != nil {
		m.sortedWrites.Inc()
	}
}

func (m *Metrics) largeSort() {
	if m != nil {
		m.largeSorts.Inc()
	}
}

// processThreads reads the threads of the process, 0 where /proc does not tell.
func processThreads() float64 {
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return 0
	}
	return float64(len(tasks))
}

func (m *Metrics) backgroundStatement() {
	if m != nil {
		m.backgrounds.Inc()
	}
}

func (m *Metrics) aggregatePasses() {
	if m != nil {
		m.aggPasses.Inc()
	}
}

func (m *Metrics) resultCached(outcome string) {
	if m != nil {
		m.resultCache.WithLabelValues(outcome).Inc()
	}
}

func (m *Metrics) resultCacheBytes(n int64) {
	if m != nil {
		m.resultBytes.Set(float64(n))
	}
}

func (m *Metrics) reportFallback() {
	if m != nil {
		m.fallbacks.Inc()
	}
}

func (m *Metrics) idsAssigned(domain string, n int64) {
	if m != nil && n > 0 {
		m.ids.WithLabelValues(domain).Add(float64(n))
	}
}

func (m *Metrics) idsFilled(domain string, n int64) {
	if m != nil && n > 0 {
		m.filled.WithLabelValues(domain).Add(float64(n))
	}
}

func (m *Metrics) idAssignSkipped(reason string) {
	if m != nil {
		m.skipped.WithLabelValues(reason).Inc()
	}
}

func (m *Metrics) sessionOpened(role string) {
	if m != nil {
		m.sessions.WithLabelValues(role).Inc()
	}
}

func (m *Metrics) sessionClosed(role string) {
	if m != nil {
		m.sessions.WithLabelValues(role).Dec()
	}
}

func (m *Metrics) login(ok bool) {
	if m == nil {
		return
	}
	result := "failure"
	if ok {
		result = "success"
	}
	m.logins.WithLabelValues(result).Inc()
}

func (m *Metrics) bitmapRerun() {
	if m != nil {
		m.reruns.Inc()
	}
}

func (m *Metrics) query(start time.Time, err error) {
	if m == nil {
		return
	}
	outcome := queryOutcome(err)
	m.queries.WithLabelValues(outcome).Inc()
	m.duration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

func queryOutcome(err error) string {
	// A client that stops reading rows early (the console showing the
	// first rows of a large result) got what it asked for.
	if err == nil || errors.Is(err, ErrStopRows) {
		return "ok"
	}
	switch pgerror.GetCode(pgError(err)) {
	case codes.InsufficientPrivilege:
		return "denied"
	case codes.QueryCanceled:
		return "canceled"
	}
	return "error"
}
