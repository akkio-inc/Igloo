package pgwire

import (
	"errors"
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
			Help: "Assignments of ids left to maintenance, by reason: busy (another held the domain's lease) or exhausted (every id below 2^32 handed out).",
		}, []string{"reason"}),
	}
	reg.MustRegister(m.sessions, m.queries, m.duration, m.logins, m.reruns, m.ids, m.filled, m.skipped)
	return m
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
