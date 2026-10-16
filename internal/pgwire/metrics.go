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
	}
	reg.MustRegister(m.sessions, m.queries, m.duration, m.logins)
	return m
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
