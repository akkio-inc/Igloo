package gateway

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the gateway's Prometheus metrics. A nil *Metrics records
// nothing.
type Metrics struct {
	running     prometheus.Gauge
	clients     prometheus.Gauge
	clientTotal prometheus.Counter
	resumes     *prometheus.CounterVec
	resumeTime  prometheus.Histogram
	suspends    prometheus.Counter
	nextDue     prometheus.Gauge
}

// NewMetrics registers the gateway's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		running: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_gateway_compute_running",
			Help: "1 while the gateway knows Igloo's compute to be running, otherwise 0.",
		}),
		clients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_gateway_connections",
			Help: "Open client connections.",
		}),
		clientTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_gateway_connections_total",
			Help: "Client connections accepted.",
		}),
		resumes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_gateway_resumes_total",
			Help: "Starts of Igloo's compute, by result: success or failure.",
		}, []string{"result"}),
		resumeTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "igloo_gateway_resume_seconds",
			Help:    "Time from asking the compute to start until Igloo accepted connections.",
			Buckets: prometheus.ExponentialBuckets(1, 1.6, 12), // 1s to ~3min
		}),
		suspends: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_gateway_suspends_total",
			Help: "Times Igloo's compute was stopped for being idle.",
		}),
		nextDue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_gateway_next_due_timestamp_seconds",
			Help: "When Igloo last said its scheduled work is next due, which the gateway starts it for; 0 if none is, or it is not known.",
		}),
	}
	reg.MustRegister(m.running, m.clients, m.clientTotal, m.resumes, m.resumeTime, m.suspends, m.nextDue)
	return m
}

func (m *Metrics) setNextDue(due time.Time) {
	if m == nil {
		return
	}
	if due.IsZero() {
		m.nextDue.Set(0)
	} else {
		m.nextDue.Set(float64(due.UnixMilli()) / 1000)
	}
}

func (m *Metrics) setRunning(running bool) {
	if m == nil {
		return
	}
	if running {
		m.running.Set(1)
	} else {
		m.running.Set(0)
	}
}

func (m *Metrics) connections(open int, accepted bool) {
	if m == nil {
		return
	}
	m.clients.Set(float64(open))
	if accepted {
		m.clientTotal.Inc()
	}
}

func (m *Metrics) resumed(ok bool, elapsed time.Duration) {
	if m == nil {
		return
	}
	if !ok {
		m.resumes.WithLabelValues("failure").Inc()
		return
	}
	m.resumes.WithLabelValues("success").Inc()
	m.resumeTime.Observe(elapsed.Seconds())
}

func (m *Metrics) suspended() {
	if m != nil {
		m.suspends.Inc()
	}
}
