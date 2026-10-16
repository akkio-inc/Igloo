package audit

import "github.com/prometheus/client_golang/prometheus"

// Metrics count history records. A nil *Metrics records nothing.
type Metrics struct {
	writtenTotal *prometheus.CounterVec
	droppedTotal *prometheus.CounterVec
}

// NewMetrics registers the history metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		writtenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_audit_written_total",
			Help: "Query and login history records written, by kind: query or login.",
		}, []string{"kind"}),
		droppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_audit_dropped_total",
			Help: "Query and login history records dropped because the queue was full or the store failed, by kind.",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.writtenTotal, m.droppedTotal)
	return m
}

func (m *Metrics) written(kind string, n int) {
	if m != nil {
		m.writtenTotal.WithLabelValues(kind).Add(float64(n))
	}
}

func (m *Metrics) dropped(kind string, n int) {
	if m != nil {
		m.droppedTotal.WithLabelValues(kind).Add(float64(n))
	}
}
