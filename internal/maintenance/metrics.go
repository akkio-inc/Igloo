package maintenance

import "github.com/prometheus/client_golang/prometheus"

// Metrics reports what maintenance does. A nil *Metrics records nothing.
type Metrics struct {
	passes         *prometheus.CounterVec
	lastSuccess    prometheus.Gauge
	filesRewritten prometheus.Counter
	filesWritten   prometheus.Counter
	snapshots      prometheus.Counter
	orphans        prometheus.Counter
}

// NewMetrics registers the maintenance metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_maintenance_passes_total",
			Help: "Passes over every table, by result: ok or error.",
		}, []string{"result"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_maintenance_last_success_timestamp_seconds",
			Help: "When this replica last finished a pass with no errors.",
		}),
		filesRewritten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_maintenance_compacted_files_total",
			Help: "Data files replaced by compaction.",
		}),
		filesWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_maintenance_compaction_output_files_total",
			Help: "Data files written by compaction.",
		}),
		snapshots: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_maintenance_expired_snapshots_total",
			Help: "Snapshots expired.",
		}),
		orphans: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_maintenance_orphan_files_deleted_total",
			Help: "Files deleted because no snapshot referenced them.",
		}),
	}
	reg.MustRegister(m.passes, m.lastSuccess, m.filesRewritten, m.filesWritten, m.snapshots, m.orphans)
	return m
}

func (m *Metrics) pass(ok bool) {
	if m == nil {
		return
	}
	if ok {
		m.passes.WithLabelValues("ok").Inc()
		m.lastSuccess.SetToCurrentTime()
		return
	}
	m.passes.WithLabelValues("error").Inc()
}

func (m *Metrics) compacted(before, after int) {
	if m != nil {
		m.filesRewritten.Add(float64(before))
		m.filesWritten.Add(float64(after))
	}
}

func (m *Metrics) expired(n int) {
	if m != nil {
		m.snapshots.Add(float64(n))
	}
}

func (m *Metrics) orphansDeleted(n int) {
	if m != nil {
		m.orphans.Add(float64(n))
	}
}
