package distributed

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the coordinator's Prometheus metrics. A nil *Metrics records
// nothing.
type Metrics struct {
	queries   *prometheus.CounterVec
	fallbacks *prometheus.CounterVec
	duration  prometheus.Histogram
	tasks     *prometheus.CounterVec
	workers   prometheus.Gauge
	scanned   prometheus.Counter
	received  prometheus.Counter
	starts    *prometheus.CounterVec
	guards    prometheus.Counter
}

// NewMetrics registers the coordinator's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_distributed_queries_total",
			Help: "Statements whose parts ran on workers, by outcome: ok, error (in the data), out_of_memory (a worker ran out), bitmap_memory (a task's bitmaps outgrew their budget), canceled, or fallback (ran on this replica instead).",
		}, []string{"outcome"}),
		fallbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_distributed_fallbacks_total",
			Help: "Statements Igloo started to run on workers and then ran on this replica, by reason: no_workers, worker_error.",
		}, []string{"reason"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "igloo_distributed_query_seconds",
			Help:    "Time to run a statement's parts on workers and combine them, before results are sent.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 14), // 50ms to ~7min
		}),
		tasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_distributed_tasks_total",
			Help: "Fragments sent to workers, by result: ok, retried (failed and sent to another worker), throttled (turned away by object storage and sent again in a while) or failed.",
		}, []string{"result"}),
		workers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_distributed_workers",
			Help: "Workers that answered the last check.",
		}),
		scanned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_distributed_scanned_bytes_total",
			Help: "Bytes of data files that statements divided among workers.",
		}),
		received: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_distributed_received_bytes_total",
			Help: "Bytes of partial results received from workers.",
		}),
		starts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_distributed_worker_starts_total",
			Help: "Times Igloo started or stopped workers, by action (start, stop) and result (success, failure).",
		}, []string{"action", "result"}),
		guards: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_auto_guard_reruns_total",
			Help: "Statements auto mode ran on this replica that ran past three times their estimate there, and were canceled and run on workers instead.",
		}),
	}
	reg.MustRegister(m.queries, m.fallbacks, m.duration, m.tasks, m.workers, m.scanned, m.received, m.starts, m.guards)
	return m
}

func (m *Metrics) query(outcome string, start time.Time) {
	if m == nil {
		return
	}
	m.queries.WithLabelValues(outcome).Inc()
	if outcome == "ok" {
		m.duration.Observe(time.Since(start).Seconds())
	}
}

// Fallback counts a statement that ran on the coordinator after all.
func (m *Metrics) guard() {
	if m != nil {
		m.guards.Inc()
	}
}

func (m *Metrics) Fallback(reason string) {
	if m == nil {
		return
	}
	m.queries.WithLabelValues("fallback").Inc()
	m.fallbacks.WithLabelValues(reason).Inc()
}

func (m *Metrics) task(result string) {
	if m != nil {
		m.tasks.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) setWorkers(n int) {
	if m != nil {
		m.workers.Set(float64(n))
	}
}

func (m *Metrics) scan(bytes int64) {
	if m != nil {
		m.scanned.Add(float64(bytes))
	}
}

func (m *Metrics) receive(bytes int64) {
	if m != nil {
		m.received.Add(float64(bytes))
	}
}

func (m *Metrics) compute(action string, err error) {
	if m == nil {
		return
	}
	result := "success"
	if err != nil {
		result = "failure"
	}
	m.starts.WithLabelValues(action, result).Inc()
}

// WorkerMetrics are a worker's Prometheus metrics. A nil *WorkerMetrics
// records nothing.
type WorkerMetrics struct {
	tasks    *prometheus.CounterVec
	duration prometheus.Histogram
	sentB    prometheus.Counter
}

// NewWorkerMetrics registers a worker's metrics with reg.
func NewWorkerMetrics(reg prometheus.Registerer) *WorkerMetrics {
	m := &WorkerMetrics{
		tasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_worker_tasks_total",
			Help: "Fragments run for coordinators, by outcome: ok, error, canceled, out_of_memory (canceled as the worker ran out), bitmap_memory (its bitmaps outgrew their budget), refused (files workers cannot read as the table says), or throttled (turned away by object storage past DuckDB's retries).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "igloo_worker_task_seconds",
			Help:    "Time to run a fragment and send its result.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 16), // 10ms to ~5min
		}),
		sentB: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_worker_sent_bytes_total",
			Help: "Bytes of results sent to coordinators.",
		}),
	}
	reg.MustRegister(m.tasks, m.duration, m.sentB)
	return m
}

func (m *WorkerMetrics) task(outcome string, start time.Time) {
	if m != nil {
		m.tasks.WithLabelValues(outcome).Inc()
		m.duration.Observe(time.Since(start).Seconds())
	}
}

func (m *WorkerMetrics) sent(n int64) {
	if m != nil {
		m.sentB.Add(float64(n))
	}
}
