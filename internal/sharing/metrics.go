package sharing

import "github.com/prometheus/client_golang/prometheus"

// Metrics count the sharing endpoint's work. A nil *Metrics records nothing.
type Metrics struct {
	requests *prometheus.CounterVec
	listed   prometheus.Counter
	bytes    prometheus.Counter
}

// NewMetrics registers the sharing metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_sharing_requests_total",
			Help: "Delta Sharing requests from recipients, by HTTP status, unauthenticated, or file_denied.",
		}, []string{"result"}),
		listed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_sharing_files_listed_total",
			Help: "Data files listed to recipients in query responses.",
		}),
		bytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_sharing_file_bytes_total",
			Help: "Bytes of data files downloaded by recipients.",
		}),
	}
	reg.MustRegister(m.requests, m.listed, m.bytes)
	return m
}

func (m *Metrics) request(result string) {
	if m != nil {
		m.requests.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) filesListed(n int) {
	if m != nil {
		m.listed.Add(float64(n))
	}
}

func (m *Metrics) served(n int64) {
	if m != nil {
		m.bytes.Add(float64(n))
	}
}
