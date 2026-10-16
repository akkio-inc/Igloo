package notify

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics counts notifications. A nil *Metrics counts nothing.
type Metrics struct {
	sends *prometheus.CounterVec
}

// NewMetrics registers the notification metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{sends: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "igloo_notifications_total",
		Help: "Notifications Igloo sent or tried to send, by integration type and result (sent, failed, rate_limited).",
	}, []string{"type", "result"})}
	reg.MustRegister(m.sends)
	return m
}

func (m *Metrics) sent(typ string, err error) {
	if m == nil {
		return
	}
	result := "sent"
	switch {
	case errors.Is(err, ErrRateLimited):
		result = "rate_limited"
	case err != nil:
		result = "failed"
	}
	m.sends.WithLabelValues(typ, result).Inc()
}
