package icebergrest

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// Every catalog request an engine makes goes to the query history like a
// statement: its operation as the statement type ("loadTable"), the method
// and path as the text, the tables it read or changed, and its outcome.
// Token requests go to the login history, through Options.Authenticate.

// Request outcomes, as in the query history.
const (
	outcomeOK     = audit.OutcomeOK
	outcomeDenied = audit.OutcomeDenied
	outcomeError  = audit.OutcomeError
)

// unrecorded operations say nothing about data: clients make them on
// their own.
var unrecorded = map[string]bool{opConfig: true, opReportMetrics: true}

// record counts a request and sends it to the history.
func (s *Server) record(c *call, start time.Time, err error) {
	outcome := outcomeOf(err)
	s.opts.Metrics.request(c.op, outcome)
	if s.opts.Audit == nil || c.who == nil || unrecorded[c.op] {
		return
	}
	text := c.r.Method + " " + c.r.URL.RequestURI()
	if c.detail != "" {
		// Commit details name what the engine declared.
		text += " (" + clip(c.detail, 1000) + ")"
	}
	q := audit.Query{
		SessionID:       c.who.session,
		User:            c.who.user.Name,
		Role:            string(c.who.user.Role),
		Start:           start,
		Duration:        time.Since(start),
		Outcome:         outcome,
		Rows:            -1,
		StatementType:   c.op,
		Text:            text,
		TablesRead:      s.names(c.read),
		TablesWritten:   s.names(c.written),
		ClientAddress:   c.r.RemoteAddr,
		ApplicationName: c.who.client,
	}
	if err != nil {
		q.SQLState = sqlState(err)
	}
	s.opts.Audit.RecordQuery(q)
}

// names are tables' names in SQL, as the history gives them:
// lake.public.events.
func (s *Server) names(objects []auth.Object) []string {
	if objects == nil {
		return nil
	}
	out := make([]string, len(objects))
	for i, o := range objects {
		out[i] = s.opts.CatalogName + "." + o.String()
	}
	return out
}

func outcomeOf(err error) string {
	var api *apiError
	switch {
	case err == nil:
		return outcomeOK
	case errors.As(err, &api) && (api.status == http.StatusForbidden || api.status == http.StatusUnauthorized):
		return outcomeDenied
	}
	return outcomeError
}

// sqlState gives a failed request the SQLSTATE a statement failing the
// same way would have.
func sqlState(err error) string {
	var api *apiError
	if !errors.As(err, &api) {
		return "XX000"
	}
	switch {
	case api.status == http.StatusUnauthorized:
		return "28000" // invalid_authorization_specification
	case api.status == http.StatusForbidden:
		return "42501" // insufficient_privilege
	case api.status == http.StatusNotFound:
		return "42704" // undefined_object
	case api.status == http.StatusConflict && api.kind == "AlreadyExistsException":
		return "42710" // duplicate_object
	case api.status == http.StatusConflict:
		return "40001" // serialization_failure: a concurrent commit
	case api.status < 500:
		return "22023" // invalid_parameter_value
	case api.status == http.StatusServiceUnavailable:
		return "57P03" // cannot_connect_now
	}
	return "XX000" // internal_error
}

// Metrics count the endpoint's requests.
type Metrics struct {
	requests *prometheus.CounterVec
}

// NewMetrics registers the endpoint's metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{requests: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "igloo_iceberg_requests_total",
		Help: "Iceberg REST catalog requests from other engines, by operation and outcome (ok, denied, error).",
	}, []string{"operation", "outcome"})}
	reg.MustRegister(m.requests)
	return m
}

func (m *Metrics) request(op, outcome string) {
	if m != nil {
		m.requests.WithLabelValues(op, outcome).Inc()
	}
}
