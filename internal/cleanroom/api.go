package cleanroom

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/sharing"
)

// The room's API is how partners take part: they reach Igloo's sharing
// endpoint, as they do for Delta Sharing, with the bearer token of the
// recipient that represents them, at /clean-rooms instead of
// /delta-sharing. The token reads nothing but this API and their shares.
//
//	GET    /clean-rooms                                       rooms the partner is in
//	GET    /clean-rooms/{room}                                collaborators, datasets and policies, templates and approvals
//	POST   /clean-rooms/{room}/templates/{template}/approval  {"version": n} approves that version
//	DELETE /clean-rooms/{room}/templates/{template}/approval  withdraws the approval
//	POST   /clean-rooms/{room}/templates/{template}/runs      {"parameters": {...}} runs it; the result comes back
//	GET    /clean-rooms/{room}/runs                           the run history
//	GET    /clean-rooms/{room}/runs/{id}                      a run, with its result if it was the partner's

// APIPrefix is where the room's API is served.
const APIPrefix = "/clean-rooms"

// Recipients authenticates partners by their Delta Sharing recipient's
// bearer token; *sharing.Server does.
type Recipients interface {
	Recipient(r *http.Request) (string, error)
}

// API serves clean rooms to partners.
type API struct {
	svc        *Service
	recipients Recipients
	audit      *audit.Log
	log        *slog.Logger
	mux        *http.ServeMux
	// timeout bounds a run.
	timeout time.Duration
}

// NewAPI serves svc to the partners recipients authenticates. Every
// request is recorded in a's query history; a may be nil. timeout bounds a
// run; zero means none.
func NewAPI(svc *Service, recipients Recipients, a *audit.Log, timeout time.Duration, log *slog.Logger) *API {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	api := &API{svc: svc, recipients: recipients, audit: a, log: log.With("component", "cleanroom-api"), timeout: timeout}
	api.mux = http.NewServeMux()
	route := func(pattern string, h func(*http.Request, Actor) (any, error)) {
		method, path, _ := strings.Cut(pattern, " ")
		api.mux.HandleFunc(method+" "+APIPrefix+path, api.handle(h))
	}
	route("GET /{$}", api.rooms)
	route("GET /{room}", api.room)
	route("POST /{room}/templates/{template}/approval", api.approve)
	route("DELETE /{room}/templates/{template}/approval", api.unapprove)
	route("POST /{room}/templates/{template}/runs", api.run)
	route("GET /{room}/runs", api.runs)
	route("GET /{room}/runs/{id}", api.runResult)
	api.mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "no such endpoint")
	})
	return api
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	h.Set("Referrer-Policy", "no-referrer")
	if !sharing.Secure(r) {
		writeError(w, http.StatusForbidden, "PERMISSION_DENIED", "the clean room API accepts unencrypted requests only at localhost; "+
			"configure TLS (IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE) to reach it across a network")
		return
	}
	api.mux.ServeHTTP(w, r)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"errorCode": code, "message": msg})
}

// handle authenticates a request, runs h and records the request.
func (api *API) handle(h func(*http.Request, Actor) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recipient, err := api.recipients.Recipient(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="clean-rooms"`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", err.Error())
			return
		}
		a := Actor{Recipient: recipient}
		out, err := h(r, a)
		status, code := http.StatusOK, ""
		if err != nil {
			status, code = errorStatus(err)
			if status == http.StatusInternalServerError {
				api.log.Warn("clean room request failed", "recipient", recipient, "path", r.URL.Path, "err", err)
				err = errors.New("the request failed; try again or ask the room's host")
			}
			writeError(w, status, code, err.Error())
		} else {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(w).Encode(out)
		}
		api.record(r, a, start, status, out)
	}
}

// errorStatus answers an error of the service.
func errorStatus(err error) (int, string) {
	var refused *RefusedError
	switch {
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST"
	case errors.Is(err, ErrExists):
		return http.StatusConflict, "RESOURCE_ALREADY_EXISTS"
	case IsDenied(err):
		return http.StatusForbidden, "PERMISSION_DENIED"
	case errors.As(err, &refused):
		return http.StatusForbidden, "RUN_REFUSED"
	case IsBudget(err):
		return http.StatusTooManyRequests, "RESOURCE_EXHAUSTED"
	case IsInvalid(err):
		return http.StatusBadRequest, "INVALID_PARAMETER_VALUE"
	case errors.Is(err, ErrUnavailable):
		return http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE"
	}
	return http.StatusInternalServerError, "INTERNAL_ERROR"
}

func (api *API) record(r *http.Request, a Actor, start time.Time, status int, out any) {
	if api.audit == nil {
		return
	}
	q := audit.Query{
		User: a.Name(), Role: "recipient", Start: start, Duration: time.Since(start), Outcome: audit.OutcomeOK, Rows: -1,
		StatementType: "CLEAN ROOM", Text: r.Method + " " + r.URL.Path, ClientAddress: r.RemoteAddr,
		ApplicationName: truncate(r.UserAgent(), 200),
	}
	if run, ok := out.(*runJSON); ok && run.Result != nil {
		q.Rows = int64(len(run.Result.Rows))
	}
	switch {
	case status == http.StatusForbidden || status == http.StatusNotFound:
		q.Outcome, q.SQLState = audit.OutcomeDenied, "42501"
	case status >= 500:
		q.Outcome, q.SQLState = audit.OutcomeError, "XX000"
	case status >= 400:
		q.Outcome, q.SQLState = audit.OutcomeError, "22023"
	}
	api.audit.RecordQuery(q)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// readBody decodes a JSON request body, which may be empty.
func readBody(r *http.Request, v any) error {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20))
	if err != nil {
		return invalid("the request body is too large")
	}
	if strings.TrimSpace(string(body)) == "" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalid("malformed request body: %v", err)
	}
	return nil
}

// --- what the API returns -------------------------------------------------------

type roomJSON struct {
	Name          string             `json:"name"`
	Comment       string             `json:"comment,omitempty"`
	You           []string           `json:"you"`
	Collaborators []collaboratorJSON `json:"collaborators,omitempty"`
	Datasets      []datasetJSON      `json:"datasets,omitempty"`
	Templates     []templateJSON     `json:"templates,omitempty"`
}

type collaboratorJSON struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type datasetJSON struct {
	Name         string `json:"name"`
	Collaborator string `json:"collaborator"`
	Kind         string `json:"kind"`
	Policy       Policy `json:"policy"`
}

type templateJSON struct {
	Name       string      `json:"name"`
	Version    int         `json:"version"`
	Digest     string      `json:"digest"`
	SQL        string      `json:"sql"`
	Parameters []Parameter `json:"parameters"`
	RunBy      []string    `json:"run_by"`
	Builtin    string      `json:"builtin,omitempty"`
	Needs      []string    `json:"needs"`
	Approved   []string    `json:"approved"`
	Runnable   bool        `json:"runnable"`
	Problem    string      `json:"problem,omitempty"`
}

type resultJSON struct {
	Columns []Column    `json:"columns"`
	Rows    [][]*string `json:"rows"`
}

type runJSON struct {
	ID          int64          `json:"id"`
	Template    string         `json:"template"`
	Version     int            `json:"version"`
	Requester   string         `json:"requester"`
	RequestedBy string         `json:"requested_by"`
	Parameters  map[string]any `json:"parameters"`
	Approvals   []string       `json:"approvals"`
	Datasets    []string       `json:"datasets"`
	Status      string         `json:"status"`
	Error       string         `json:"error,omitempty"`
	Rows        int64          `json:"row_count"`
	Truncated   bool           `json:"truncated"`
	Noise       float64        `json:"noise_epsilon,omitempty"`
	Started     time.Time      `json:"started_at"`
	Finished    *time.Time     `json:"finished_at,omitempty"`
	Result      *resultJSON    `json:"result,omitempty"`
}

func toRunJSON(r *Run) *runJSON {
	out := &runJSON{ID: r.ID, Template: r.Template, Version: r.Version, Requester: r.Requester, RequestedBy: r.RequestedBy,
		Parameters: r.Parameters, Approvals: r.Approvals, Datasets: r.Datasets, Status: r.Status, Error: r.Error,
		Rows: r.Rows, Truncated: r.Truncated, Noise: r.Noise, Started: r.Started, Finished: r.Finished}
	if r.Columns != nil {
		out.Result = &resultJSON{Columns: r.Columns, Rows: r.Result}
		if out.Result.Rows == nil {
			out.Result.Rows = [][]*string{}
		}
	}
	return out
}

// --- handlers --------------------------------------------------------------------

func (api *API) rooms(r *http.Request, a Actor) (any, error) {
	rooms, err := api.svc.Rooms(r.Context(), a)
	if err != nil {
		return nil, err
	}
	items := []roomJSON{}
	for _, room := range rooms {
		items = append(items, roomJSON{Name: room.Name, Comment: room.Comment, You: room.Yours})
	}
	return map[string]any{"items": items}, nil
}

func (api *API) room(r *http.Request, a Actor) (any, error) {
	v, err := api.svc.Describe(r.Context(), a, r.PathValue("room"))
	if err != nil {
		return nil, err
	}
	out := roomJSON{Name: v.Room.Name, Comment: v.Room.Comment, You: v.Yours}
	for _, c := range v.Collaborators {
		out.Collaborators = append(out.Collaborators, collaboratorJSON{Name: c.Name, Kind: c.Kind})
	}
	// Partners see what each dataset allows, not where it is kept.
	for _, d := range v.Datasets {
		out.Datasets = append(out.Datasets, datasetJSON{Name: d.Name, Collaborator: d.Collaborator, Kind: d.Kind, Policy: d.Policy})
	}
	for _, t := range v.Templates {
		out.Templates = append(out.Templates, templateJSON{Name: t.Name, Version: t.Version, Digest: t.Approves, SQL: t.SQL, Parameters: nonNil(t.Parameters),
			RunBy: t.RunBy, Builtin: t.Builtin, Needs: nonNil(t.Needs), Approved: nonNil(t.Approved), Runnable: t.Runnable(), Problem: t.Problem})
	}
	return out, nil
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func (api *API) approve(r *http.Request, a Actor) (any, error) {
	var body struct {
		Version int `json:"version"`
	}
	if err := readBody(r, &body); err != nil {
		return nil, err
	}
	if body.Version < 1 {
		return nil, invalid(`say which version you approve: {"version": n}`)
	}
	c, err := api.svc.Approve(r.Context(), a, r.PathValue("room"), r.PathValue("template"), body.Version, "")
	if err != nil {
		return nil, err
	}
	return map[string]any{"template": r.PathValue("template"), "version": body.Version, "approved_by": c}, nil
}

func (api *API) unapprove(r *http.Request, a Actor) (any, error) {
	c, err := api.svc.Unapprove(r.Context(), a, r.PathValue("room"), r.PathValue("template"), "")
	if err != nil {
		return nil, err
	}
	return map[string]any{"template": r.PathValue("template"), "withdrawn_by": c}, nil
}

func (api *API) run(r *http.Request, a Actor) (any, error) {
	var body struct {
		Parameters map[string]any `json:"parameters"`
	}
	if err := readBody(r, &body); err != nil {
		return nil, err
	}
	ctx := r.Context()
	if api.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, api.timeout)
		defer cancel()
	}
	run, err := api.svc.Run(ctx, a, r.PathValue("room"), r.PathValue("template"), body.Parameters, "")
	if err != nil {
		return nil, err
	}
	return toRunJSON(run), nil
}

func (api *API) runs(r *http.Request, a Actor) (any, error) {
	runs, err := api.svc.Runs(r.Context(), a, r.PathValue("room"), 500)
	if err != nil {
		return nil, err
	}
	items := []*runJSON{}
	for i := range runs {
		items = append(items, toRunJSON(&runs[i]))
	}
	return map[string]any{"items": items}, nil
}

func (api *API) runResult(r *http.Request, a Actor) (any, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		return nil, ErrNotFound
	}
	run, err := api.svc.RunResult(r.Context(), a, r.PathValue("room"), id)
	if err != nil {
		return nil, err
	}
	return toRunJSON(run), nil
}
