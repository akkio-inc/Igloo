package icebergrest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
)

// maxResponse bounds a response from Lakekeeper. Table metadata with a long
// history can run to tens of megabytes.
const maxResponse = 256 << 20

// upstream calls Lakekeeper's Iceberg REST API for one warehouse.
type upstream struct {
	// base is the catalog URL, e.g. http://lakekeeper:8181/catalog.
	base      string
	warehouse string
	http      *http.Client
	log       *slog.Logger

	mu sync.Mutex
	// prefix is the warehouse's path prefix in Lakekeeper's API, from its
	// /v1/config.
	prefix string
}

// reply is a successful response from Lakekeeper.
type reply struct {
	status int
	body   []byte
}

// call sends a request to the warehouse's path in Lakekeeper's API, such
// as "namespaces/public/tables". It returns an *apiError for error
// responses.
func (u *upstream) call(ctx context.Context, method, path string, query url.Values, body []byte) (*reply, error) {
	prefix, err := u.warehousePrefix(ctx)
	if err != nil {
		return nil, err
	}
	target := u.base + "/v1/" + url.PathEscape(prefix) + "/" + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	return u.do(ctx, method, target, body)
}

func (u *upstream) do(ctx context.Context, method, target string, body []byte) (*reply, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := u.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "request canceled")
		}
		u.log.Warn("catalog request failed", "method", method, "url", target, "err", err)
		return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "the catalog is not reachable; try again")
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "could not read the catalog's response; try again")
	}
	if len(data) > maxResponse {
		return nil, fmt.Errorf("catalog response to %s %s is larger than %d bytes", method, target, maxResponse)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, u.refusal(method, target, resp.StatusCode, data)
	}
	return &reply{status: resp.StatusCode, body: data}, nil
}

// catalogRefusals are the error types of Lakekeeper's refusals that
// clients act on, which pass on with Igloo's words for them.
var catalogRefusals = map[string]string{
	"NoSuchTableException":       "the table does not exist",
	"NoSuchNamespaceException":   "the namespace does not exist",
	"AlreadyExistsException":     "a table or namespace of that name already exists",
	"NamespaceNotEmptyException": "the namespace is not empty",
	"CommitFailedException":      "the table changed since it was loaded; load it again and retry",
}

// refusalKinds are the error types a refusal of each status gets when
// Lakekeeper's is not one clients act on.
var refusalKinds = map[int]string{
	http.StatusNotFound: "NotFoundException",
	http.StatusConflict: "CommitFailedException",
}

// refusalCode is what a Lakekeeper error type that passes on, as a hint,
// looks like: a name such as SetLocationNotAllowed.
var refusalCode = regexp.MustCompile(`^[A-Za-z]{1,64}$`)

// refusal turns an error response from Lakekeeper into the client's.
// Lakekeeper's messages can name its internal IDs and storage locations,
// so they go to the log; the client gets the status, the type for the
// refusals clients act on (no such table, a conflicting commit), and
// Igloo's own message.
func (u *upstream) refusal(method, target string, status int, body []byte) *apiError {
	if status >= 500 {
		u.log.Error("catalog error", "method", method, "url", target, "status", status, "body", truncate(string(body), 2000))
		if status == http.StatusServiceUnavailable {
			return errorf(status, errUnavailable, "the catalog is unavailable; try again")
		}
		return errorf(http.StatusInternalServerError, errInternal, "the catalog failed; see Igloo's log")
	}
	var m struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &m)
	level := slog.LevelInfo
	if status == http.StatusNotFound {
		level = slog.LevelDebug // engines look tables up before creating them
	}
	u.log.Log(context.Background(), level, "catalog refused a request", "method", method, "url", target, "status", status,
		"type", m.Error.Type, "message", truncate(m.Error.Message, 2000))
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		// Lakekeeper refusing Igloo is Igloo's misconfiguration, not the
		// client's mistake.
		return errorf(http.StatusInternalServerError, errInternal, "the catalog refused Igloo's request; see Igloo's log")
	case catalogRefusals[m.Error.Type] != "":
		return errorf(status, m.Error.Type, "%s", catalogRefusals[m.Error.Type])
	}
	kind := refusalKinds[status]
	if kind == "" {
		kind = errBadRequest
	}
	if refusalCode.MatchString(m.Error.Type) {
		return errorf(status, kind, "the catalog refused the request: %s", m.Error.Type)
	}
	return errorf(status, kind, "the catalog refused the request (HTTP %d)", status)
}

// warehousePrefix returns, and remembers, the warehouse's prefix.
func (u *upstream) warehousePrefix(ctx context.Context) (string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.prefix != "" {
		return u.prefix, nil
	}
	r, err := u.do(ctx, http.MethodGet, u.base+"/v1/config?warehouse="+url.QueryEscape(u.warehouse), nil)
	if err != nil {
		return "", err
	}
	var cfg struct {
		Overrides map[string]string `json:"overrides"`
		Defaults  map[string]string `json:"defaults"`
	}
	if err := json.Unmarshal(r.body, &cfg); err != nil {
		return "", fmt.Errorf("catalog config: %w", err)
	}
	prefix := cfg.Overrides["prefix"]
	if prefix == "" {
		prefix = cfg.Defaults["prefix"]
	}
	if prefix == "" {
		return "", fmt.Errorf("the catalog names no prefix for warehouse %q", u.warehouse)
	}
	u.prefix = prefix
	return prefix, nil
}

// namespacePath is the API path of a namespace.
func namespacePathOf(ns []string) string {
	return "namespaces/" + url.PathEscape(strings.Join(ns, "\x1f"))
}

// tablePathOf is the API path of a table.
func tablePathOf(ns []string, table string) string {
	return namespacePathOf(ns) + "/tables/" + url.PathEscape(table)
}

// pick keeps the query parameters an operation passes on to Lakekeeper.
// Others, such as Lakekeeper's own recursive drops, are not for engines.
func pick(q url.Values, keys ...string) url.Values {
	out := url.Values{}
	for _, k := range keys {
		if v, ok := q[k]; ok {
			out[k] = v
		}
	}
	return out
}

// loadResult is a LoadTableResult from Lakekeeper.
type loadResult struct {
	fields map[string]json.RawMessage
	// location and uuid are the table's, from its metadata.
	location, uuid string
}

// parseLoadResult reads a LoadTableResult and drops what Lakekeeper says
// about reaching the table's files ("config", "storage-credentials"):
// those are Lakekeeper's, pointing at its own request signer. Igloo adds
// its own.
func parseLoadResult(body []byte) (*loadResult, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("catalog returned malformed table metadata: %w", err)
	}
	var meta struct {
		Location string `json:"location"`
		UUID     string `json:"table-uuid"`
	}
	if err := json.Unmarshal(fields["metadata"], &meta); err != nil || meta.Location == "" {
		return nil, fmt.Errorf("catalog returned table metadata without a location")
	}
	delete(fields, "config")
	delete(fields, "storage-credentials")
	return &loadResult{fields: fields, location: meta.Location, uuid: meta.UUID}, nil
}

// marshal returns the result with Igloo's credentials, if any.
func (l *loadResult) marshal(creds *Credentials, refresh string) ([]byte, error) {
	config := map[string]string{}
	if creds != nil {
		for k, v := range creds.Config {
			config[k] = v
		}
		config["client.refresh-credentials-endpoint"] = refresh
		sc, err := json.Marshal(creds.storageCredentials())
		if err != nil {
			return nil, err
		}
		l.fields["storage-credentials"] = sc
	}
	c, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	l.fields["config"] = c
	return json.Marshal(l.fields)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
