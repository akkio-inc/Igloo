// Package icebergtest provides an Iceberg REST catalog for tests, and a
// proxy that injects faults in front of any catalog.
//
// The catalog keeps namespaces, tables and views in memory and handles
// commits as Lakekeeper does: it checks a commit's requirements against the
// table, applies its updates with iceberg-go, all tables of a transaction or
// none, refuses a commit whose requirements fail with 409
// CatalogCommitConflicts, and honours Idempotency-Key headers, answering a
// repeat of an applied change with success. Tables' files go wherever their
// locations say; tests use a local directory. Views are created, replaced,
// renamed and dropped through the REST API's view endpoints, which it does
// not list in its configuration, so DuckDB leaves them alone, as Igloo
// expects of the catalogs it runs on.
package icebergtest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Catalog is an in-memory Iceberg REST catalog.
type Catalog struct {
	// URL is the catalog's base URL, as IGLOO_CATALOG_URI takes it.
	URL string

	srv      *httptest.Server
	location string

	mu         sync.Mutex
	namespaces map[string]map[string]string
	tables     map[string]*tableState
	views      map[string]*viewState
	// keys are the idempotency keys of applied changes.
	keys       map[string]bool
	ignoreKeys bool
	// ignoreCase finds tables and views by name whatever its case (see
	// IgnoreNameCase).
	ignoreCase bool
	// holdViewLists, if set, runs between making a list of views and
	// sending it.
	holdViewLists func(namespace string)
	// commits counts applied commits.
	commits int
}

type tableState struct {
	ns       []string
	name     string
	meta     table.Metadata
	metaLoc  string
	versions int
}

// Prefix is the warehouse's path prefix, as Lakekeeper names it with the
// warehouse's ID.
const Prefix = "8a6f3c1e-warehouse"

// NewCatalog starts a catalog whose tables live under location, a
// directory or object store URL. Close stops it.
func NewCatalog(location string) *Catalog {
	c := &Catalog{
		location:   strings.TrimSuffix(location, "/"),
		namespaces: map[string]map[string]string{},
		tables:     map[string]*tableState{},
		views:      map[string]*viewState{},
		keys:       map[string]bool{},
	}
	c.srv = httptest.NewServer(c)
	c.URL = c.srv.URL + "/catalog"
	return c
}

// Close stops the catalog.
func (c *Catalog) Close() { c.srv.Close() }

// Commits is how many commits the catalog has applied.
func (c *Catalog) Commits() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commits
}

// Metadata returns a table's current metadata, or nil if it does not exist.
func (c *Catalog) Metadata(ns, name string) table.Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t := c.tables[c.tableKey([]string{ns}, name)]; t != nil {
		return t.meta
	}
	return nil
}

// endpoints are those Lakekeeper lists in its configuration.
var endpoints = []string{
	"GET /v1/{prefix}/namespaces",
	"POST /v1/{prefix}/namespaces",
	"GET /v1/{prefix}/namespaces/{namespace}",
	"HEAD /v1/{prefix}/namespaces/{namespace}",
	"DELETE /v1/{prefix}/namespaces/{namespace}",
	"POST /v1/{prefix}/namespaces/{namespace}/properties",
	"GET /v1/{prefix}/namespaces/{namespace}/tables",
	"POST /v1/{prefix}/namespaces/{namespace}/tables",
	"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
	"POST /v1/{prefix}/tables/rename",
	"POST /v1/{prefix}/transactions/commit",
}

func (c *Catalog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), "/catalog/v1/")
	if !ok {
		apiError(w, http.StatusNotFound, "NotFoundException", "no such path")
		return
	}
	var parts []string
	for _, p := range strings.Split(rest, "/") {
		s, err := url.PathUnescape(p)
		if err != nil {
			apiError(w, http.StatusBadRequest, "BadRequestException", "bad path")
			return
		}
		parts = append(parts, s)
	}
	if len(parts) == 1 && parts[0] == "config" && r.Method == http.MethodGet {
		cfg := map[string]any{
			"defaults":  map[string]string{"prefix": Prefix, "rest-page-size": "100"},
			"overrides": map[string]string{"uri": c.URL},
			"endpoints": endpoints,
		}
		c.mu.Lock()
		if !c.ignoreKeys {
			cfg["idempotency-key-lifetime"] = "PT30M"
		}
		c.mu.Unlock()
		writeJSON(w, http.StatusOK, cfg)
		return
	}
	if len(parts) < 2 || parts[0] != Prefix {
		apiError(w, http.StatusNotFound, "NotFoundException", "no such path")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	key := r.Header.Get("Idempotency-Key")

	if p := parts[1:]; len(p) == 3 && p[0] == "namespaces" && p[2] == "views" && r.Method == http.MethodGet {
		c.listViews(w, r, p)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch p := parts[1:]; {
	case len(p) == 1 && p[0] == "namespaces" && r.Method == http.MethodGet:
		// The children of parent, or the top-level namespaces.
		parent := r.URL.Query().Get("parent")
		list := [][]string{}
		for ns := range c.namespaces {
			levels := strings.Split(ns, "\x1f")
			if parent == "" && len(levels) == 1 || parent != "" && strings.HasPrefix(ns, parent+"\x1f") && !strings.Contains(ns[len(parent)+1:], "\x1f") {
				list = append(list, levels)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"namespaces": list})
	case len(p) == 1 && p[0] == "namespaces" && r.Method == http.MethodPost:
		var req struct {
			Namespace  []string          `json:"namespace"`
			Properties map[string]string `json:"properties"`
		}
		if json.Unmarshal(body, &req) != nil || len(req.Namespace) == 0 {
			apiError(w, http.StatusBadRequest, "BadRequestException", "bad namespace")
			return
		}
		ns := strings.Join(req.Namespace, "\x1f")
		if c.replay(key) {
			writeJSON(w, http.StatusOK, map[string]any{"namespace": req.Namespace, "properties": c.namespaces[ns]})
			return
		}
		if _, ok := c.namespaces[ns]; ok {
			apiError(w, http.StatusConflict, "AlreadyExistsException", "namespace exists")
			return
		}
		if req.Properties == nil {
			req.Properties = map[string]string{}
		}
		c.namespaces[ns] = req.Properties
		c.remember(key)
		writeJSON(w, http.StatusOK, map[string]any{"namespace": req.Namespace, "properties": req.Properties})
	case len(p) == 2 && p[0] == "namespaces":
		props, ok := c.namespaces[p[1]]
		switch {
		case !ok:
			apiError(w, http.StatusNotFound, "NoSuchNamespaceException", "no such namespace")
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]any{"namespace": strings.Split(p[1], "\x1f"), "properties": props})
		case r.Method == http.MethodDelete:
			delete(c.namespaces, p[1])
			w.WriteHeader(http.StatusNoContent)
		default:
			apiError(w, http.StatusMethodNotAllowed, "BadRequestException", "bad method")
		}
	case len(p) == 3 && p[0] == "namespaces" && p[2] == "tables" && r.Method == http.MethodGet:
		ids := []map[string]any{}
		for _, t := range c.tables {
			if strings.Join(t.ns, "\x1f") == p[1] {
				ids = append(ids, map[string]any{"namespace": t.ns, "name": t.name})
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"identifiers": ids})
	case len(p) == 3 && p[0] == "namespaces" && p[2] == "tables" && r.Method == http.MethodPost:
		c.createTable(w, strings.Split(p[1], "\x1f"), body, key)
	case len(p) == 4 && p[0] == "namespaces" && p[2] == "tables":
		ns := strings.Split(p[1], "\x1f")
		t := c.tables[c.tableKey(ns, p[3])]
		switch r.Method {
		case http.MethodHead:
			if t == nil {
				apiError(w, http.StatusNotFound, "NoSuchTableException", "no such table")
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			if t == nil {
				apiError(w, http.StatusNotFound, "NoSuchTableException", "no such table")
				return
			}
			writeJSON(w, http.StatusOK, loadResult(t))
		case http.MethodDelete:
			if t == nil {
				apiError(w, http.StatusNotFound, "NoSuchTableException", "no such table")
				return
			}
			delete(c.tables, c.tableKey(ns, p[3]))
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPost:
			var change tableChange
			if err := json.Unmarshal(body, &change); err != nil {
				apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
				return
			}
			change.Identifier = &identifier{Namespace: ns, Name: p[3]}
			if c.replay(key) {
				writeJSON(w, http.StatusOK, commitResponse(c.tables[c.tableKey(ns, p[3])]))
				return
			}
			if status, typ, err := c.commit([]tableChange{change}); err != nil {
				apiError(w, status, typ, err.Error())
				return
			}
			c.remember(key)
			writeJSON(w, http.StatusOK, commitResponse(c.tables[c.tableKey(ns, p[3])]))
		}
	case len(p) == 5 && p[0] == "namespaces" && p[2] == "tables" && p[4] == "metrics":
		w.WriteHeader(http.StatusNoContent)
	case len(p) == 2 && p[0] == "tables" && p[1] == "rename" && r.Method == http.MethodPost:
		var req struct {
			Source      identifier `json:"source"`
			Destination identifier `json:"destination"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
			return
		}
		if c.replay(key) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		from, to := c.tableKey(req.Source.Namespace, req.Source.Name), c.tableKey(req.Destination.Namespace, req.Destination.Name)
		t := c.tables[from]
		switch {
		case t == nil:
			apiError(w, http.StatusNotFound, "NoSuchTableException", "no such table")
			return
		case c.tables[to] != nil || c.views[to] != nil:
			apiError(w, http.StatusConflict, "AlreadyExistsException", "table exists")
			return
		}
		if _, ok := c.namespaces[strings.Join(req.Destination.Namespace, "\x1f")]; !ok {
			apiError(w, http.StatusNotFound, "NoSuchNamespaceException", "no such namespace")
			return
		}
		delete(c.tables, from)
		t.ns, t.name = req.Destination.Namespace, req.Destination.Name
		c.tables[to] = t
		c.remember(key)
		w.WriteHeader(http.StatusNoContent)
	case len(p) == 2 && p[0] == "transactions" && p[1] == "commit" && r.Method == http.MethodPost:
		var req struct {
			TableChanges []tableChange `json:"table-changes"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
			return
		}
		if c.replay(key) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if status, typ, err := c.commit(req.TableChanges); err != nil {
			apiError(w, status, typ, err.Error())
			return
		}
		c.remember(key)
		w.WriteHeader(http.StatusNoContent)
	case len(p) >= 3 && p[0] == "namespaces" && p[2] == "views",
		len(p) == 2 && p[0] == "views" && p[1] == "rename":
		c.serveView(w, r, p, body)
	default:
		apiError(w, http.StatusNotFound, "NotFoundException", "no such path: "+r.Method+" "+rest)
	}
}

// IgnoreIdempotencyKeys makes the catalog ignore Idempotency-Key headers,
// as catalogs that do not support them do.
func (c *Catalog) IgnoreIdempotencyKeys() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ignoreKeys = true
}

// IgnoreNameCase makes the catalog find tables and views by name whatever
// the case it is spelled in, as Lakekeeper does: a table created as ORDERS
// is loaded, changed, renamed or dropped as orders too, and keeps the name
// it was created with. Call it before making any whose name is not lower
// case.
func (c *Catalog) IgnoreNameCase() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ignoreCase = true
}

// replay reports whether a change with key was applied already.
func (c *Catalog) replay(key string) bool {
	return key != "" && !c.ignoreKeys && c.keys[key]
}

func (c *Catalog) remember(key string) {
	if key != "" {
		c.keys[key] = true
	}
}

func (c *Catalog) createTable(w http.ResponseWriter, ns []string, body []byte, key string) {
	if _, ok := c.namespaces[strings.Join(ns, "\x1f")]; !ok {
		apiError(w, http.StatusNotFound, "NoSuchNamespaceException", "no such namespace")
		return
	}
	var req struct {
		Name          string             `json:"name"`
		Location      string             `json:"location"`
		Schema        *iceberg.Schema    `json:"schema"`
		PartitionSpec *json.RawMessage   `json:"partition-spec"`
		WriteOrder    *table.SortOrder   `json:"write-order"`
		StageCreate   bool               `json:"stage-create"`
		Properties    iceberg.Properties `json:"properties"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Schema == nil || req.Name == "" {
		apiError(w, http.StatusBadRequest, "BadRequestException", fmt.Sprintf("bad table: %v", err))
		return
	}
	k := c.tableKey(ns, req.Name)
	if t := c.tables[k]; t != nil {
		if c.replay(key) {
			writeJSON(w, http.StatusOK, loadResult(t))
			return
		}
		apiError(w, http.StatusConflict, "AlreadyExistsException", "table exists")
		return
	}
	if c.views[k] != nil {
		apiError(w, http.StatusConflict, "AlreadyExistsException", "a view of the name exists")
		return
	}
	spec := iceberg.UnpartitionedSpec
	if req.PartitionSpec != nil {
		var s iceberg.PartitionSpec
		if err := json.Unmarshal(*req.PartitionSpec, &s); err != nil {
			apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
			return
		}
		spec = &s
	}
	order := table.UnsortedSortOrder
	if req.WriteOrder != nil {
		order = *req.WriteOrder
	}
	loc := req.Location
	if loc == "" {
		// A fresh directory per table, as Lakekeeper names them by ID.
		loc = c.location + "/" + uuid.NewString()
	}
	if strings.HasPrefix(loc, "/") {
		// DuckDB makes no directories: object stores need none.
		for _, dir := range []string{"data", "metadata"} {
			if err := os.MkdirAll(loc+"/"+dir, 0o755); err != nil {
				apiError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
				return
			}
		}
	}
	meta, err := table.NewMetadata(req.Schema, spec, order, loc, req.Properties)
	if err != nil {
		apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}
	t := &tableState{ns: ns, name: req.Name, meta: meta}
	t.metaLoc = t.nextMetadataLocation()
	if err := writeMetadataFile(t); err != nil {
		apiError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	if !req.StageCreate {
		c.tables[k] = t
		c.remember(key)
	}
	writeJSON(w, http.StatusOK, loadResult(t))
}

type identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type tableChange struct {
	Identifier   *identifier        `json:"identifier"`
	Requirements table.Requirements `json:"requirements"`
	Updates      table.Updates      `json:"updates"`
}

// commit applies changes to their tables, all of them or none.
func (c *Catalog) commit(changes []tableChange) (int, string, error) {
	type result struct {
		key   string
		state *tableState
	}
	var results []result
	for _, ch := range changes {
		if ch.Identifier == nil {
			return http.StatusBadRequest, "BadRequestException", errors.New("a table change names no table")
		}
		k := c.tableKey(ch.Identifier.Namespace, ch.Identifier.Name)
		old := c.tables[k]
		var base table.Metadata
		if old != nil {
			base = old.meta
		}
		for _, req := range ch.Requirements {
			if err := req.Validate(base); err != nil {
				return http.StatusConflict, "CatalogCommitConflicts", fmt.Errorf("requirement failed: %w", err)
			}
		}
		meta, err := apply(base, old, ch.Updates)
		if err != nil {
			return http.StatusBadRequest, "BadRequestException", err
		}
		t := &tableState{ns: ch.Identifier.Namespace, name: ch.Identifier.Name, meta: meta}
		if old != nil {
			// A commit naming the table in another case leaves its name.
			t.ns, t.name, t.versions = old.ns, old.name, old.versions
		}
		t.metaLoc = t.nextMetadataLocation()
		if err := writeMetadataFile(t); err != nil {
			return http.StatusInternalServerError, "InternalServerError", err
		}
		results = append(results, result{k, t})
	}
	for _, r := range results {
		c.tables[r.key] = r.state
	}
	c.commits++
	return 0, "", nil
}

// apply applies updates to a table's metadata, or makes a new table's
// (base nil) from them.
func apply(base table.Metadata, old *tableState, updates table.Updates) (table.Metadata, error) {
	var snapshots, rest table.Updates
	for _, u := range updates {
		switch u.Action() {
		case table.UpdateAddSnapshot, table.UpdateSetSnapshotRef, table.UpdateRemoveSnapshots, table.UpdateRemoveSnapshotRef:
			snapshots = append(snapshots, u)
		default:
			rest = append(rest, u)
		}
	}
	if base == nil {
		// iceberg-go needs a new table's schema before its partition spec
		// and sort order, which DuckDB sends in another order, and adds
		// snapshots only on top of built metadata.
		slices.SortStableFunc(rest, func(a, b table.Update) int {
			return createOrder(a.Action()) - createOrder(b.Action())
		})
		b, err := table.NewMetadataBuilder(1)
		if err != nil {
			return nil, err
		}
		for _, u := range rest {
			if err := u.Apply(b); err != nil {
				return nil, fmt.Errorf("%s: %w", u.Action(), err)
			}
		}
		if base, err = b.Build(); err != nil {
			return nil, err
		}
		rest = nil
	}
	loc := ""
	if old != nil {
		loc = old.metaLoc
	}
	b, err := table.MetadataBuilderFromBase(base, loc)
	if err != nil {
		return nil, err
	}
	for _, u := range append(rest, snapshots...) {
		if err := u.Apply(b); err != nil {
			return nil, fmt.Errorf("%s: %w", u.Action(), err)
		}
	}
	return b.Build()
}

// createOrder ranks a new table's updates in the order iceberg-go can
// apply them.
func createOrder(action string) int {
	order := []string{
		table.UpdateAssignUUID, table.UpdateUpgradeFormatVersion, table.UpdateAddSchema, table.UpdateSetCurrentSchema,
		table.UpdateAddSpec, table.UpdateSetDefaultSpec, table.UpdateAddSortOrder, table.UpdateSetDefaultSortOrder,
	}
	if i := slices.Index(order, action); i >= 0 {
		return i
	}
	return len(order)
}

// writeMetadataFile writes t's metadata at its metadata location, when that
// is a local path, as a real catalog writes it before it answers: DuckDB
// reads a table's metadata from the file at times, as when another
// connection of the same instance has just committed to the table, and
// failed with "Cannot open file ...metadata.json" when it was not there.
func writeMetadataFile(t *tableState) error {
	loc := strings.TrimPrefix(t.metaLoc, "file://")
	if !strings.HasPrefix(loc, "/") {
		return nil
	}
	b, err := json.Marshal(t.meta)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(path.Dir(loc), 0o755); err != nil {
		return err
	}
	return os.WriteFile(loc, b, 0o644)
}

func (t *tableState) nextMetadataLocation() string {
	t.versions++
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", t.meta.Location(), t.versions, uuid.NewString())
}

func loadResult(t *tableState) map[string]any {
	return map[string]any{"metadata-location": t.metaLoc, "metadata": t.meta, "config": map[string]string{}}
}

func commitResponse(t *tableState) map[string]any {
	return map[string]any{"metadata-location": t.metaLoc, "metadata": t.meta}
}

// tableKey is the key of the table or view ns.name. c.mu is held.
func (c *Catalog) tableKey(ns []string, name string) string {
	if c.ignoreCase {
		name = strings.ToLower(name)
	}
	return path.Join(strings.Join(ns, "\x1f"), name)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

func apiError(w http.ResponseWriter, status int, typ, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": status}})
}
