package icebergrest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/deletefiles"
)

// schemaOf names a namespace as grants do. Only admins may use nested
// namespaces, which SQL does not show, so grants never name one; the
// history names them with their levels joined by dots, which no level
// contains.
func schemaOf(ns []string) string { return strings.Join(ns, ".") }

// validName reports whether s may name a table. "." and ".." would be path
// segments that move up Lakekeeper's API.
func validName(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsFunc(s, unicode.IsControl)
}

// validLevel reports whether s may name a namespace level. Levels with dots
// would make ["a", "b"] and ["a.b"] one schema to grants; Lakekeeper refuses
// them too.
func validLevel(s string) bool { return validName(s) && !strings.Contains(s, ".") }

// pathName reads a namespace level or table name in a request's path as
// Lakekeeper does, which turns "+" into a space: Iceberg's Java client
// form-encodes names, spaces as "+", and Lakekeeper refuses "+" in names.
// Read otherwise, a request could be checked for one table and act on
// another.
func pathName(s string) string { return strings.ReplaceAll(s, "+", " ") }

// pathNamespace is the namespace in a request's path: levels separated by
// the unit separator (%1F).
func pathNamespace(r *http.Request) ([]string, error) {
	ns := strings.Split(pathName(r.PathValue("namespace")), "\x1f")
	if !allValid(ns) {
		return nil, errorf(http.StatusBadRequest, errBadRequest, "malformed namespace %q", r.PathValue("namespace"))
	}
	return ns, nil
}

func allValid(levels []string) bool {
	return len(levels) > 0 && !slices.ContainsFunc(levels, func(n string) bool { return !validLevel(n) })
}

// pathTable is the namespace and table in a request's path.
func pathTable(r *http.Request) ([]string, auth.Object, error) {
	ns, err := pathNamespace(r)
	if err != nil {
		return nil, auth.Object{}, err
	}
	name := pathName(r.PathValue("table"))
	if !validName(name) {
		return nil, auth.Object{}, errorf(http.StatusBadRequest, errBadRequest, "malformed table name %q", r.PathValue("table"))
	}
	return ns, auth.Object{Schema: schemaOf(ns), Table: name}, nil
}

// namespace is the namespace in c's path, which must be one c may use.
func (c *call) namespace() ([]string, error) {
	ns, err := pathNamespace(c.r)
	if err != nil {
		return nil, err
	}
	return ns, c.who.requireTopLevel(ns)
}

// table is the namespace and table in c's path, in a namespace c may use.
func (c *call) table() ([]string, auth.Object, error) {
	ns, o, err := pathTable(c.r)
	if err != nil {
		return nil, auth.Object{}, err
	}
	return ns, o, c.who.requireTopLevel(ns)
}

// requireTopLevel refuses non-admins a nested namespace. Igloo's SQL shows
// top-level schemas only, so grants cannot name nested ones, nor SQL
// clients see their tables.
func (c *caller) requireTopLevel(ns []string) error {
	if len(ns) == 1 || c.admin() {
		return nil
	}
	return c.deny("only admins may use nested namespaces, which Igloo's SQL does not show")
}

// identifier is a TableIdentifier.
type identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

func (id identifier) object() auth.Object {
	return auth.Object{Schema: schemaOf(id.Namespace), Table: id.Name}
}

func (id identifier) valid() bool { return validName(id.Name) && allValid(id.Namespace) }

// is reports whether id names the table name in namespace ns, exactly, as
// Lakekeeper compares them.
func (id identifier) is(ns []string, name string) bool {
	return slices.Equal(id.Namespace, ns) && id.Name == name
}

// vendRequested reports whether the client asked for storage credentials.
func vendRequested(r *http.Request) bool {
	for _, v := range strings.Split(r.Header.Get(accessDelegation), ",") {
		if strings.TrimSpace(v) == vendedCredentials {
			return true
		}
	}
	return false
}

// config serves GET /v1/config. Igloo serves one warehouse, with no path
// prefix.
func (s *Server) config(c *call) error {
	if w := c.r.URL.Query().Get("warehouse"); w != "" && w != s.opts.Warehouse {
		return errorf(http.StatusNotFound, errNoSuchWarehouse, "this catalog serves one warehouse, %q", s.opts.Warehouse)
	}
	writeJSON(c.w, http.StatusOK, map[string]any{
		"defaults":  map[string]string{},
		"overrides": map[string]string{},
		"endpoints": endpoints,
	})
	return nil
}

func (s *Server) listNamespaces(c *call) error {
	r, err := s.cat.call(c.ctx(), http.MethodGet, "namespaces", pick(c.r.URL.Query(), "parent", "pageToken", "pageSize"), nil)
	if err != nil {
		return err
	}
	var list struct {
		Namespaces [][]string      `json:"namespaces"`
		Next       json.RawMessage `json:"next-page-token,omitempty"`
	}
	if err := json.Unmarshal(r.body, &list); err != nil {
		return err
	}
	list.Namespaces = slices.DeleteFunc(list.Namespaces, func(ns []string) bool {
		return c.who.requireTopLevel(ns) != nil || !c.who.seesSchema(schemaOf(ns))
	})
	if list.Namespaces == nil {
		list.Namespaces = [][]string{}
	}
	writeJSON(c.w, http.StatusOK, list)
	return nil
}

func (s *Server) createNamespace(c *call) error {
	var req struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	body, err := readJSON(c, &req)
	if err != nil {
		return err
	}
	if !allValid(req.Namespace) {
		return errorf(http.StatusBadRequest, errBadRequest, "missing or malformed namespace")
	}
	c.written = []auth.Object{{Schema: schemaOf(req.Namespace)}}
	if err := c.who.requireWriter("creating schemas"); err != nil {
		return err
	}
	if err := c.who.requireTopLevel(req.Namespace); err != nil {
		return err
	}
	if err := c.who.checkLocation(keys(req.Properties)); err != nil {
		return err
	}
	if err := c.who.checkOwnProperties(keys(req.Properties)); err != nil {
		return err
	}
	r, err := s.cat.call(c.ctx(), http.MethodPost, "namespaces", nil, body)
	if err != nil {
		return err
	}
	s.recordOwner(c, c.written[0])
	writeBody(c.w, r.status, r.body)
	return nil
}

func (s *Server) loadNamespace(c *call) error {
	ns, err := c.namespace()
	if err != nil {
		return err
	}
	if err := c.who.requireSeesSchema(schemaOf(ns)); err != nil {
		return s.unlessMissing(c, ns, "", err)
	}
	r, err := s.cat.call(c.ctx(), http.MethodGet, namespacePathOf(ns), nil, nil)
	if err != nil {
		return err
	}
	writeBody(c.w, r.status, r.body)
	return nil
}

func (s *Server) namespaceExists(c *call) error {
	ns, err := c.namespace()
	if err != nil {
		return err
	}
	if err := c.who.requireSeesSchema(schemaOf(ns)); err != nil {
		return s.unlessMissing(c, ns, "", err)
	}
	r, err := s.cat.call(c.ctx(), http.MethodHead, namespacePathOf(ns), nil, nil)
	if err != nil {
		return err
	}
	c.w.WriteHeader(r.status)
	return nil
}

func (s *Server) dropNamespace(c *call) error {
	ns, err := c.namespace()
	if err != nil {
		return err
	}
	c.written = []auth.Object{{Schema: schemaOf(ns)}}
	if err := c.who.requireWriter("dropping schemas"); err != nil {
		return err
	}
	if _, err := s.cat.call(c.ctx(), http.MethodDelete, namespacePathOf(ns), nil, nil); err != nil {
		return err
	}
	// Grants go with what they name, as with DROP SCHEMA.
	s.forget(c, auth.Object{Schema: schemaOf(ns)})
	c.w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) updateNamespace(c *call) error {
	ns, err := c.namespace()
	if err != nil {
		return err
	}
	var req struct {
		Removals []string          `json:"removals"`
		Updates  map[string]string `json:"updates"`
	}
	body, err := readJSON(c, &req)
	if err != nil {
		return err
	}
	c.written = []auth.Object{{Schema: schemaOf(ns)}}
	if err := c.who.requireWriter("changing schema properties"); err != nil {
		return err
	}
	if err := c.who.checkLocation(append(keys(req.Updates), req.Removals...)); err != nil {
		return err
	}
	if err := c.who.checkOwnProperties(append(keys(req.Updates), req.Removals...)); err != nil {
		return err
	}
	r, err := s.cat.call(c.ctx(), http.MethodPost, namespacePathOf(ns)+"/properties", nil, body)
	if err != nil {
		return err
	}
	writeBody(c.w, r.status, r.body)
	return nil
}

// checkLocation refuses non-admins a schema's location. Lakekeeper places
// tables under their schema's location, so choosing it would choose where
// tables' files go, possibly among old files of dropped ones.
func (c *caller) checkLocation(props []string) error {
	if c.admin() || !slices.Contains(props, "location") {
		return nil
	}
	return c.deny("only admins may choose a schema's location")
}

// checkOwnProperties refuses everyone a schema's properties that are Igloo's
// own (igloo.*), which only Igloo writes, reaching the catalog itself: its
// notes of what stopped processes left (see catalog.Pending) say which tables
// and files it may drop and delete.
func (c *caller) checkOwnProperties(props []string) error {
	if !slices.ContainsFunc(props, func(p string) bool { return strings.HasPrefix(strings.ToLower(p), "igloo.") }) {
		return nil
	}
	return c.deny("a schema's igloo.* properties are Igloo's own")
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func (s *Server) listTables(c *call) error {
	ns, err := c.namespace()
	if err != nil {
		return err
	}
	if err := c.who.requireSeesSchema(schemaOf(ns)); err != nil {
		return s.unlessMissing(c, ns, "", err)
	}
	r, err := s.cat.call(c.ctx(), http.MethodGet, namespacePathOf(ns)+"/tables", pick(c.r.URL.Query(), "pageToken", "pageSize"), nil)
	if err != nil {
		return err
	}
	var list struct {
		Identifiers []identifier    `json:"identifiers"`
		Next        json.RawMessage `json:"next-page-token,omitempty"`
	}
	if err := json.Unmarshal(r.body, &list); err != nil {
		return err
	}
	list.Identifiers = slices.DeleteFunc(list.Identifiers, func(id identifier) bool { return !c.who.sees(id.object()) })
	if list.Identifiers == nil {
		list.Identifiers = []identifier{}
	}
	writeJSON(c.w, http.StatusOK, list)
	return nil
}

func (s *Server) createTable(c *call) error {
	ns, err := c.namespace()
	if err != nil {
		return err
	}
	var req struct {
		Name       string            `json:"name"`
		Location   string            `json:"location"`
		Properties map[string]string `json:"properties"`
	}
	body, err := readJSON(c, &req)
	if err != nil {
		return err
	}
	if !validName(req.Name) {
		return errorf(http.StatusBadRequest, errBadRequest, "missing or malformed table name")
	}
	o := auth.Object{Schema: schemaOf(ns), Table: req.Name}
	c.written = []auth.Object{o}
	if err := c.who.require(auth.PrivilegeCreate, auth.Object{Schema: o.Schema}); err != nil {
		return err
	}
	// Lakekeeper gives each table a new location under its schema's. A
	// chosen one could hold old files of a dropped table, which the
	// table's credentials would then reach.
	if req.Location != "" && !c.who.admin() {
		return c.who.deny("only admins may choose a table's location; leave it out")
	}
	if err := checkProperties(keys(req.Properties)); err != nil {
		return err
	}
	if err := checkCloneProperties(keys(req.Properties)); err != nil {
		return err
	}
	// Policies name the tables they protect.
	if err := s.requireUnprotected(c, o); err != nil {
		return err
	}
	if err := s.checkCreateFormat(c, o, req.Properties); err != nil {
		return err
	}
	r, err := s.cat.call(c.ctx(), http.MethodPost, namespacePathOf(ns)+"/tables", nil, body)
	if err != nil {
		return err
	}
	s.recordOwner(c, o)
	// The new table is empty, and its creator fills it, as with CREATE
	// TABLE AS.
	return s.writeTable(c, ns, o, r, true)
}

func (s *Server) loadTable(c *call) error {
	ns, o, err := c.table()
	if err != nil {
		return err
	}
	c.read = []auth.Object{o}
	if err := c.who.require(auth.PrivilegeSelect, o); err != nil {
		return s.unlessMissing(c, ns, o.Table, err)
	}
	if err := s.requireUnprotected(c, o); err != nil {
		return err
	}
	r, err := s.cat.call(c.ctx(), http.MethodGet, tablePathOf(ns, o.Table), pick(c.r.URL.Query(), "snapshots"), nil)
	if err != nil {
		return err
	}
	return s.writeTable(c, ns, o, r, c.who.writesData(o))
}

// writeTable sends a LoadTableResult with Igloo's credentials for the
// table, if the client asked for them.
func (s *Server) writeTable(c *call, ns []string, o auth.Object, r *reply, write bool) error {
	result, err := parseLoadResult(r.body)
	if err != nil {
		return err
	}
	var creds *Credentials
	if vendRequested(c.r) {
		if creds, err = s.vend(c, ns, o, result, write); err != nil {
			return err
		}
	}
	body, err := result.marshal(creds, "v1/"+tablePathOf(ns, o.Table)+"/credentials")
	if err != nil {
		return err
	}
	writeBody(c.w, r.status, body)
	return nil
}

// vend gets credentials for table o, loaded as t, in namespace ns, or nil
// when none can be given.
func (s *Server) vend(c *call, ns []string, o auth.Object, t *loadResult, write bool) (*Credentials, error) {
	if s.opts.Credentials == nil {
		return nil, nil
	}
	if err := s.cloneCredentials(o, t); err != nil {
		return nil, err
	}
	location := t.location
	if !c.who.admin() {
		assigned, err := s.assignedLocation(c, ns, t)
		if err != nil {
			return nil, err
		}
		if !assigned {
			s.log.Warn("no storage credentials for a table that is not where the catalog put it",
				"location", location, "uuid", t.uuid, "namespace", schemaOf(ns))
			return nil, nil
		}
	}
	access := ReadFiles
	if write {
		access = s.writeAccess(c, location)
	}
	creds, err := s.opts.Credentials.Vend(c.ctx(), location, access, c.who.user.Name)
	if errors.Is(err, ErrNoCredentials) {
		return nil, nil
	}
	if err != nil {
		s.log.Warn("could not vend storage credentials", "location", location, "err", err)
		return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "could not get storage credentials for the table; try again")
	}
	c.detail = access.credentials()
	return &creds, nil
}

// assignedLocation reports whether table t is where Lakekeeper put it when
// it was created: in its namespace's location, in a directory named for its
// UUID. Lakekeeper refuses to move a table or change its UUID, and Igloo
// lets only admins ask; credentials for a table anywhere else could reach
// another table's files, or a directory above or below another table's.
func (s *Server) assignedLocation(c *call, ns []string, t *loadResult) (bool, error) {
	r, err := s.cat.call(c.ctx(), http.MethodGet, namespacePathOf(ns), nil, nil)
	if err != nil {
		return false, err
	}
	var meta struct {
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(r.body, &meta); err != nil {
		return false, fmt.Errorf("catalog returned malformed namespace metadata: %w", err)
	}
	base := strings.TrimSuffix(meta.Properties["location"], "/")
	return base != "" && validName(t.uuid) && !strings.Contains(t.uuid, "/") &&
		strings.TrimSuffix(t.location, "/") == base+"/"+t.uuid, nil
}

func (s *Server) tableExists(c *call) error {
	ns, o, err := c.table()
	if err != nil {
		return err
	}
	if err := c.who.requireSees(o); err != nil {
		return s.unlessMissing(c, ns, o.Table, err)
	}
	r, err := s.cat.call(c.ctx(), http.MethodHead, tablePathOf(ns, o.Table), nil, nil)
	if err != nil {
		return err
	}
	c.w.WriteHeader(r.status)
	return nil
}

func (s *Server) updateTable(c *call) error {
	ns, o, err := c.table()
	if err != nil {
		return err
	}
	var req commitRequest
	body, err := readJSON(c, &req)
	if err != nil {
		return err
	}
	c.written = []auth.Object{o}
	// Lakekeeper commits to the body's table when it differs from the
	// path's in one way (a Spark branch commit), so Igloo would check one
	// table and Lakekeeper change another.
	if req.Identifier != nil && !req.Identifier.is(ns, o.Table) {
		return errorf(http.StatusBadRequest, errBadRequest, "the table identifier in the request body differs from the table in the path")
	}
	n := classify(req)
	c.detail = strings.Join(n.summary, ", ")
	if err := c.who.allowsCommit(o, n); err != nil {
		return err
	}
	if err := checkProperties(req.propertyKeys()); err != nil {
		return err
	}
	if err := checkCloneProperties(append(req.propertyKeys(), req.removedKeys()...)); err != nil {
		return err
	}
	if err := s.checkManifestLists(c, ns, o, req); err != nil {
		return err
	}
	if err := s.requireUnprotected(c, o); err != nil {
		return err
	}
	if err := s.checkErasing(c, o, req); err != nil {
		return err
	}
	if err := s.checkClassifiedSchema(c, ns, o, req); err != nil {
		return err
	}
	// The staged table already has the UUID and location Lakekeeper gave
	// it, which clients restate when they create it. Lakekeeper refuses
	// others, and Igloo leaves them out, so they cannot name another
	// table's.
	if n.creates && !c.who.admin() {
		if body, err = withoutUpdates(body, stagedUpdates...); err != nil {
			return err
		}
	}
	// Creating needs only CREATE, so it must not touch an existing table.
	// Lakekeeper refuses that too.
	if n.creates {
		if _, err := s.cat.call(c.ctx(), http.MethodHead, tablePathOf(ns, o.Table), nil, nil); err == nil {
			return errorf(http.StatusConflict, "CommitFailedException", "table %s already exists", o)
		}
	}
	r, err := s.cat.call(c.ctx(), http.MethodPost, tablePathOf(ns, o.Table), nil, body)
	if err != nil {
		return err
	}
	writeBody(c.w, r.status, r.body)
	return nil
}

func (s *Server) dropTable(c *call) error {
	ns, o, err := c.table()
	if err != nil {
		return err
	}
	c.written = []auth.Object{o}
	if err := c.who.require(auth.PrivilegeCreate, auth.Object{Schema: o.Schema}); err != nil {
		return err
	}
	// A table with data policies is read and managed through Igloo, not other
	// engines: dropping it here (and purging its files) would be a bypass, so
	// it is refused for non-admins like the other operations on it; a clean
	// room's dataset is its party's.
	if err := s.requireUnprotected(c, o); err != nil {
		return err
	}
	if err := s.refuseErasing(c, o, "it cannot be dropped"); err != nil {
		return err
	}
	// Purging deletes every file under the table's location, those that
	// clones read included (see clones.go). A dropped table's files go
	// with it, as in SQL, when they may, so that they do not linger;
	// erasure requests delete whatever a drop leaves (package erasure).
	query := pick(c.r.URL.Query(), "purgeRequested")
	if s.opts.Clones != nil {
		v, err := s.purgeVerdict(c, tablePathOf(ns, o.Table), append(slices.Clone(ns), o.Table))
		switch {
		case err != nil && purgeRequested(c):
			return err
		case err != nil:
			s.log.Warn("could not load a dropped table to tell whether its files may go; it is dropped without them",
				"table", o.String(), "err", err)
		case v.Purge:
			defer v.Release()
			query.Set("purgeRequested", "true")
			c.detail = "purge"
		case purgeRequested(c):
			return purgeRefusal(v)
		}
	}
	if _, err := s.cat.call(c.ctx(), http.MethodDelete, tablePathOf(ns, o.Table), query, nil); err != nil {
		return err
	}
	// A table created later under the name starts with no grants.
	s.forget(c, o)
	c.w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) renameTable(c *call) error {
	var req struct {
		Source      identifier `json:"source"`
		Destination identifier `json:"destination"`
	}
	body, err := readJSON(c, &req)
	if err != nil {
		return err
	}
	if !req.Source.valid() || !req.Destination.valid() {
		return errorf(http.StatusBadRequest, errBadRequest, "missing source or destination")
	}
	from, to := req.Source.object(), req.Destination.object()
	c.written = []auth.Object{from, to}
	for _, ns := range [][]string{req.Source.Namespace, req.Destination.Namespace} {
		if err := c.who.requireTopLevel(ns); err != nil {
			return err
		}
	}
	if err := c.who.require(auth.PrivilegeCreate, auth.Object{Schema: from.Schema}); err != nil {
		return err
	}
	// Grants on the other schema would then reach the table: moving a
	// table out of a schema could hand it to whoever may read another.
	// SQL cannot move tables between schemas either. Lakekeeper compares
	// names without case, as grants do.
	if !strings.EqualFold(from.Schema, to.Schema) && !c.who.admin() {
		return c.who.deny("only admins may move a table to another schema")
	}
	// Policies name the tables they protect, so a protected table renamed
	// would lose them, and a table renamed to a protected name gain them.
	for _, o := range []auth.Object{from, to} {
		if err := s.requireUnprotected(c, o); err != nil {
			return err
		}
	}
	// An erasure request finds its tables by name: one renamed would be
	// out of its reach, and one renamed to its name taken for it.
	if err := s.refuseErasing(c, from, "it cannot be renamed or moved"); err != nil {
		return err
	}
	if err := s.refuseErasing(c, to, "no other table can take its name"); err != nil {
		return err
	}
	// Tags and data retentions name tables too: a renamed table would be
	// out of reach of erasure requests and its retention.
	if s.opts.Classified != nil {
		_, classified, err := s.opts.Classified(c.ctx(), from)
		if err != nil {
			return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check the table's policies and tags; try again")
		}
		if classified {
			return c.who.deny(fmt.Sprintf("table %s has policies, tags or a data retention, which stay with its name; unset them before renaming it", from))
		}
	}
	if _, err := s.cat.call(c.ctx(), http.MethodPost, "tables/rename", nil, body); err != nil {
		return err
	}
	// Grants follow a renamed table, as with ALTER TABLE ... RENAME TO.
	if err := s.opts.Grants.RenameObject(c.ctx(), from, to); err != nil {
		s.log.Warn("could not move grants to a renamed table", "from", from.String(), "to", to.String(), "err", err)
	}
	c.w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) loadCredentials(c *call) error {
	ns, o, err := c.table()
	if err != nil {
		return err
	}
	c.read = []auth.Object{o}
	if err := c.who.require(auth.PrivilegeSelect, o); err != nil {
		return s.unlessMissing(c, ns, o.Table, err)
	}
	if err := s.requireUnprotected(c, o); err != nil {
		return err
	}
	r, err := s.cat.call(c.ctx(), http.MethodGet, tablePathOf(ns, o.Table), pick(c.r.URL.Query(), "snapshots"), nil)
	if err != nil {
		return err
	}
	result, err := parseLoadResult(r.body)
	if err != nil {
		return err
	}
	creds, err := s.vend(c, ns, o, result, c.who.writesData(o))
	if err != nil {
		return err
	}
	list := []map[string]any{}
	if creds != nil {
		list = creds.storageCredentials()
	}
	writeJSON(c.w, http.StatusOK, map[string]any{"storage-credentials": list})
	return nil
}

// reportMetrics accepts an engine's scan and commit reports, and drops
// them.
func (s *Server) reportMetrics(c *call) error {
	_, _ = io.Copy(io.Discard, http.MaxBytesReader(c.w, c.r.Body, maxRequestBody))
	c.w.WriteHeader(http.StatusNoContent)
	return nil
}

// unlessMissing returns refusal, the reason c may not use a table (or with
// an empty table, a namespace), unless it does not exist: then a 404 says
// so. Names are no secret (SQL clients see every table's), and engines
// learn whether a table exists by loading it, as before CREATE TABLE AS.
func (s *Server) unlessMissing(c *call, ns []string, table string, refusal error) error {
	path, kind, name := namespacePathOf(ns), "NoSuchNamespaceException", "namespace "+schemaOf(ns)
	if table != "" {
		path, kind, name = tablePathOf(ns, table), "NoSuchTableException", "table "+schemaOf(ns)+"."+table
	}
	_, err := s.cat.call(c.ctx(), http.MethodHead, path, nil, nil)
	var api *apiError
	if errors.As(err, &api) && api.status == http.StatusNotFound {
		return errorf(http.StatusNotFound, kind, "%s does not exist", name)
	}
	return refusal
}

// requireUnprotected refuses non-admins table o if it has data policies,
// or a clean room keeps it to others. Igloo enforces masking and row
// access policies by rewriting the SQL it runs; an engine with the table's
// metadata and files reads the raw data, so such tables are read through
// Igloo, as Unity Catalog does for engines outside it. Admins, whom
// policies do not bind, are not refused.
func (s *Server) requireUnprotected(c *call, o auth.Object) error {
	if s.opts.Protected == nil || c.who.admin() {
		return nil
	}
	protected, err := s.opts.Protected(c.ctx(), c.who.user, o)
	if err != nil {
		s.log.Warn("could not check a table's data policies", "table", o.String(), "err", err)
		return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check the table's data policies; try again")
	}
	if protected {
		return c.who.deny(fmt.Sprintf("table %s has data policies (masking or row access), or a clean room keeps it to its party, "+
			"which other engines would bypass; read it through Igloo's SQL or Flight SQL", o))
	}
	return nil
}

// recordOwner makes the caller the owner of the schema or table they
// created. A table's owner may set its comments and hand it on, and
// nothing else (see auth.Ownership).
func (s *Server) recordOwner(c *call, o auth.Object) {
	if s.opts.Owners == nil {
		return
	}
	user := c.who.user.Name
	err := s.opts.Owners.SetObjectOwner(context.WithoutCancel(c.ctx()),
		auth.Ownership{Object: o, Owner: user, OwnerType: auth.GranteeUser, SetBy: user}, false)
	if err != nil {
		s.log.Warn("could not record the owner of a new "+o.Kind(), "object", o.String(), "owner", user, "err", err)
	}
}

// checkClassifiedSchema refuses a commit that renames or drops a column of
// table o with policies or tags, or changes its type, or adds another
// column under its name, as ALTER TABLE in SQL is refused: they stay with
// the column's name, and a column that changed under them would be out of
// reach of its policies and of erasure requests.
func (s *Server) checkClassifiedSchema(c *call, ns []string, o auth.Object, req commitRequest) error {
	if s.opts.Classified == nil || !upgradesFormat(req) && !slices.ContainsFunc(req.Updates, func(u tableUpdate) bool {
		return u.Action == "add-schema" || u.Action == "set-current-schema" || u.Action == "remove-schemas"
	}) {
		return nil
	}
	columns, _, err := s.opts.Classified(c.ctx(), o)
	if err != nil {
		return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check the table's policies and tags; try again")
	}
	if len(columns) == 0 {
		return nil
	}
	r, err := s.cat.call(c.ctx(), http.MethodGet, tablePathOf(ns, o.Table), nil, nil)
	if err != nil {
		return err
	}
	var loaded struct {
		Metadata struct {
			Current int          `json:"current-schema-id"`
			Schemas []schemaJSON `json:"schemas"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(r.body, &loaded); err != nil {
		return fmt.Errorf("catalog returned malformed table metadata: %w", err)
	}
	schemas := loaded.Metadata.Schemas
	byID := func(id int) *schemaJSON {
		for i := range schemas {
			if schemas[i].ID == id {
				return &schemas[i]
			}
		}
		return nil
	}
	current := byID(loaded.Metadata.Current)
	if current == nil {
		return nil
	}
	refuse := func(name string) error {
		if name == "" {
			name = strings.Join(columns, ", ")
		}
		return c.who.deny(fmt.Sprintf("column %s of %s has a policy or tags, which stay with its name; "+
			"unset them before renaming or dropping it or changing its type", name, o))
	}
	// lost is the first classified column of the current schema that next
	// does not keep as the same field, by name and type, or "".
	lost := func(next *schemaJSON) string {
		for _, name := range columns {
			id, _, typ, ok := current.field(0, name)
			if !ok {
				continue
			}
			_, newName, newTyp, found := next.field(id, "")
			otherID, _, _, other := next.field(0, name)
			if !found || !strings.EqualFold(newName, name) || newTyp != typ || other && otherID != id {
				return name
			}
		}
		return ""
	}
	// Every schema the commit adds is checked, whatever ID it claims:
	// catalogs assign their own, so set-current-schema may name an added
	// schema by an ID the request never wrote. An ID that names none of the
	// table's schemas can only be one of those. Old schemas name the
	// columns' fields as they were, which erasure requests find values by,
	// so none is removed.
	added := false
	for _, u := range req.Updates {
		switch u.Action {
		case "upgrade-format-version", "set-properties":
			// Erasure requests cannot rewrite tables of format version 3,
			// whose deletion vectors Igloo cannot read yet.
			if upgradesFormat(commitRequest{Updates: []tableUpdate{u}}) {
				return c.who.deny(fmt.Sprintf("%s has policies or tags; it stays at Iceberg format version 2, as erasure requests "+
					"cannot rewrite tables of version 3 yet", o))
			}
		case "remove-schemas":
			return c.who.deny(fmt.Sprintf("%s has policies or tags, whose columns its old schemas name; its schemas are not removed", o))
		case "add-schema":
			if u.Schema == nil {
				return refuse("")
			}
			if name := lost(u.Schema); name != "" {
				return refuse(name)
			}
			added = true
		case "set-current-schema":
			switch {
			case u.SchemaID == nil:
				return refuse("")
			case *u.SchemaID == -1 || byID(*u.SchemaID) == nil:
				if !added {
					return refuse("")
				}
			default:
				if name := lost(byID(*u.SchemaID)); name != "" {
					return refuse(name)
				}
			}
		}
	}
	return nil
}

// upgradesFormat reports whether req may upgrade a table to Iceberg format
// version 3 or later: by upgrade-format-version, or by setting the
// format-version property, as some catalogs take it, to a version that is
// not below 3, or that cannot be told.
func upgradesFormat(req commitRequest) bool {
	return slices.ContainsFunc(req.Updates, func(u tableUpdate) bool {
		switch u.Action {
		case "upgrade-format-version":
			return u.FormatVersion == nil || *u.FormatVersion >= 3
		case "set-properties":
			for k, v := range u.Updates {
				if strings.EqualFold(k, "format-version") {
					if n, err := strconv.Atoi(strings.TrimSpace(v)); err != nil || n >= 3 {
						return true
					}
				}
			}
		}
		return false
	})
}

// checkManifestLists refuses a commit to table o that adds a snapshot
// whose manifest list is not in the warehouse's storage: no engine could
// read the snapshot, and erasure requests, which read every snapshot of
// the tables they erase from, could not. A catalog takes such a snapshot
// (Lakekeeper does); Igloo does not, so that no writer can make a table
// unreadable to erasure. A table in the warehouse keeps its manifest lists
// there, however their paths spell it; one elsewhere is not Igloo's to
// check.
func (s *Server) checkManifestLists(c *call, ns []string, o auth.Object, req commitRequest) error {
	if s.opts.Exists == nil || s.opts.StorageRoot == "" || !slices.ContainsFunc(req.Updates, func(u tableUpdate) bool {
		return u.Action == "add-snapshot" && u.Snapshot != nil
	}) {
		return nil
	}
	root := deletefiles.Canonical(s.opts.StorageRoot) + "/"
	inWarehouse := false
	if r, err := s.cat.call(c.ctx(), http.MethodGet, tablePathOf(ns, o.Table), nil, nil); err == nil {
		if t, err := parseLoadResult(r.body); err == nil {
			inWarehouse = strings.HasPrefix(deletefiles.Canonical(t.location)+"/", root)
		}
	}
	for _, u := range req.Updates {
		// A snapshot of format version 1 may list its manifests instead.
		if u.Action != "add-snapshot" || u.Snapshot == nil || u.Snapshot.ManifestList == "" {
			continue
		}
		list := deletefiles.Canonical(u.Snapshot.ManifestList)
		if !strings.HasPrefix(list, root) {
			if inWarehouse {
				return errorf(http.StatusBadRequest, errBadRequest, "snapshot %d's manifest list %s is outside the warehouse, "+
					"which holds the table's files: write it under the table's location", u.Snapshot.ID, u.Snapshot.ManifestList)
			}
			continue
		}
		// As the warehouse's storage spells it.
		list = strings.TrimSuffix(s.opts.StorageRoot, "/") + "/" + strings.TrimPrefix(list, root)
		ok, err := s.opts.Exists(c.ctx(), list)
		if err != nil {
			s.log.Warn("could not check that a snapshot's manifest list exists", "snapshot", u.Snapshot.ID, "err", err)
			return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check that snapshot %d's manifest list exists; try again", u.Snapshot.ID)
		}
		if !ok {
			return errorf(http.StatusBadRequest, errBadRequest, "snapshot %d's manifest list %s does not exist: write it before committing the snapshot",
				u.Snapshot.ID, u.Snapshot.ManifestList)
		}
	}
	return nil
}

// checkErasing refuses a commit that may upgrade table o to Iceberg format
// version 3 while an erasure request erases from it, whether its columns
// have tags or not (a clone of a tagged table, or one that shares its
// files): requests cannot rewrite tables of version 3 yet, and one that has
// begun deleting from the table could not go on.
func (s *Server) checkErasing(c *call, o auth.Object, req commitRequest) error {
	if s.opts.Erasing == nil || !upgradesFormat(req) {
		return nil
	}
	erasing, err := s.opts.Erasing(c.ctx(), o)
	if err != nil {
		return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check whether an erasure request erases from the table; try again")
	}
	if erasing {
		return c.who.deny(fmt.Sprintf("an erasure request erases from %s; it stays at its Iceberg format version until the request is over, "+
			"as erasure requests cannot rewrite tables of version 3 yet", o))
	}
	return nil
}

// refuseErasing refuses what to table o, as what says, while an erasure
// request that is not over erases from it.
func (s *Server) refuseErasing(c *call, o auth.Object, what string) error {
	if s.opts.Erasing == nil {
		return nil
	}
	erasing, err := s.opts.Erasing(c.ctx(), o)
	if err != nil {
		return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check whether an erasure request erases from the table; try again")
	}
	if erasing {
		return c.who.deny(fmt.Sprintf("an erasure request erases from %s; %s until the request is over", o, what))
	}
	return nil
}

// checkCreateFormat refuses to create a table of Iceberg format version 3
// under the name of a table with tags, or that an erasure request that is
// not over erases from, which the request would take for that table:
// erasure requests cannot rewrite tables of version 3 yet.
func (s *Server) checkCreateFormat(c *call, o auth.Object, props map[string]string) error {
	if !upgradesFormat(commitRequest{Updates: []tableUpdate{{Action: "set-properties", Updates: props}}}) {
		return nil
	}
	if s.opts.Erasing != nil {
		erasing, err := s.opts.Erasing(c.ctx(), o)
		if err != nil {
			return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check whether an erasure request erases from the table; try again")
		}
		if erasing {
			return c.who.deny(fmt.Sprintf("an erasure request erases from %s; a table under its name stays at Iceberg format version 2 "+
				"until the request is over, as erasure requests cannot rewrite tables of version 3 yet", o))
		}
	}
	if s.opts.Classified == nil {
		return nil
	}
	_, classified, err := s.opts.Classified(c.ctx(), o)
	if err != nil {
		return errorf(http.StatusServiceUnavailable, errUnavailable, "could not check the table's policies and tags; try again")
	}
	if classified {
		return c.who.deny(fmt.Sprintf("%s has policies or tags; a table under its name stays at Iceberg format version 2, "+
			"as erasure requests cannot rewrite tables of version 3 yet", o))
	}
	return nil
}

// forget removes the grants on a dropped table or schema.
func (s *Server) forget(c *call, o auth.Object) {
	if err := s.opts.Grants.ForgetObject(c.ctx(), o); err != nil {
		s.log.Warn("could not remove the grants of a dropped "+o.Kind(), "object", o.String(), "err", err)
	}
}
