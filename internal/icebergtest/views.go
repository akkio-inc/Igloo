package icebergtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
)

type viewState struct {
	ns       []string
	name     string
	meta     view.Metadata
	metaLoc  string
	versions int
}

// Views lists the catalog's views as schema.name.
func (c *Catalog) Views() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, v := range c.views {
		out = append(out, strings.Join(v.ns, ".")+"."+v.name)
	}
	return out
}

// Tables lists the catalog's tables as schema.name.
func (c *Catalog) Tables() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, t := range c.tables {
		out = append(out, strings.Join(t.ns, ".")+"."+t.name)
	}
	return out
}

// HoldViewLists makes the catalog run hold, if not nil, each time it has
// listed a namespace's views and before it sends the list, as a slow
// catalog or network delays it: whatever changes in the meantime, the
// client gets the list as it was. hold must not call the catalog's methods.
func (c *Catalog) HoldViewLists(hold func(namespace string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.holdViewLists = hold
}

// listViews answers GET .../namespaces/{namespace}/views; p is the path
// after the prefix.
func (c *Catalog) listViews(w http.ResponseWriter, r *http.Request, p []string) {
	rec := httptest.NewRecorder()
	c.mu.Lock()
	if _, ok := c.namespaces[p[1]]; !ok {
		apiError(rec, http.StatusNotFound, "NoSuchNamespaceException", "no such namespace")
	} else {
		ids := []map[string]any{}
		for _, v := range c.views {
			if strings.Join(v.ns, "\x1f") == p[1] {
				ids = append(ids, map[string]any{"namespace": v.ns, "name": v.name})
			}
		}
		writeJSON(rec, http.StatusOK, map[string]any{"identifiers": ids})
	}
	hold := c.holdViewLists
	c.mu.Unlock()
	if hold != nil {
		hold(strings.ReplaceAll(p[1], "\x1f", "."))
	}
	if r.Context().Err() != nil {
		return
	}
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

// serveView answers the other view endpoints: p is the path after the
// prefix. c.mu is held.
func (c *Catalog) serveView(w http.ResponseWriter, r *http.Request, p []string, body []byte) {
	if p[0] == "views" {
		c.renameView(w, body)
		return
	}
	ns := strings.Split(p[1], "\x1f")
	if _, ok := c.namespaces[p[1]]; !ok {
		apiError(w, http.StatusNotFound, "NoSuchNamespaceException", "no such namespace")
		return
	}
	if len(p) == 3 {
		if r.Method != http.MethodPost {
			apiError(w, http.StatusMethodNotAllowed, "BadRequestException", "bad method")
			return
		}
		c.createView(w, ns, body)
		return
	}
	if len(p) != 4 {
		apiError(w, http.StatusNotFound, "NotFoundException", "no such path")
		return
	}
	k := tableKey(ns, p[3])
	v := c.views[k]
	if v == nil {
		apiError(w, http.StatusNotFound, "NoSuchViewException", "no such view")
		return
	}
	switch r.Method {
	case http.MethodHead:
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, viewResult(v))
	case http.MethodDelete:
		delete(c.views, k)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		var req struct {
			Requirements view.Requirements `json:"requirements"`
			Updates      view.Updates      `json:"updates"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
			return
		}
		for _, rq := range req.Requirements {
			if err := rq.Validate(v.meta); err != nil {
				apiError(w, http.StatusConflict, "CommitFailedException", fmt.Sprintf("requirement failed: %v", err))
				return
			}
		}
		b, err := view.MetadataBuilderFromBase(v.meta)
		if err != nil {
			apiError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		for _, u := range req.Updates {
			if err := u.Apply(b); err != nil {
				apiError(w, http.StatusBadRequest, "BadRequestException", fmt.Sprintf("%s: %v", u.Action(), err))
				return
			}
		}
		res, err := b.Build()
		if err != nil {
			apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
			return
		}
		next := &viewState{ns: v.ns, name: v.name, meta: res.Metadata, versions: v.versions}
		next.metaLoc = next.nextMetadataLocation()
		c.views[k] = next
		writeJSON(w, http.StatusOK, viewResult(next))
	default:
		apiError(w, http.StatusMethodNotAllowed, "BadRequestException", "bad method")
	}
}

func (c *Catalog) createView(w http.ResponseWriter, ns []string, body []byte) {
	var req struct {
		Name       string             `json:"name"`
		Location   string             `json:"location"`
		Schema     *iceberg.Schema    `json:"schema"`
		Version    *view.Version      `json:"view-version"`
		Properties iceberg.Properties `json:"properties"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Name == "" || req.Schema == nil || req.Version == nil {
		apiError(w, http.StatusBadRequest, "BadRequestException", fmt.Sprintf("bad view: %v", err))
		return
	}
	k := tableKey(ns, req.Name)
	if c.views[k] != nil || c.tables[k] != nil {
		apiError(w, http.StatusConflict, "AlreadyExistsException", "a view or table of the name exists")
		return
	}
	loc := req.Location
	if loc == "" {
		loc = c.location + "/" + uuid.NewString()
	}
	meta, err := view.NewMetadata(req.Version, req.Schema, loc, req.Properties)
	if err != nil {
		apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}
	if res, ok := meta.(*view.MetadataBuildResult); ok {
		meta = res.Metadata
	}
	v := &viewState{ns: ns, name: req.Name, meta: meta}
	v.metaLoc = v.nextMetadataLocation()
	c.views[k] = v
	writeJSON(w, http.StatusOK, viewResult(v))
}

func (c *Catalog) renameView(w http.ResponseWriter, body []byte) {
	var req struct {
		Source      identifier `json:"source"`
		Destination identifier `json:"destination"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		apiError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}
	from, to := tableKey(req.Source.Namespace, req.Source.Name), tableKey(req.Destination.Namespace, req.Destination.Name)
	v := c.views[from]
	switch {
	case v == nil:
		apiError(w, http.StatusNotFound, "NoSuchViewException", "no such view")
		return
	case c.views[to] != nil || c.tables[to] != nil:
		apiError(w, http.StatusConflict, "AlreadyExistsException", "a view or table of the name exists")
		return
	}
	if _, ok := c.namespaces[strings.Join(req.Destination.Namespace, "\x1f")]; !ok {
		apiError(w, http.StatusNotFound, "NoSuchNamespaceException", "no such namespace")
		return
	}
	delete(c.views, from)
	c.views[to] = &viewState{ns: req.Destination.Namespace, name: req.Destination.Name, meta: v.meta, metaLoc: v.metaLoc, versions: v.versions}
	w.WriteHeader(http.StatusNoContent)
}

func (v *viewState) nextMetadataLocation() string {
	v.versions++
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", v.meta.Location(), v.versions, uuid.NewString())
}

func viewResult(v *viewState) map[string]any {
	return map[string]any{"metadata-location": v.metaLoc, "metadata": v.meta, "config": map[string]string{}}
}
