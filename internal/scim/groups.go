package scim

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Groups are kept with their members; the group mappings (GRANT role TO SSO
// GROUP 'name') match their display names. A change to a group applies the
// mappings to its members, old and new, at once. The store records them
// with the change, so that those a failure leaves behind are applied by
// the next request, even a provider's retry that finds a removed member
// gone already, or by Run.

func groupResource(r *http.Request, g auth.SCIMGroup, members bool) map[string]any {
	res := map[string]any{
		"schemas":     []string{schemaGroup},
		"id":          g.ID,
		"displayName": g.DisplayName,
		"meta": map[string]any{"resourceType": "Group", "created": g.Created.UTC().Format(time.RFC3339),
			"lastModified": g.Modified.UTC().Format(time.RFC3339), "location": location(r, "/Groups/"+g.ID)},
	}
	if g.ExternalID != "" {
		res["externalId"] = g.ExternalID
	}
	if members {
		list := make([]map[string]string, len(g.Members))
		for i, m := range g.Members {
			list[i] = map[string]string{"value": m, "display": m, "type": "User", "$ref": location(r, "/Users/"+m)}
		}
		res["members"] = list
	}
	return res
}

// memberIDs reads the members of a value: a list of {"value": id}, or one.
func memberIDs(v any) ([]string, error) {
	var list []any
	switch m := v.(type) {
	case []any:
		list = m
	case map[string]any:
		list = []any{m}
	case nil:
		return nil, nil
	default:
		return nil, invalid("members must be a list of {\"value\": id}")
	}
	var ids []string
	for _, x := range list {
		m, _ := x.(map[string]any)
		id, _ := m["value"].(string)
		if id == "" {
			return nil, invalid("a member needs a value, the user's id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// memberFilter matches a remove path that names one member.
var memberFilter = regexp.MustCompile(`(?i)^members\[\s*value\s+eq\s+"([^"]*)"\s*\]$`)

// patchGroup applies one PATCH operation to g.
func patchGroup(g *auth.SCIMGroup, op patchOp) error {
	verb := strings.ToLower(op.Op)
	path := strings.ToLower(strings.TrimPrefix(op.Path, schemaGroup+":"))
	if verb != "add" && verb != "replace" && verb != "remove" {
		return invalid("unknown operation %q", op.Op)
	}
	if path == "" {
		if verb == "remove" {
			return invalid("remove needs a path")
		}
		body, ok := op.Value.(map[string]any)
		if !ok {
			return invalid("an operation without a path needs an object value")
		}
		for k, v := range body {
			if err := patchGroup(g, patchOp{Op: op.Op, Path: k, Value: v}); err != nil {
				return err
			}
		}
		return nil
	}
	if m := memberFilter.FindStringSubmatch(op.Path); m != nil && verb == "remove" {
		g.Members = slices.DeleteFunc(g.Members, func(id string) bool { return id == m[1] })
		return nil
	}
	switch path {
	case "displayname":
		name, _ := op.Value.(string)
		if verb == "remove" || strings.TrimSpace(name) == "" {
			return invalid("displayName must be a non-empty string")
		}
		g.DisplayName = strings.TrimSpace(name)
	case "externalid":
		id, _ := op.Value.(string)
		if verb == "remove" {
			id = ""
		}
		g.ExternalID = id
	case "members":
		ids, err := memberIDs(op.Value)
		if err != nil {
			return err
		}
		switch verb {
		case "add":
			for _, id := range ids {
				if !slices.Contains(g.Members, id) {
					g.Members = append(g.Members, id)
				}
			}
		case "replace":
			g.Members = ids
		case "remove":
			if op.Value == nil {
				g.Members = nil
			} else {
				g.Members = slices.DeleteFunc(g.Members, func(id string) bool { return slices.Contains(ids, id) })
			}
		}
	case "schemas", "id", "meta":
	default:
		return invalid("unknown group attribute %q", op.Path)
	}
	return nil
}

// applyChanges applies the group mappings to the users whose groups
// changed (SCIMChanges): those of this request, and those earlier ones
// left behind. It reports the first failure; the users it failed for stay
// recorded. A replica applies changes one call at a time, so one that read
// a user's groups before a later change never writes its roles over that
// change's.
func (h *Handler) applyChanges(ctx context.Context) error {
	h.applying.Lock()
	defer h.applying.Unlock()
	changes, err := h.scim.SCIMChanges(ctx)
	if err != nil {
		return err
	}
	var failed error
	for _, c := range changes {
		err := h.sso.RefreshRoles(ctx, c.User)
		if err == nil || errors.Is(err, auth.ErrNotFound) {
			err = h.scim.SCIMChangeApplied(ctx, c)
		}
		if err != nil {
			h.log.Warn("cannot apply the group mappings to a user yet", "user", c.User, "err", err)
			failed = cmp.Or(failed, err)
		}
	}
	return failed
}

// Run applies the group mappings to the users whose groups changed but
// whose roles a failure left behind, now and every interval until ctx
// ends.
func (h *Handler) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := h.applyChanges(ctx); err != nil && ctx.Err() == nil {
			h.log.Warn("cannot apply the group mappings after SCIM changed groups; trying again", "in", interval, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// changeError answers a group change that was kept but whose members'
// roles could not be applied: the provider may send it again, which
// applies them, and Run applies them meanwhile.
func (h *Handler) changeError(w http.ResponseWriter, r *http.Request, err error) {
	h.log.Error("SCIM changed a group but cannot apply the group mappings to its members yet", "err", err, "by", caller(r))
	writeError(w, http.StatusInternalServerError, "", "the group was changed, but its members' roles could not be updated; "+
		"Igloo updates them within a minute, or when the request is sent again")
}

// groupError writes the response for an error of the group store.
func (h *Handler) groupError(w http.ResponseWriter, err error) {
	var bad errInvalid
	switch {
	case errors.As(err, &bad):
		writeError(w, http.StatusBadRequest, "invalidValue", bad.Error())
	case errors.Is(err, auth.ErrSCIMGroupExists):
		writeError(w, http.StatusConflict, "uniqueness", "a group of that displayName exists")
	case errors.Is(err, auth.ErrNotFound):
		writeError(w, http.StatusBadRequest, "invalidValue", "a member is not a user Igloo knows; provision the user first")
	case errors.Is(err, auth.ErrSCIMNotFound):
		writeError(w, http.StatusNotFound, "", "no such group")
	default:
		h.serverError(w, err)
	}
}

func (h *Handler) listGroups(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r.URL.Query().Get("filter"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalidFilter", err.Error())
		return
	}
	groups, err := h.scim.SCIMGroups(r.Context())
	if err != nil {
		h.serverError(w, err)
		return
	}
	var matches []auth.SCIMGroup
	for _, g := range groups {
		if f != nil {
			switch f.attr {
			case "displayname":
				if !strings.EqualFold(g.DisplayName, f.value) {
					continue
				}
			case "externalid":
				if g.ExternalID != f.value {
					continue
				}
			case "id":
				if g.ID != f.value {
					continue
				}
			default:
				writeError(w, http.StatusBadRequest, "invalidFilter", "groups can be filtered by displayName, externalId or id")
				return
			}
		}
		matches = append(matches, g)
	}
	start, count := page(r)
	var resources []any
	for _, g := range slice(matches, start, count) {
		resources = append(resources, groupResource(r, g, !excluded(r, "members")))
	}
	writeJSON(w, http.StatusOK, listResponse(resources, len(matches), start))
}

func (h *Handler) getGroup(w http.ResponseWriter, r *http.Request) {
	g, err := h.scim.SCIMGroup(r.Context(), r.PathValue("id"))
	if err != nil {
		h.groupError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, groupResource(r, g, !excluded(r, "members")))
}

func (h *Handler) createGroup(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readJSON(w, r, &body) {
		return
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		h.serverError(w, err)
		return
	}
	g := auth.SCIMGroup{ID: hex.EncodeToString(id)}
	if err := patchGroup(&g, patchOp{Op: "replace", Value: body}); err != nil {
		h.groupError(w, err)
		return
	}
	if g.DisplayName == "" {
		writeError(w, http.StatusBadRequest, "invalidValue", "displayName is required")
		return
	}
	if err := h.scim.CreateSCIMGroup(r.Context(), g); err != nil {
		h.groupError(w, err)
		return
	}
	h.log.Info("SCIM created a group", "group", g.DisplayName, "members", len(g.Members), "by", caller(r))
	// The group exists now, and a retry would be refused as a duplicate:
	// it is answered with its id, and Run applies what failed.
	if err := h.applyChanges(r.Context()); err != nil {
		h.log.Error("SCIM created a group but cannot apply the group mappings to its members yet", "err", err, "by", caller(r))
	}
	created, err := h.scim.SCIMGroup(r.Context(), g.ID)
	if err != nil {
		h.serverError(w, err)
		return
	}
	w.Header().Set("Location", location(r, "/Groups/"+g.ID))
	writeJSON(w, http.StatusCreated, groupResource(r, created, true))
}

// modify changes a group and applies the mappings to its old and new
// members.
func (h *Handler) modify(w http.ResponseWriter, r *http.Request, change func(*auth.SCIMGroup) error) (auth.SCIMGroup, bool) {
	err := h.scim.ModifySCIMGroup(r.Context(), r.PathValue("id"), func(g *auth.SCIMGroup) error {
		if err := change(g); err != nil {
			return err
		}
		if strings.TrimSpace(g.DisplayName) == "" {
			return invalid("displayName is required")
		}
		return nil
	})
	if err != nil {
		h.groupError(w, err)
		return auth.SCIMGroup{}, false
	}
	if err := h.applyChanges(r.Context()); err != nil {
		h.changeError(w, r, err)
		return auth.SCIMGroup{}, false
	}
	g, err := h.scim.SCIMGroup(r.Context(), r.PathValue("id"))
	if err != nil {
		h.groupError(w, err)
		return auth.SCIMGroup{}, false
	}
	h.log.Info("SCIM changed a group", "group", g.DisplayName, "members", len(g.Members), "by", caller(r))
	return g, true
}

// replaceGroup sets what the body has: the name, and the members if it
// lists them.
func (h *Handler) replaceGroup(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readJSON(w, r, &body) {
		return
	}
	g, ok := h.modify(w, r, func(g *auth.SCIMGroup) error {
		return patchGroup(g, patchOp{Op: "replace", Value: body})
	})
	if ok {
		writeJSON(w, http.StatusOK, groupResource(r, g, true))
	}
}

func (h *Handler) patchGroup(w http.ResponseWriter, r *http.Request) {
	var req patchRequest
	if !readJSON(w, r, &req) {
		return
	}
	_, ok := h.modify(w, r, func(g *auth.SCIMGroup) error {
		for _, op := range req.Operations {
			if err := patchGroup(g, op); err != nil {
				return err
			}
		}
		return nil
	})
	if ok {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *Handler) deleteGroup(w http.ResponseWriter, r *http.Request) {
	g, err := h.scim.SCIMGroup(r.Context(), r.PathValue("id"))
	if err == nil {
		err = h.scim.DeleteSCIMGroup(r.Context(), g.ID)
	}
	if err != nil {
		h.groupError(w, err)
		return
	}
	h.log.Info("SCIM deleted a group", "group", g.DisplayName, "by", caller(r))
	if err := h.applyChanges(r.Context()); err != nil {
		h.changeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
