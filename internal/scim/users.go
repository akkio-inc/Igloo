package scim

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sso"
)

// userState is a user's attributes as SCIM sets them.
type userState struct {
	UserName    string
	ExternalID  string
	DisplayName string
	GivenName   string
	FamilyName  string
	Email       string
	Active      bool
}

// stateOf is what Igloo holds about a provisioned user.
func stateOf(u auth.User, s auth.SCIMUser) userState {
	return userState{UserName: s.UserName, ExternalID: s.ExternalID, DisplayName: s.DisplayName,
		GivenName: s.GivenName, FamilyName: s.FamilyName, Email: u.Email, Active: !u.Disabled}
}

// errInvalid is a request's attribute value Igloo cannot use.
type errInvalid struct{ msg string }

func (e errInvalid) Error() string { return e.msg }

func invalid(format string, args ...any) error { return errInvalid{fmt.Sprintf(format, args...)} }

// set applies one attribute, as an add or replace operation, or with
// remove, removes it. Attributes Igloo does not keep are ignored, as are
// removals of the user name, email and active.
func (st *userState) set(attr string, v any, remove bool) error {
	a := strings.ToLower(strings.TrimPrefix(attr, schemaUser+":"))
	if strings.HasPrefix(a, strings.ToLower(schemaEnterprise)) {
		return nil
	}
	text := func() (string, error) {
		if remove || v == nil {
			return "", nil
		}
		s, ok := v.(string)
		if !ok {
			return "", invalid("%s must be a string", attr)
		}
		return strings.TrimSpace(s), nil
	}
	var err error
	switch {
	case a == "username":
		if remove {
			return nil
		}
		st.UserName, err = text()
	case a == "externalid":
		st.ExternalID, err = text()
	case a == "displayname":
		st.DisplayName, err = text()
	case a == "name.givenname":
		st.GivenName, err = text()
	case a == "name.familyname":
		st.FamilyName, err = text()
	case a == "name":
		if remove {
			st.GivenName, st.FamilyName = "", ""
			return nil
		}
		name, ok := v.(map[string]any)
		if !ok {
			return invalid("name must be an object")
		}
		for k, sub := range name {
			if err := st.set("name."+k, sub, false); err != nil {
				return err
			}
		}
	case a == "active":
		if remove {
			return nil
		}
		active, ok := boolish(v)
		if !ok {
			return invalid("active must be a boolean")
		}
		st.Active = active
	case a == "emails":
		if remove {
			return nil
		}
		if email := pickEmail(v); email != "" {
			st.Email = email
		}
	case strings.HasPrefix(a, "emails[") && strings.HasSuffix(a, "].value"), a == "emails.value":
		if remove {
			return nil
		}
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			st.Email = strings.TrimSpace(s)
		}
	case strings.HasPrefix(a, "name."):
		// formatted, middleName and the like.
	}
	return err
}

// boolish reads a boolean, which Entra ID sends as "True" or "False" in
// patches.
func boolish(v any) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	case string:
		switch strings.ToLower(strings.TrimSpace(b)) {
		case "true":
			return true, true
		case "false":
			return false, true
		}
	}
	return false, false
}

// pickEmail chooses from emails the primary one, else a work one, else the
// first.
func pickEmail(v any) string {
	var list []any
	switch e := v.(type) {
	case []any:
		list = e
	case map[string]any:
		list = []any{e}
	}
	value := func(x any) string {
		m, _ := x.(map[string]any)
		s, _ := m["value"].(string)
		return strings.TrimSpace(s)
	}
	for _, x := range list {
		if m, _ := x.(map[string]any); m != nil {
			if primary, _ := boolish(m["primary"]); primary && value(x) != "" {
				return value(x)
			}
		}
	}
	for _, x := range list {
		if m, _ := x.(map[string]any); m != nil {
			if t, _ := m["type"].(string); strings.EqualFold(t, "work") && value(x) != "" {
				return value(x)
			}
		}
	}
	for _, x := range list {
		if value(x) != "" {
			return value(x)
		}
	}
	return ""
}

// apply sets every attribute of a resource body.
func (st *userState) apply(body map[string]any) error {
	for k, v := range body {
		switch strings.ToLower(k) {
		case "schemas", "id", "meta", "groups", "password":
			continue
		}
		if err := st.set(k, v, false); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) userResource(ctx context.Context, r *http.Request, u auth.User, s auth.SCIMUser) (map[string]any, error) {
	res := map[string]any{
		"schemas":  []string{schemaUser},
		"id":       u.Name,
		"userName": s.UserName,
		"active":   !u.Disabled,
		"meta": map[string]any{"resourceType": "User", "created": s.Created.UTC().Format(time.RFC3339),
			"lastModified": s.Modified.UTC().Format(time.RFC3339), "location": location(r, "/Users/"+u.Name)},
	}
	if s.ExternalID != "" {
		res["externalId"] = s.ExternalID
	}
	if s.DisplayName != "" {
		res["displayName"] = s.DisplayName
	}
	if s.GivenName != "" || s.FamilyName != "" {
		res["name"] = map[string]string{"givenName": s.GivenName, "familyName": s.FamilyName}
	}
	if u.Email != "" {
		res["emails"] = []map[string]any{{"value": u.Email, "type": "work", "primary": true}}
	}
	groups, err := h.scim.SCIMGroups(ctx)
	if err != nil {
		return nil, err
	}
	var refs []map[string]string
	for _, g := range groups {
		if slices.Contains(g.Members, u.Name) {
			refs = append(refs, map[string]string{"value": g.ID, "display": g.DisplayName, "$ref": location(r, "/Groups/"+g.ID)})
		}
	}
	if refs != nil {
		res["groups"] = refs
	}
	return res, nil
}

// provisioned returns a provisioned user and their record, writing a 404
// for anyone else.
func (h *Handler) provisioned(w http.ResponseWriter, r *http.Request, id string) (auth.User, auth.SCIMUser, bool) {
	s, err := h.scim.SCIMUser(r.Context(), id)
	if err == nil {
		var u auth.User
		if u, err = h.users.Get(r.Context(), id); err == nil {
			return u, s, true
		}
	}
	if errors.Is(err, auth.ErrSCIMNotFound) || errors.Is(err, auth.ErrNotFound) {
		writeError(w, http.StatusNotFound, "", "no provisioned user "+id)
	} else {
		h.serverError(w, err)
	}
	return auth.User{}, auth.SCIMUser{}, false
}

func (h *Handler) serverError(w http.ResponseWriter, err error) {
	h.log.Error("SCIM request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "", "the request failed; see Igloo's log")
}

// store saves st for u: email, active and the SCIM record. It writes the
// error response itself and reports false on one.
func (h *Handler) store(w http.ResponseWriter, r *http.Request, u auth.User, st userState) bool {
	ctx := r.Context()
	if st.UserName == "" {
		writeError(w, http.StatusBadRequest, "invalidValue", "userName is required")
		return false
	}
	if email := auth.NormalizeEmail(st.Email); email != u.Email && email != "" {
		err := h.accounts.SetEmail(ctx, u.Name, email)
		if errors.Is(err, auth.ErrEmailTaken) {
			writeError(w, http.StatusConflict, "uniqueness", "another user has the email "+email)
			return false
		}
		if err != nil {
			h.serverError(w, err)
			return false
		}
	}
	if st.Active == u.Disabled {
		err := h.accounts.SetLogin(ctx, u.Name, st.Active)
		if errors.Is(err, auth.ErrLastAdmin) {
			writeError(w, http.StatusBadRequest, "mutability", u.Name+" is the last enabled admin and cannot be deactivated")
			return false
		}
		if err != nil {
			h.serverError(w, err)
			return false
		}
		h.changed(u.Name)
		h.log.Info("SCIM changed whether a user may sign in", "user", u.Name, "active", st.Active, "by", caller(r))
	}
	err := h.scim.PutSCIMUser(ctx, auth.SCIMUser{User: u.Name, ExternalID: st.ExternalID, UserName: st.UserName,
		DisplayName: st.DisplayName, GivenName: st.GivenName, FamilyName: st.FamilyName})
	if err != nil {
		h.serverError(w, err)
		return false
	}
	return true
}

// respondUser sends the current resource of user.
func (h *Handler) respondUser(w http.ResponseWriter, r *http.Request, status int, name string) {
	u, s, ok := h.provisioned(w, r, name)
	if !ok {
		return
	}
	res, err := h.userResource(r.Context(), r, u, s)
	if err != nil {
		h.serverError(w, err)
		return
	}
	if status == http.StatusCreated {
		w.Header().Set("Location", location(r, "/Users/"+u.Name))
	}
	writeJSON(w, status, res)
}

func (h *Handler) listUsers(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r.URL.Query().Get("filter"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalidFilter", err.Error())
		return
	}
	ctx := r.Context()
	records, err := h.scim.SCIMUsers(ctx)
	if err != nil {
		h.serverError(w, err)
		return
	}
	all, err := h.users.List(ctx)
	if err != nil {
		h.serverError(w, err)
		return
	}
	byName := map[string]auth.User{}
	for _, u := range all {
		byName[u.Name] = u
	}
	var matches []int
	for i, s := range records {
		u, ok := byName[s.User]
		if !ok {
			continue
		}
		if f != nil {
			var got string
			switch f.attr {
			case "username":
				got = s.UserName
			case "externalid":
				got = s.ExternalID
			case "id":
				got = u.Name
			case "emails", "emails.value":
				got = u.Email
			case "displayname":
				got = s.DisplayName
			case "active":
				got = fmt.Sprint(!u.Disabled)
			default:
				writeError(w, http.StatusBadRequest, "invalidFilter", "users can be filtered by userName, externalId, id, emails.value, displayName or active")
				return
			}
			// userName and emails compare without case, as identity
			// providers expect.
			match := strings.EqualFold(got, f.value)
			if f.attr == "externalid" || f.attr == "id" {
				match = got == f.value
			}
			if !match {
				continue
			}
		}
		matches = append(matches, i)
	}
	start, count := page(r)
	var resources []any
	for _, i := range slice(matches, start, count) {
		res, err := h.userResource(ctx, r, byName[records[i].User], records[i])
		if err != nil {
			h.serverError(w, err)
			return
		}
		resources = append(resources, res)
	}
	writeJSON(w, http.StatusOK, listResponse(resources, len(matches), start))
}

func (h *Handler) getUser(w http.ResponseWriter, r *http.Request) {
	h.respondUser(w, r, http.StatusOK, r.PathValue("id"))
}

// createUser provisions a user: it finds the Igloo user the account is, as
// single sign-on would, or creates one with role none, and starts managing
// them.
func (h *Handler) createUser(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readJSON(w, r, &body) {
		return
	}
	st := userState{Active: true}
	if err := st.apply(body); err != nil {
		writeError(w, http.StatusBadRequest, "invalidValue", err.Error())
		return
	}
	if st.UserName == "" {
		writeError(w, http.StatusBadRequest, "invalidValue", "userName is required")
		return
	}
	email := st.Email
	if email == "" && strings.Contains(st.UserName, "@") {
		email = st.UserName
	}
	st.Email = email
	ctx := r.Context()
	u, created, err := h.sso.Provision(ctx, st.UserName, email)
	var refusal *sso.Refusal
	switch {
	case errors.As(err, &refusal):
		writeError(w, http.StatusConflict, "uniqueness", refusal.Reason)
		return
	case err != nil:
		h.serverError(w, err)
		return
	case u.IsService():
		writeError(w, http.StatusConflict, "uniqueness", u.Name+" is a service account, which cannot be provisioned")
		return
	}
	if _, err := h.scim.SCIMUser(ctx, u.Name); err == nil {
		writeError(w, http.StatusConflict, "uniqueness", fmt.Sprintf("%s is provisioned already, as id %s", st.UserName, u.Name))
		return
	} else if !errors.Is(err, auth.ErrSCIMNotFound) {
		h.serverError(w, err)
		return
	}
	// A user who existed and was disabled stays so: an admin, or an
	// earlier deprovisioning, decided that, and taking the user over does
	// not undo it. Later changes to active apply as usual.
	if !created && u.Disabled && st.Active {
		st.Active = false
		h.log.Warn("SCIM took over a disabled user, who stays disabled until an admin or a later SCIM change enables them", "user", u.Name, "by", caller(r))
	}
	if !h.store(w, r, u, st) {
		return
	}
	h.log.Info("SCIM provisioned a user", "user", u.Name, "username", st.UserName, "by", caller(r))
	h.respondUser(w, r, http.StatusCreated, u.Name)
}

// replaceUser sets the attributes the body has; others stay.
func (h *Handler) replaceUser(w http.ResponseWriter, r *http.Request) {
	u, s, ok := h.provisioned(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	var body map[string]any
	if !readJSON(w, r, &body) {
		return
	}
	st := stateOf(u, s)
	if err := st.apply(body); err != nil {
		writeError(w, http.StatusBadRequest, "invalidValue", err.Error())
		return
	}
	if h.store(w, r, u, st) {
		h.respondUser(w, r, http.StatusOK, u.Name)
	}
}

// patchOp is one operation of a PATCH request.
type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

type patchRequest struct {
	Operations []patchOp `json:"Operations"`
}

func (h *Handler) patchUser(w http.ResponseWriter, r *http.Request) {
	u, s, ok := h.provisioned(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	var req patchRequest
	if !readJSON(w, r, &req) {
		return
	}
	st := stateOf(u, s)
	for _, op := range req.Operations {
		var err error
		switch strings.ToLower(op.Op) {
		case "add", "replace":
			if op.Path == "" {
				body, ok := op.Value.(map[string]any)
				if !ok {
					err = invalid("an operation without a path needs an object value")
				} else {
					err = st.apply(body)
				}
			} else {
				err = st.set(op.Path, op.Value, false)
			}
		case "remove":
			if op.Path == "" {
				err = invalid("remove needs a path")
			} else {
				err = st.set(op.Path, nil, true)
			}
		default:
			err = invalid("unknown operation %q", op.Op)
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalidValue", err.Error())
			return
		}
	}
	if h.store(w, r, u, st) {
		h.respondUser(w, r, http.StatusOK, u.Name)
	}
}

// deleteUser deprovisions a user: they are disabled and no longer managed,
// and lose the roles their groups gave them. Admins drop users themselves.
func (h *Handler) deleteUser(w http.ResponseWriter, r *http.Request) {
	u, _, ok := h.provisioned(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	ctx := r.Context()
	err := h.accounts.SetLogin(ctx, u.Name, false)
	if errors.Is(err, auth.ErrLastAdmin) {
		writeError(w, http.StatusBadRequest, "mutability", u.Name+" is the last enabled admin and cannot be deprovisioned")
		return
	}
	if err != nil {
		h.serverError(w, err)
		return
	}
	if err := h.scim.DeleteSCIMUser(ctx, u.Name); err != nil {
		h.serverError(w, err)
		return
	}
	if err := h.sso.RefreshRoles(ctx, u.Name); err != nil {
		h.log.Warn("cannot update a deprovisioned user's roles", "user", u.Name, "err", err)
	}
	h.changed(u.Name)
	h.log.Info("SCIM deprovisioned a user, who is now disabled", "user", u.Name, "by", caller(r))
	w.WriteHeader(http.StatusNoContent)
}
