// Package scim lets an identity provider such as Okta or Microsoft Entra ID
// provision Igloo's users and groups with SCIM 2.0 (RFC 7643 and 7644), so
// that people get an Igloo user before they first sign in, are disabled as
// soon as the provider deactivates them, and get the roles their groups map
// to (GRANT role TO SSO GROUP) as group memberships change.
//
// It serves /scim/v2/Users and /scim/v2/Groups, with the discovery
// documents, on the web console's port. Requests carry an Igloo access token
// of an admin (Authorization: Bearer igloo_...), as a service account made
// for the purpose should hold. Users are ordinary Igloo users, named and
// matched as single sign-on does (package sso), and their SCIM id is their
// user name. Deleting a user disables them and ends their provisioning;
// admins drop users themselves. Unknown attributes are accepted and ignored.
package scim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sso"
)

// Prefix is where the handler serves.
const Prefix = "/scim/v2"

// Schema and message URNs.
const (
	schemaUser       = "urn:ietf:params:scim:schemas:core:2.0:User"
	schemaGroup      = "urn:ietf:params:scim:schemas:core:2.0:Group"
	schemaList       = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
	schemaPatch      = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
	schemaError      = "urn:ietf:params:scim:api:messages:2.0:Error"
	schemaConfig     = "urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"
	schemaType       = "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
	schemaSchema     = "urn:ietf:params:scim:schemas:core:2.0:Schema"
	schemaEnterprise = "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"
)

// maxBody bounds a request body: a group's full member list fits.
const maxBody = 4 << 20

// Options configure a Handler.
type Options struct {
	Users    auth.Store
	Accounts auth.AccountStore
	SCIM     auth.SCIMStore
	// Tokens checks the bearer token of each request.
	Tokens *auth.TokenChecker
	// SSO names and finds users as single sign-on does, and applies the
	// group mappings.
	SSO *sso.Service
	// Changed, if set, is told the name of a user who was disabled or
	// whose roles changed, so open sessions see it.
	Changed func(user string)
	Log     *slog.Logger
}

// Handler serves SCIM. It is safe for concurrent use.
type Handler struct {
	users    auth.Store
	accounts auth.AccountStore
	scim     auth.SCIMStore
	tokens   *auth.TokenChecker
	sso      *sso.Service
	changed  func(string)
	log      *slog.Logger
	mux      *http.ServeMux
}

// New returns a handler for opts, which needs every field but Changed and
// Log.
func New(opts Options) (*Handler, error) {
	if opts.Users == nil || opts.Accounts == nil || opts.SCIM == nil || opts.Tokens == nil || opts.SSO == nil {
		return nil, errors.New("scim: provisioning needs the metadata database and single sign-on")
	}
	if opts.Changed == nil {
		opts.Changed = func(string) {}
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	h := &Handler{users: opts.Users, accounts: opts.Accounts, scim: opts.SCIM, tokens: opts.Tokens, sso: opts.SSO,
		changed: opts.Changed, log: opts.Log.With("component", "scim"), mux: http.NewServeMux()}
	h.routes()
	return h, nil
}

func (h *Handler) routes() {
	h.mux.HandleFunc("GET "+Prefix+"/ServiceProviderConfig", h.serviceProviderConfig)
	h.mux.HandleFunc("GET "+Prefix+"/ResourceTypes", h.resourceTypes)
	h.mux.HandleFunc("GET "+Prefix+"/ResourceTypes/{name}", h.resourceTypes)
	h.mux.HandleFunc("GET "+Prefix+"/Schemas", h.schemas)
	h.mux.HandleFunc("GET "+Prefix+"/Schemas/{id}", h.schemas)
	h.mux.HandleFunc("GET "+Prefix+"/Users", h.listUsers)
	h.mux.HandleFunc("POST "+Prefix+"/Users", h.createUser)
	h.mux.HandleFunc("GET "+Prefix+"/Users/{id}", h.getUser)
	h.mux.HandleFunc("PUT "+Prefix+"/Users/{id}", h.replaceUser)
	h.mux.HandleFunc("PATCH "+Prefix+"/Users/{id}", h.patchUser)
	h.mux.HandleFunc("DELETE "+Prefix+"/Users/{id}", h.deleteUser)
	h.mux.HandleFunc("GET "+Prefix+"/Groups", h.listGroups)
	h.mux.HandleFunc("POST "+Prefix+"/Groups", h.createGroup)
	h.mux.HandleFunc("GET "+Prefix+"/Groups/{id}", h.getGroup)
	h.mux.HandleFunc("PUT "+Prefix+"/Groups/{id}", h.replaceGroup)
	h.mux.HandleFunc("PATCH "+Prefix+"/Groups/{id}", h.patchGroup)
	h.mux.HandleFunc("DELETE "+Prefix+"/Groups/{id}", h.deleteGroup)
	h.mux.HandleFunc(Prefix+"/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "", "no such SCIM endpoint")
	})
}

// callerKey carries the admin a request authenticated as.
type callerKey struct{}

// ServeHTTP authenticates the request's bearer token, which must be an
// admin's, and serves it.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	secret, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="igloo"`)
		writeError(w, http.StatusUnauthorized, "", "send an Igloo access token: Authorization: Bearer igloo_...")
		return
	}
	user, token, err := h.tokens.Verify(r.Context(), strings.TrimSpace(secret))
	if err != nil {
		if !errors.Is(err, auth.ErrInvalidToken) {
			h.log.Error("cannot check a SCIM token", "err", err)
			writeError(w, http.StatusServiceUnavailable, "", "cannot check the token now")
			return
		}
		h.log.Warn("SCIM request with an invalid token", "addr", r.RemoteAddr, "reason", err)
		w.Header().Set("WWW-Authenticate", `Bearer realm="igloo", error="invalid_token"`)
		writeError(w, http.StatusUnauthorized, "", "invalid access token")
		return
	}
	if user.Role != auth.RoleAdmin {
		h.log.Warn("SCIM request by a non-admin", "user", user.Name, "token", token.Name)
		writeError(w, http.StatusForbidden, "", "SCIM needs an access token of a user with the admin role")
		return
	}
	h.mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callerKey{}, user.Name+"/"+token.Name)))
}

// caller names who made a request, for logs: user/token.
func caller(r *http.Request) string {
	c, _ := r.Context().Value(callerKey{}).(string)
	return c
}

// location is the URL of a resource, as the client reached Igloo.
func location(r *http.Request, path string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + Prefix + path
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/scim+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends a SCIM error; scimType may be empty.
func writeError(w http.ResponseWriter, status int, scimType, detail string) {
	body := map[string]any{"schemas": []string{schemaError}, "status": fmt.Sprint(status), "detail": detail}
	if scimType != "" {
		body["scimType"] = scimType
	}
	writeJSON(w, status, body)
}

// readJSON decodes a request body into v.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	ct, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";")
	switch strings.TrimSpace(strings.ToLower(ct)) {
	case "application/scim+json", "application/json":
	default:
		writeError(w, http.StatusUnsupportedMediaType, "", "send application/scim+json")
		return false
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalidSyntax", "malformed JSON: "+err.Error())
		return false
	}
	return true
}

// listResponse wraps resources in a SCIM ListResponse.
func listResponse(resources []any, total, start int) map[string]any {
	if resources == nil {
		resources = []any{}
	}
	return map[string]any{"schemas": []string{schemaList}, "totalResults": total, "startIndex": start,
		"itemsPerPage": len(resources), "Resources": resources}
}

func (h *Handler) serviceProviderConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"schemas":          []string{schemaConfig},
		"documentationUri": "https://github.com/akkio-inc/Igloo/blob/main/docs/sso.md#scim-provisioning",
		"patch":            map[string]bool{"supported": true},
		"bulk":             map[string]any{"supported": false, "maxOperations": 0, "maxPayloadSize": 0},
		"filter":           map[string]any{"supported": true, "maxResults": maxPage},
		"changePassword":   map[string]bool{"supported": false},
		"sort":             map[string]bool{"supported": false},
		"etag":             map[string]bool{"supported": false},
		"authenticationSchemes": []map[string]any{{
			"type": "oauthbearertoken", "name": "Igloo access token", "primary": true,
			"description": "An access token of an Igloo user with the admin role (CREATE TOKEN), as a bearer token",
		}},
		"meta": map[string]string{"resourceType": "ServiceProviderConfig", "location": location(r, "/ServiceProviderConfig")},
	})
}

func (h *Handler) resourceTypes(w http.ResponseWriter, r *http.Request) {
	types := map[string]map[string]any{
		"User": {"schemas": []string{schemaType}, "id": "User", "name": "User", "endpoint": "/Users", "schema": schemaUser,
			"schemaExtensions": []map[string]any{{"schema": schemaEnterprise, "required": false}},
			"meta":             map[string]string{"resourceType": "ResourceType", "location": location(r, "/ResourceTypes/User")}},
		"Group": {"schemas": []string{schemaType}, "id": "Group", "name": "Group", "endpoint": "/Groups", "schema": schemaGroup,
			"meta": map[string]string{"resourceType": "ResourceType", "location": location(r, "/ResourceTypes/Group")}},
	}
	if name := r.PathValue("name"); name != "" {
		t, ok := types[name]
		if !ok {
			writeError(w, http.StatusNotFound, "", "no resource type "+name)
			return
		}
		writeJSON(w, http.StatusOK, t)
		return
	}
	writeJSON(w, http.StatusOK, listResponse([]any{types["User"], types["Group"]}, 2, 1))
}

// schemas describes the attributes Igloo keeps; others are accepted and
// ignored.
func (h *Handler) schemas(w http.ResponseWriter, r *http.Request) {
	attr := func(name, typ string, extra map[string]any) map[string]any {
		a := map[string]any{"name": name, "type": typ, "multiValued": false, "required": false, "caseExact": false,
			"mutability": "readWrite", "returned": "default", "uniqueness": "none"}
		for k, v := range extra {
			a[k] = v
		}
		return a
	}
	all := map[string]map[string]any{
		schemaUser: {"schemas": []string{schemaSchema}, "id": schemaUser, "name": "User", "attributes": []any{
			attr("userName", "string", map[string]any{"required": true, "uniqueness": "server"}),
			attr("name", "complex", map[string]any{"subAttributes": []any{attr("givenName", "string", nil), attr("familyName", "string", nil)}}),
			attr("displayName", "string", nil),
			attr("emails", "complex", map[string]any{"multiValued": true, "subAttributes": []any{
				attr("value", "string", nil), attr("type", "string", nil), attr("primary", "boolean", nil)}}),
			attr("active", "boolean", nil),
			attr("groups", "complex", map[string]any{"multiValued": true, "mutability": "readOnly"}),
		}, "meta": map[string]string{"resourceType": "Schema", "location": location(r, "/Schemas/"+schemaUser)}},
		schemaGroup: {"schemas": []string{schemaSchema}, "id": schemaGroup, "name": "Group", "attributes": []any{
			attr("displayName", "string", map[string]any{"required": true, "uniqueness": "server"}),
			attr("members", "complex", map[string]any{"multiValued": true, "subAttributes": []any{attr("value", "string", nil)}}),
		}, "meta": map[string]string{"resourceType": "Schema", "location": location(r, "/Schemas/"+schemaGroup)}},
	}
	if id := r.PathValue("id"); id != "" {
		s, ok := all[id]
		if !ok {
			writeError(w, http.StatusNotFound, "", "no schema "+id)
			return
		}
		writeJSON(w, http.StatusOK, s)
		return
	}
	writeJSON(w, http.StatusOK, listResponse([]any{all[schemaUser], all[schemaGroup]}, 2, 1))
}
