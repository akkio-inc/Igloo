package console

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/sso"
)

// Single sign-on: the sign-in page's button opens /sso/start, which sends
// the browser to the identity provider with the state, nonce and PKCE
// challenge of a new sign-in (package sso) and keeps them, sealed, in a
// short-lived cookie. The provider sends the browser back to /sso/callback,
// which checks the state against the cookie, lets package sso verify the
// result and find the user, and starts a console session as a password
// sign-in does.
//
// The pending sign-in's cookie is SameSite=Lax, unlike the session's: the
// provider's redirect back is a navigation from another site, which would
// not carry a Strict cookie. It holds nothing that signs anyone in without
// the provider's code, and lives ten minutes.

const (
	ssoCookieName       = "igloo-sso"
	secureSSOCookieName = "__Host-igloo-sso"
)

// ssoAD binds sealed pending sign-ins to their purpose.
var ssoAD = []byte("igloo-console-sso")

func ssoCookieFor(r *http.Request) string {
	if r.TLS != nil {
		return secureSSOCookieName
	}
	return ssoCookieName
}

// callbackURL is where the provider sends the browser back: the configured
// URL, or this console's /sso/callback as the request reached it.
func (c *Console) callbackURL(r *http.Request) string {
	if u := c.sso.RedirectURL(); u != "" {
		return u
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/sso/callback"
}

// ssoFailed sends the browser to the sign-in page with code, one of
// sso's Refused* constants or a few of the console's own, which the page
// explains.
func (c *Console) ssoFailed(w http.ResponseWriter, r *http.Request, code string) {
	http.Redirect(w, r, "/?sso_error="+url.QueryEscape(code), http.StatusFound)
}

func (c *Console) ssoStart(w http.ResponseWriter, r *http.Request) {
	if c.sso == nil {
		http.NotFound(w, r)
		return
	}
	// Without TLS the session cookie would cross the network in the
	// clear, as a password would for a password sign-in.
	if err := c.signInAllowed(r); err != nil {
		c.ssoFailed(w, r, "insecure")
		return
	}
	callback := c.callbackURL(r)
	// The pending sign-in's cookie must be set on the host the provider
	// returns to.
	if u, err := url.Parse(callback); err == nil && !strings.EqualFold(u.Host, r.Host) {
		http.Redirect(w, r, (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/sso/start"}).String(), http.StatusFound)
		return
	}
	authURL, pending, err := c.sso.Begin(r.Context(), callback)
	if err != nil {
		c.log.Warn("cannot start single sign-on", "err", err)
		c.ssoFailed(w, r, "unavailable")
		return
	}
	value, err := c.cookies.sealAs(pending, ssoAD)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not start the sign-in")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     ssoCookieFor(r),
		Value:    value,
		Path:     "/",
		MaxAge:   int(sso.PendingLifetime.Seconds()),
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (c *Console) ssoCallback(w http.ResponseWriter, r *http.Request) {
	if c.sso == nil {
		http.NotFound(w, r)
		return
	}
	addr := remoteAddr(r)
	// The pending sign-in is used once, whatever happens.
	http.SetCookie(w, &http.Cookie{Name: ssoCookieFor(r), Path: "/", MaxAge: -1, HttpOnly: true, Secure: r.TLS != nil,
		SameSite: http.SameSiteLaxMode})

	var pending sso.Pending
	cookie, err := r.Cookie(ssoCookieFor(r))
	if err != nil || c.cookies.openAs(cookie.Value, ssoAD, &pending) != nil || pending.Expired(time.Now()) {
		c.ssoFailed(w, r, "expired")
		return
	}
	q := r.URL.Query()
	if !pending.CheckState(q.Get("state")) {
		c.log.Warn("single sign-on callback with the wrong state", "addr", addr)
		c.ssoFailed(w, r, "expired")
		return
	}
	if e := q.Get("error"); e != "" {
		c.log.Warn("the identity provider refused the sign-in", "addr", addr, "error", printable(e, 64),
			"description", printable(q.Get("error_description"), 300))
		c.sql.SignInRefused("", addr, audit.MethodSSO, "identity provider: "+printable(e, 64))
		c.ssoFailed(w, r, "denied")
		return
	}
	id, err := c.sso.Finish(r.Context(), pending, q.Get("code"))
	if err != nil {
		c.log.Warn("single sign-on could not be verified", "addr", addr, "err", err)
		c.sql.SignInRefused("", addr, audit.MethodSSO, "could not verify the sign-in")
		c.ssoFailed(w, r, "failed")
		return
	}
	user, err := c.sso.SignIn(r.Context(), id)
	var refusal *sso.Refusal
	if errors.As(err, &refusal) {
		name := refusal.User
		if name == "" {
			name = id.Username
		}
		c.sql.SignInRefused(name, addr, audit.MethodSSO, refusal.Reason)
		c.ssoFailed(w, r, refusal.Code)
		return
	}
	if err != nil {
		c.log.Error("single sign-on failed", "username", id.Username, "err", err)
		c.ssoFailed(w, r, "unavailable")
		return
	}
	c.sql.SignedIn(user, addr, audit.MethodSSO)
	if _, err := c.startSession(w, r, user, audit.MethodSSO); err != nil {
		writeError(w, http.StatusInternalServerError, "could not start a session")
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

// printable keeps up to n printable characters of what a provider sent,
// for logs.
func printable(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}, s)
	if runes := []rune(s); len(runes) > n {
		s = string(runes[:n]) + "…"
	}
	return s
}

// ssoStatus describes single sign-on for the admin page: its settings,
// whether the provider answers, the users linked to it, and the group
// mappings (SHOW SSO GROUPS, run as the admin).
func (c *Console) ssoStatus(w http.ResponseWriter, r *http.Request, req *request) {
	out := map[string]any{"enabled": c.sso != nil}
	if c.sso != nil {
		st := c.sso.Status(r.Context())
		out["status"] = st
		out["callback"] = c.callbackURL(r)
		ids, err := c.sso.Identities(r.Context())
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "could not read the linked users")
			return
		}
		type linked struct {
			User       string    `json:"user"`
			Email      string    `json:"email"`
			Groups     []string  `json:"groups"`
			LastSignIn time.Time `json:"last_sign_in"`
		}
		users := make([]linked, len(ids))
		for i, id := range ids {
			users[i] = linked{User: id.User, Email: id.Email, Groups: id.Groups, LastSignIn: id.LastSignIn}
		}
		out["users"] = users
	}
	res, err := c.lookup(r.Context(), req.user, r, "SHOW SSO GROUPS")
	if err != nil {
		e := toSQLError(err)
		out["mappings"] = grantsResult{Error: e.Message, Code: e.Code}
	} else {
		out["mappings"] = grantsResult{result: &res}
	}
	writeJSON(w, http.StatusOK, out)
}
