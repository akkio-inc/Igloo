package sharing

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
)

// A recipient gets its bearer token from a one-time activation link that an
// admin sends it. Opening the link shows a page with a button; only the
// button's POST creates the token and downloads the profile file. Chat and
// mail tools that fetch links to preview them therefore cannot use up the
// link, and the token itself never passes through SQL, the admin, or any
// channel but the recipient's own download.

// profile is the protocol's profile file.
type profile struct {
	ShareCredentialsVersion int    `json:"shareCredentialsVersion"`
	Endpoint                string `json:"endpoint"`
	BearerToken             string `json:"bearerToken"`
	ExpirationTime          string `json:"expirationTime,omitempty"`
}

// activationCSP allows the page nothing but submitting its own form.
const activationCSP = "default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"

func (s *Server) activationPage(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", activationCSP)
	h.Set("X-Frame-Options", "DENY")
	h.Set("Content-Type", "text/html; charset=utf-8")
	if _, _, ok := parseToken(activationPrefix, r.PathValue("code")); !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, activationHTML("This activation link is not valid. Ask your data provider for a new one.", false))
		return
	}
	_, _ = fmt.Fprint(w, activationHTML("Download the credential file to start reading the data shared with you. "+
		"This link works once: keep the file safe, as anyone who has it can read the data.", true))
}

func activationHTML(msg string, button bool) string {
	form := ""
	if button {
		form = `<form method="post"><button type="submit">Download credential file</button></form>`
	}
	return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>Delta Sharing credential</title></head><body><main><h1>Delta Sharing credential</h1><p>` +
		html.EscapeString(msg) + `</p>` + form + `</main></body></html>`
}

// activate redeems an activation code: it creates the recipient's bearer
// token and sends the profile file.
func (s *Server) activate(w http.ResponseWriter, r *http.Request) {
	fail := func(status int, msg string) {
		w.Header().Set("Content-Security-Policy", activationCSP)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, activationHTML(msg, false))
	}
	id, hash, ok := parseToken(activationPrefix, r.PathValue("code"))
	if !ok {
		fail(http.StatusNotFound, "This activation link is not valid. Ask your data provider for a new one.")
		return
	}
	bearer, err := newToken(tokenPrefix)
	if err != nil {
		fail(http.StatusInternalServerError, "Could not create a token; try again.")
		return
	}
	a, err := s.store.Activate(r.Context(), id, hash, bearer)
	if errors.Is(err, ErrNotFound) {
		s.recordFailure(r, "", "unknown, used or expired activation link")
		fail(http.StatusNotFound, "This activation link was already used or has expired. Ask your data provider for a new one.")
		return
	}
	if err != nil {
		s.log.Warn("could not activate a recipient", "err", err)
		fail(http.StatusServiceUnavailable, "Could not activate the link; try again.")
		return
	}
	s.Forget()
	s.log.Info("recipient activated a token", "recipient", a.Recipient, "expires", a.Expires)
	if s.audit != nil {
		s.audit.RecordLogin(audit.Login{
			Time: s.now(), User: recipientUser + a.Recipient, Result: audit.LoginSuccess, Reason: "activated a token",
			ClientAddress: clientAddress(r), ApplicationName: truncate(r.UserAgent(), 200),
		})
	}
	p := profile{
		ShareCredentialsVersion: 1,
		Endpoint:                s.baseURL(r),
		BearerToken:             bearer.Text,
		ExpirationTime:          a.Expires.UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.share"`, a.Recipient))
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(p)
}
