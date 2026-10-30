package console

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// Console sessions live in a cookie that is encrypted and authenticated
// with a key every replica shares, so they survive restarts and work on any
// replica. They end after sessionLifetime, or sessionIdle without a request,
// or at once when the user is dropped or disabled or their password changes.
const (
	sessionLifetime = 12 * time.Hour
	sessionIdle     = time.Hour
	// sessionRefresh is how often a request renews the cookie, recording
	// that the session is in use.
	sessionRefresh = time.Minute
)

// Cookie names. Under TLS the __Host- prefix makes browsers insist the
// cookie is Secure, host-only and for the whole site, so another site on a
// sibling domain cannot plant one.
const (
	cookieName       = "igloo-console"
	secureCookieName = "__Host-igloo-console"
)

// csrfHeader carries the session's CSRF token on every request that
// changes anything.
const csrfHeader = "X-CSRF-Token"

// session is what the cookie holds.
type session struct {
	User string `json:"u"`
	// Issued and Seen are Unix times: sign-in, and the last request.
	Issued int64 `json:"i"`
	Seen   int64 `json:"s"`
	// CSRF is a random token the page must echo in csrfHeader.
	CSRF string `json:"c"`
	// Password fingerprints the user's verifier and epoch at sign-in (see
	// credentials), so changing the password or disabling the user signs
	// out every console session.
	Password string `json:"p"`
	// Method is how the user signed in: audit.MethodPassword or
	// audit.MethodSSO.
	Method string `json:"m,omitempty"`
}

// credentials is what a session's Password fingerprints: the user's
// verifier, epoch and creation time, so a user dropped and created again
// under the same name, even without a password, does not inherit the old
// one's sessions. Users with a password and no epoch yet fingerprint the
// verifier alone, as before epochs existed, so their sessions stay valid
// across the upgrade.
func credentials(u auth.User) string {
	if u.Epoch == 0 && u.Verifier != "" {
		return u.Verifier
	}
	return u.Verifier + "\x00epoch:" + strconv.FormatInt(u.Epoch, 10) + "\x00created:" + strconv.FormatInt(u.CreatedAt.UnixMicro(), 10)
}

// sealer encrypts and authenticates cookies with AES-256-GCM.
type sealer struct {
	aead cipher.AEAD
	// mac fingerprints password verifiers.
	mac []byte
}

func newSealer(secret []byte) (*sealer, error) {
	if len(secret) < 32 {
		return nil, errors.New("console: the session key must be at least 32 bytes")
	}
	key, err := hkdf.Key(sha256.New, secret, nil, "igloo console session cookie v1", 32)
	if err != nil {
		return nil, err
	}
	mac, err := hkdf.Key(sha256.New, secret, nil, "igloo console password fingerprint v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealer{aead: aead, mac: mac}, nil
}

// cookieAD binds sealed cookies to their purpose.
var cookieAD = []byte("igloo-console-session")

func (s *sealer) seal(sess session) (string, error) { return s.sealAs(sess, cookieAD) }

func (s *sealer) open(value string) (session, error) {
	var sess session
	err := s.openAs(value, cookieAD, &sess)
	return sess, err
}

// sealAs encrypts and authenticates v as JSON for the purpose ad names.
func (s *sealer) sealAs(v any, ad []byte) (string, error) {
	plain, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(plain)+s.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(s.aead.Seal(nonce, nonce, plain, ad)), nil
}

// openAs reverses sealAs into v.
func (s *sealer) openAs(value string, ad []byte, v any) error {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(raw) < s.aead.NonceSize() {
		return errors.New("malformed cookie")
	}
	n := s.aead.NonceSize()
	plain, err := s.aead.Open(nil, raw[:n], raw[n:], ad)
	if err != nil {
		return errors.New("cookie does not verify")
	}
	return json.Unmarshal(plain, v)
}

// fingerprint identifies a password verifier without revealing it.
func (s *sealer) fingerprint(verifier string) string {
	m := hmac.New(sha256.New, s.mac)
	m.Write([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil)[:16])
}

func randomToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func cookieNameFor(r *http.Request) string {
	if r.TLS != nil {
		return secureCookieName
	}
	return cookieName
}

// setCookie stores sess in the response, or clears the cookie when sess is
// nil.
func (c *Console) setCookie(w http.ResponseWriter, r *http.Request, sess *session) error {
	cookie := &http.Cookie{
		Name:     cookieNameFor(r),
		Path:     "/",
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteStrictMode,
	}
	if sess == nil {
		cookie.MaxAge = -1
	} else {
		value, err := c.cookies.seal(*sess)
		if err != nil {
			return err
		}
		cookie.Value = value
		cookie.MaxAge = int(time.Until(time.Unix(sess.Issued, 0).Add(sessionLifetime)).Seconds())
	}
	http.SetCookie(w, cookie)
	return nil
}

// request is a signed-in request.
type request struct {
	user auth.User
	sess session
}

// signedIn wraps handlers that need a signed-in user. It checks the session
// cookie, re-reads the user (who may have been dropped or changed role
// since), and for anything but GET checks the CSRF token and the request's
// origin.
func (c *Console) signedIn(next func(http.ResponseWriter, *http.Request, *request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, status, msg := c.authenticate(r)
		if req == nil {
			if status == http.StatusUnauthorized {
				_ = c.setCookie(w, r, nil)
				body := map[string]any{"error": msg, "signin": c.signInAllowed(r) == nil}
				if c.sso != nil {
					body["sso"] = map[string]string{"name": c.sso.DisplayName()}
				}
				writeJSON(w, status, body)
				return
			}
			writeError(w, status, msg)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if !sameOrigin(r) {
				writeError(w, http.StatusForbidden, "cross-origin request refused")
				return
			}
			token := r.Header.Get(csrfHeader)
			if subtle.ConstantTimeCompare([]byte(token), []byte(req.sess.CSRF)) != 1 {
				writeError(w, http.StatusForbidden, "missing or wrong CSRF token; reload the page")
				return
			}
		}
		if now := time.Now(); now.Sub(time.Unix(req.sess.Seen, 0)) > sessionRefresh {
			req.sess.Seen = now.Unix()
			if err := c.setCookie(w, r, &req.sess); err != nil {
				writeError(w, http.StatusInternalServerError, "could not renew the session")
				return
			}
		}
		next(w, r, req)
	}
}

// admin wraps handlers only administrators may use. The SQL they run is
// checked again by the SQL server, as for any statement.
func (c *Console) admin(next func(http.ResponseWriter, *http.Request, *request)) http.HandlerFunc {
	return c.signedIn(func(w http.ResponseWriter, r *http.Request, req *request) {
		if req.user.Role != auth.RoleAdmin {
			writeError(w, http.StatusForbidden, "permission denied: this page requires the admin role")
			return
		}
		next(w, r, req)
	})
}

// authenticate returns the request's session and current user, or an HTTP
// status and message saying why there is none.
func (c *Console) authenticate(r *http.Request) (*request, int, string) {
	cookie, err := r.Cookie(cookieNameFor(r))
	if err != nil {
		return nil, http.StatusUnauthorized, "sign in"
	}
	sess, err := c.cookies.open(cookie.Value)
	if err != nil {
		return nil, http.StatusUnauthorized, "sign in"
	}
	now := time.Now()
	if now.After(time.Unix(sess.Issued, 0).Add(sessionLifetime)) || now.After(time.Unix(sess.Seen, 0).Add(sessionIdle)) {
		return nil, http.StatusUnauthorized, "your session expired; sign in again"
	}
	user, err := c.users.Get(r.Context(), sess.User)
	if errors.Is(err, auth.ErrNotFound) {
		return nil, http.StatusUnauthorized, "user no longer exists"
	}
	if err != nil {
		c.log.Warn("could not look up user", "user", sess.User, "err", err)
		return nil, http.StatusServiceUnavailable, "could not reach the metadata database"
	}
	switch {
	case user.Disabled:
		return nil, http.StatusUnauthorized, "your user was disabled"
	case user.IsService():
		return nil, http.StatusUnauthorized, "service accounts cannot use the console"
	case subtle.ConstantTimeCompare([]byte(c.cookies.fingerprint(credentials(user))), []byte(sess.Password)) != 1:
		return nil, http.StatusUnauthorized, "your password changed or your sessions were ended; sign in again"
	}
	return &request{user: user, sess: sess}, 0, ""
}

// sameOrigin reports whether a request came from the console's own pages.
// Browsers send Origin with every POST, and Sec-Fetch-Site in current
// versions; either one naming another site is refused.
func sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && strings.EqualFold(u.Host, r.Host)
}

// signInAllowed refuses sign-ins that would send a password unencrypted
// across a network: without TLS, only a browser on the same machine (or
// through an SSH tunnel or kubectl port-forward) may sign in. That is a
// request addressed to localhost, by its Host header, which catches a
// browser sent to the console across a network by mistake, and, since a
// client may send any Host, one from this machine or a private network
// (see privatePeer). It guards against that mistake; it is not an access
// control, as a client on a private network that reaches the port can
// address it as localhost.
func (c *Console) signInAllowed(r *http.Request) error {
	if r.TLS != nil || loopback(r.Host) && privatePeer(r.RemoteAddr) {
		return nil
	}
	return errors.New("this connection is not encrypted, so the console accepts sign-ins only at localhost; " +
		"configure TLS (IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE) to sign in from other machines")
}

// loopback reports whether host, from the Host header, names this machine.
func loopback(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// sharedAddressSpace is 100.64.0.0/10 (RFC 6598), which some Kubernetes
// networks give pods.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// privatePeer reports whether addr, a client's address as its connection
// reports it, is on this machine or a private network, as the Iceberg REST
// catalog requires of clients without TLS. It cannot be this machine
// alone: Docker's published ports, which the single-machine setup binds to
// loopback, present a client on the host as the bridge network's gateway.
func privatePeer(addr string) bool {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false
	}
	ip := ap.Addr().Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || sharedAddressSpace.Contains(ip)
}

type loginRequest struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// sessionInfo is what the page learns about its session.
type sessionInfo struct {
	User string    `json:"user"`
	Role auth.Role `json:"role"`
	CSRF string    `json:"csrf"`
	// Method is how the user signed in: password or sso.
	Method string `json:"method"`
}

// startSession signs user in with a new session cookie.
func (c *Console) startSession(w http.ResponseWriter, r *http.Request, user auth.User, method string) (session, error) {
	now := time.Now().Unix()
	sess := session{User: user.Name, Issued: now, Seen: now, CSRF: randomToken(),
		Password: c.cookies.fingerprint(credentials(user)), Method: method}
	return sess, c.setCookie(w, r, &sess)
}

func (c *Console) login(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		writeError(w, http.StatusForbidden, "cross-origin request refused")
		return
	}
	if err := c.signInAllowed(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var in loginRequest
	if !readJSON(w, r, 4<<10, &in) {
		return
	}
	name := strings.ToLower(strings.TrimSpace(in.User))
	if name == "" || in.Password == "" {
		writeError(w, http.StatusBadRequest, "enter a user name and password")
		return
	}
	user, err := c.sql.Authenticate(r.Context(), name, in.Password, remoteAddr(r))
	var blocked *pgwire.LoginBlockedError
	switch {
	case errors.Is(err, pgwire.ErrLoginFailed):
		writeError(w, http.StatusUnauthorized, fmt.Sprintf("password authentication failed for user %q", name))
		return
	case errors.Is(err, pgwire.ErrUserDisabled):
		writeError(w, http.StatusForbidden, fmt.Sprintf("user %q is not permitted to sign in", name))
		return
	case errors.As(err, &blocked):
		writeError(w, http.StatusTooManyRequests, blocked.Error())
		return
	case err != nil:
		c.log.Warn("sign-in failed", "user", name, "err", err)
		writeError(w, http.StatusServiceUnavailable, "could not check the password; try again")
		return
	}
	sess, err := c.startSession(w, r, user, audit.MethodPassword)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not start a session")
		return
	}
	writeJSON(w, http.StatusOK, sessionInfo{User: user.Name, Role: user.Role, CSRF: sess.CSRF, Method: sess.Method})
}

func (c *Console) session(w http.ResponseWriter, _ *http.Request, req *request) {
	method := req.sess.Method
	if method == "" {
		method = audit.MethodPassword
	}
	writeJSON(w, http.StatusOK, sessionInfo{User: req.user.Name, Role: req.user.Role, CSRF: req.sess.CSRF, Method: method})
}

func (c *Console) logout(w http.ResponseWriter, r *http.Request, req *request) {
	_ = c.setCookie(w, r, nil)
	c.log.Info("signed out", "user", req.user.Name, "addr", remoteAddr(r))
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
