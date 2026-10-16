package sharing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Recipients download data files from URLs that Igloo signs and serves
// itself, rather than object storage URLs: it works on every object store
// and with every kind of storage credential, keeps bucket names and paths
// private (the URL's last segment is encrypted), and lets Igloo end access
// at once. Each URL names one file, for one recipient token, for an hour,
// and every request for it checks that the token is still valid and that
// the table is still in a share granted to the recipient.

// accessTTL is how long a recipient's access to a shared table, once
// checked, is trusted without asking the metadata database again.
const accessTTL = 10 * time.Second

type cachedAccess struct {
	ok     bool
	loaded time.Time
}

func (s *Server) serveFile(w http.ResponseWriter, r *http.Request) {
	denied := func(msg string) {
		s.metrics.request("file_denied")
		writeError(w, &apiError{status: http.StatusForbidden, code: "PERMISSION_DENIED", msg: msg})
	}
	g, err := s.signer.openFile(r.PathValue("grant"), s.now())
	if err != nil {
		denied(err.Error())
		return
	}
	tok, err := s.bearer(r.Context(), g.Token)
	if err != nil || !tok.Valid || !s.now().Before(tok.Expires) {
		denied("the recipient's access has ended; ask the provider")
		return
	}
	ok, err := s.canRead(r.Context(), tok.Recipient, g.Table)
	if err != nil {
		s.log.Warn("could not check a recipient's access", "recipient", tok.Recipient, "err", err)
		writeError(w, &apiError{status: http.StatusServiceUnavailable, code: "INTERNAL_ERROR", msg: "could not check access; try again"})
		return
	}
	if !ok {
		denied("the table is no longer shared with you")
		return
	}
	if !strings.HasPrefix(g.Path, s.warehouse) {
		// Only Igloo signs these URLs, and only for warehouse files.
		s.log.Error("a signed file URL names a file outside the warehouse", "table", g.Table)
		denied("this file cannot be served")
		return
	}

	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Accept-Ranges", "bytes")
	sum := sha256.Sum256([]byte(g.Path))
	h.Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
	if r.Method == http.MethodHead {
		// The size is in the URL, so a HEAD request costs nothing in
		// object storage.
		h.Set("Content-Length", strconv.FormatInt(g.Size, 10))
		return
	}
	f, err := s.files.Open(g.Path)
	if err != nil {
		if isNotExist(err) {
			s.source.gone(g.Path)
		}
		s.log.Warn("could not open a shared data file", "table", g.Table, "err", err)
		writeError(w, notFound("the file no longer exists; list the table's files again").(*apiError))
		return
	}
	defer f.Close() //nolint:errcheck
	cw := &countingWriter{ResponseWriter: w}
	http.ServeContent(cw, r, "", time.Time{}, f)
	s.metrics.served(cw.n)
}

// canRead reports whether recipient may still read the shared table named
// share.schema.table, through a short-lived cache.
func (s *Server) canRead(ctx context.Context, recipient, table string) (bool, error) {
	key := recipient + "/" + table
	now := s.now()
	s.tokensMu.Lock()
	c, ok := s.access[key]
	s.tokensMu.Unlock()
	if ok && now.Sub(c.loaded) < accessTTL {
		return c.ok, nil
	}
	share, rest, _ := strings.Cut(table, ".")
	schema, name, _ := strings.Cut(rest, ".")
	shares, err := s.store.RecipientShares(ctx, recipient)
	if err != nil {
		return false, err
	}
	allowed := false
	if slices.ContainsFunc(shares, func(sh Share) bool { return sh.Name == share }) {
		tables, err := s.store.ShareTables(ctx, share)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, err
		}
		allowed = slices.ContainsFunc(tables, func(t SharedTable) bool {
			return strings.EqualFold(t.Schema, schema) && strings.EqualFold(t.Name, name)
		})
	}
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	if len(s.access) > 10_000 {
		clear(s.access)
	}
	s.access[key] = cachedAccess{ok: allowed, loaded: now}
	return allowed, nil
}

// countingWriter counts the bytes of a response body.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}
