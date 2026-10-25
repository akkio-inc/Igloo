package pgwire

import (
	"context"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Switches a session turns off and on for itself, as it does reading ahead
// (igloo_prefetch, prefetch.go). Each is on where the server has its feature
// on, unless the session turned it off:
//
//	SET igloo_report_rewrite = off | on   -- recognizing reports (IGLOO_REPORT_REWRITE)
//	SET igloo_bitmap_indexes = off | on   -- reading bitmap indexes (IGLOO_BITMAP_INDEXES)
//	SET igloo_id_counts = off | on        -- counting through ID domains' ids (IGLOO_ID_DOMAINS)
//
// and RESET and SHOW them.
type sessionSwitch struct {
	name             string
	set, reset, show *regexp.Regexp
	// off is the session's own setting, and on whether the switch is on.
	off func(sess *session) *bool
	on  func(s *Server, sess *session) bool
}

var sessionSwitches = []sessionSwitch{
	newSessionSwitch("igloo_report_rewrite", func(sess *session) *bool { return &sess.reportRewriteOff },
		func(s *Server, sess *session) bool { return s.reportRewrite.Load() && !sess.reportRewriteOff }),
	newSessionSwitch("igloo_bitmap_indexes", func(sess *session) *bool { return &sess.indexesOff },
		func(s *Server, sess *session) bool { return s.bitmapIndexesOn && !sess.indexesOff }),
	newSessionSwitch("igloo_id_counts", func(sess *session) *bool { return &sess.idCountsOff },
		func(s *Server, sess *session) bool { return s.idDomainsOn && !sess.idCountsOff }),
}

func newSessionSwitch(name string, off func(*session) *bool, on func(*Server, *session) bool) sessionSwitch {
	return sessionSwitch{
		name:  name,
		set:   regexp.MustCompile(`(?is)^\s*(?:SET\s+(?:SESSION\s+)?|ALTER\s+SESSION\s+SET\s+)"?` + name + `"?\s*(?:=|\bTO\b)\s*'?([a-z]+)'?\s*;?\s*$`),
		reset: regexp.MustCompile(`(?is)^\s*(?:RESET|ALTER\s+SESSION\s+UNSET)\s+"?` + name + `"?\s*;?\s*$`),
		show:  regexp.MustCompile(`(?is)^\s*SHOW\s+"?` + name + `"?\s*;?\s*$`),
		off:   off,
		on:    on,
	}
}

// switchCommand handles SET, RESET and SHOW of the session switches.
func (s *Server) switchCommand(sess *session, text string) (wire.PreparedStatements, bool) {
	query := stripComments(text)
	lower := strings.ToLower(query)
	if !strings.Contains(lower, "igloo_") || len(sqllex.Split(text)) > 1 {
		return nil, false
	}
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, text, fn), opts...)), true
	}
	for _, sw := range sessionSwitches {
		if !strings.Contains(lower, sw.name) {
			continue
		}
		if m := sw.set.FindStringSubmatch(query); m != nil {
			var off bool
			var err error
			switch strings.ToLower(m[1]) {
			case "on", "true", "default":
			case "off", "false":
				off = true
			default:
				err = pgerror.WithCode(errInvalidSwitch{sw.name, m[1]}, codes.InvalidParameterValue)
			}
			return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				if err != nil {
					return err
				}
				*sw.off(sess) = off
				return w.Complete("SET")
			})
		}
		if sw.reset.MatchString(query) {
			return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				*sw.off(sess) = false
				return w.Complete("RESET")
			})
		}
		if sw.show.MatchString(query) {
			return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				v := "off"
				if sw.on(s, sess) {
					v = "on"
				}
				if err := w.Row([]any{v}); err != nil {
					return err
				}
				return w.Complete("SHOW")
			}, wire.WithColumns(wire.Columns{{Name: sw.name, Oid: pgtype.TextOID, Width: -1}}))
		}
	}
	return nil, false
}

type errInvalidSwitch struct{ name, value string }

func (e errInvalidSwitch) Error() string {
	return "invalid value for parameter " + e.name + ": " + e.value + " (on or off)"
}
