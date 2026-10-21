package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sharing"
)

// Delta Sharing statements (package sharing). Igloo carries them out
// itself, like user management, and only admins may run them:
//
//	CREATE SHARE [IF NOT EXISTS] name [COMMENT 'text']
//	ALTER SHARE name ADD [TABLE] table [AS [schema.]alias] [WITH HISTORY]
//	ALTER SHARE name REMOVE [TABLE] [schema.]alias
//	DROP SHARE [IF EXISTS] name
//	SHOW SHARES
//	SHOW ALL IN SHARE name
//	SHOW GRANTS ON SHARE name
//	CREATE RECIPIENT [IF NOT EXISTS] name [COMMENT 'text'] [TOKEN LIFETIME 'duration']
//	ALTER RECIPIENT name ROTATE TOKEN [KEEP OLD FOR 'duration']
//	DROP RECIPIENT [IF EXISTS] name
//	SHOW RECIPIENTS
//	GRANT SELECT ON SHARE name TO RECIPIENT recipient
//	REVOKE SELECT ON SHARE name FROM RECIPIENT recipient
//	CREATE [OR REPLACE] PROVIDER [IF NOT EXISTS] name FROM 'profile file contents'
//	DROP PROVIDER [IF EXISTS] name
//	SHOW PROVIDERS
//	SHOW SHARES IN PROVIDER name
//	CREATE EXTERNAL TABLE [IF NOT EXISTS] table FROM SHARE provider.share.schema.table
//	DROP EXTERNAL TABLE [IF EXISTS] table
//	SHOW EXTERNAL TABLES

type shareKind int

const (
	shCreateShare shareKind = iota
	shDropShare
	shAddTable
	shRemoveTable
	shShowShares
	shShowShareTables
	shShowShareGrants
	shCreateRecipient
	shRotateToken
	shDropRecipient
	shShowRecipients
	shGrantShare
	shRevokeShare
	shCreateProvider
	shDropProvider
	shShowProviders
	shShowProviderTables
	shCreateExternal
	shDropExternal
	shShowExternal
)

// shareCommand is a parsed Delta Sharing statement.
type shareCommand struct {
	kind                                  shareKind
	tag                                   string
	ifExists, ifNotExists, replace, histo bool
	// name is the share, recipient or provider.
	name    string
	comment string
	// recipient is the grantee of GRANT and REVOKE.
	recipient string
	// table is the catalog table of ADD TABLE or the external table.
	table []string
	// alias is the shared name of ADD TABLE and REMOVE TABLE.
	alias []string
	// remote is provider.share.schema.table of an external table.
	remote []string
	// lifetime is TOKEN LIFETIME, keep is KEEP OLD FOR.
	lifetime, keep time.Duration
	// profile is a provider's profile file.
	profile string
}

var (
	shareColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "comment", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	shareTableColumns = wire.Columns{
		{Name: "share", Oid: pgtype.TextOID, Width: -1},
		{Name: "schema", Oid: pgtype.TextOID, Width: -1},
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "source", Oid: pgtype.TextOID, Width: -1},
		{Name: "history", Oid: pgtype.BoolOID, Width: 1},
		{Name: "added_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "added_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	shareGrantColumns = wire.Columns{
		{Name: "share", Oid: pgtype.TextOID, Width: -1},
		{Name: "recipient", Oid: pgtype.TextOID, Width: -1},
		{Name: "privilege", Oid: pgtype.TextOID, Width: -1},
		{Name: "granted_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "granted_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	activationColumns = wire.Columns{
		{Name: "recipient", Oid: pgtype.TextOID, Width: -1},
		{Name: "activation_link", Oid: pgtype.TextOID, Width: -1},
		{Name: "link_expires_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	recipientColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "comment", Oid: pgtype.TextOID, Width: -1},
		{Name: "shares", Oid: pgtype.TextOID, Width: -1},
		{Name: "token", Oid: pgtype.TextOID, Width: -1},
		{Name: "token_expires_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "token_lifetime", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	providerColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "endpoint", Oid: pgtype.TextOID, Width: -1},
		{Name: "token_expires_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	remoteTableColumns = wire.Columns{
		{Name: "share", Oid: pgtype.TextOID, Width: -1},
		{Name: "schema", Oid: pgtype.TextOID, Width: -1},
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
	}
	externalColumns = wire.Columns{
		{Name: "schema", Oid: pgtype.TextOID, Width: -1},
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "provider", Oid: pgtype.TextOID, Width: -1},
		{Name: "share", Oid: pgtype.TextOID, Width: -1},
		{Name: "remote_schema", Oid: pgtype.TextOID, Width: -1},
		{Name: "remote_table", Oid: pgtype.TextOID, Width: -1},
		{Name: "version", Oid: pgtype.Int8OID, Width: 8},
		{Name: "checked_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
)

func (c *shareCommand) columns() wire.Columns {
	switch c.kind {
	case shShowShares:
		return shareColumns
	case shShowShareTables:
		return shareTableColumns
	case shShowShareGrants:
		return shareGrantColumns
	case shCreateRecipient, shRotateToken:
		return activationColumns
	case shShowRecipients:
		return recipientColumns
	case shShowProviders:
		return providerColumns
	case shShowProviderTables:
		return remoteTableColumns
	case shShowExternal:
		return externalColumns
	}
	return nil
}

// parseShareCommand parses one statement's tokens. ok is false when it is
// not a Delta Sharing statement.
func parseShareCommand(toks []token) (c *shareCommand, ok bool, err error) {
	p := &tokParser{toks: toks}
	syntax := func(format string, args ...any) (*shareCommand, bool, error) {
		return nil, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	c = &shareCommand{}
	name := func() bool {
		var err error
		if p.i >= len(p.toks) {
			return false
		}
		c.name, err = principalName(p.toks[p.i])
		p.i++
		return err == nil
	}
	switch {
	case p.words("CREATE", "SHARE"):
		c.kind, c.tag = shCreateShare, "CREATE SHARE"
		c.ifNotExists = p.words("IF", "NOT", "EXISTS")
		if !name() {
			return syntax("CREATE SHARE needs a share name: lowercase letters, digits and underscores")
		}
		if p.words("COMMENT") {
			if c.comment, ok = p.str(); !ok {
				return syntax("COMMENT needs a quoted string")
			}
		}
	case p.words("DROP", "SHARE"):
		c.kind, c.tag = shDropShare, "DROP SHARE"
		c.ifExists = p.words("IF", "EXISTS")
		if !name() {
			return syntax("DROP SHARE needs a share name")
		}
	case p.words("ALTER", "SHARE"):
		c.tag = "ALTER SHARE"
		if !name() {
			return syntax("ALTER SHARE needs a share name")
		}
		switch {
		case p.words("ADD"):
			c.kind = shAddTable
			p.words("TABLE")
			if c.table = p.name(); c.table == nil || len(c.table) > 3 {
				return syntax("expected ALTER SHARE name ADD TABLE [schema.]table")
			}
			if p.words("AS") {
				if c.alias = p.name(); c.alias == nil || len(c.alias) > 2 {
					return syntax("expected AS [schema.]alias")
				}
			}
			c.histo = p.words("WITH", "HISTORY")
		case p.words("REMOVE"):
			c.kind = shRemoveTable
			p.words("TABLE")
			if c.alias = p.name(); c.alias == nil || len(c.alias) > 2 {
				return syntax("expected ALTER SHARE name REMOVE TABLE [schema.]table, as recipients see it")
			}
		default:
			return syntax("expected ALTER SHARE name ADD TABLE or REMOVE TABLE")
		}
	case p.words("SHOW", "SHARES", "IN", "PROVIDER"):
		c.kind, c.tag = shShowProviderTables, "SHOW"
		if !name() {
			return syntax("SHOW SHARES IN PROVIDER needs a provider name")
		}
	case p.words("SHOW", "SHARES"):
		c.kind, c.tag = shShowShares, "SHOW"
	case p.words("SHOW", "ALL", "IN", "SHARE"):
		c.kind, c.tag = shShowShareTables, "SHOW"
		if !name() {
			return syntax("SHOW ALL IN SHARE needs a share name")
		}
	case p.words("SHOW", "GRANTS", "ON", "SHARE"):
		c.kind, c.tag = shShowShareGrants, "SHOW"
		if !name() {
			return syntax("SHOW GRANTS ON SHARE needs a share name")
		}
	case p.words("CREATE", "RECIPIENT"):
		c.kind, c.tag = shCreateRecipient, "CREATE RECIPIENT"
		c.ifNotExists = p.words("IF", "NOT", "EXISTS")
		if !name() {
			return syntax("CREATE RECIPIENT needs a recipient name: lowercase letters, digits and underscores")
		}
		for p.i < len(p.toks) {
			switch {
			case p.words("COMMENT"):
				if c.comment, ok = p.str(); !ok {
					return syntax("COMMENT needs a quoted string")
				}
			case p.words("TOKEN", "LIFETIME"):
				if c.lifetime, err = p.duration(); err != nil {
					return nil, true, err
				}
			default:
				return syntax("unexpected %q; expected COMMENT or TOKEN LIFETIME", p.toks[p.i].text)
			}
		}
	case p.words("ALTER", "RECIPIENT"):
		c.kind, c.tag = shRotateToken, "ALTER RECIPIENT"
		if !name() || !p.words("ROTATE", "TOKEN") {
			return syntax("expected ALTER RECIPIENT name ROTATE TOKEN [KEEP OLD FOR 'duration']")
		}
		if p.words("KEEP", "OLD", "FOR") {
			if c.keep, err = p.duration(); err != nil {
				return nil, true, err
			}
		}
	case p.words("DROP", "RECIPIENT"):
		c.kind, c.tag = shDropRecipient, "DROP RECIPIENT"
		c.ifExists = p.words("IF", "EXISTS")
		if !name() {
			return syntax("DROP RECIPIENT needs a recipient name")
		}
	case p.words("SHOW", "RECIPIENTS"):
		c.kind, c.tag = shShowRecipients, "SHOW"
	case p.words("GRANT", "SELECT", "ON", "SHARE"), p.words("REVOKE", "SELECT", "ON", "SHARE"):
		grant := p.toks[0].upper() == "GRANT"
		c.kind, c.tag, c.recipient = shGrantShare, "GRANT", ""
		to := "TO"
		if !grant {
			c.kind, c.tag, to = shRevokeShare, "REVOKE", "FROM"
		}
		if !name() || !p.words(to, "RECIPIENT") || p.i >= len(p.toks) {
			return syntax("expected %s SELECT ON SHARE name %s RECIPIENT recipient", c.tag, to)
		}
		if c.recipient, err = principalName(p.toks[p.i]); err != nil {
			return nil, true, err
		}
		p.i++
	case p.words("CREATE", "OR", "REPLACE", "PROVIDER"), p.words("CREATE", "PROVIDER"):
		c.kind, c.tag = shCreateProvider, "CREATE PROVIDER"
		c.replace = p.toks[1].upper() == "OR"
		c.ifNotExists = p.words("IF", "NOT", "EXISTS")
		if !name() || !p.words("FROM") {
			return syntax("expected CREATE PROVIDER name FROM 'profile file contents'")
		}
		if c.profile, ok = p.str(); !ok {
			return syntax("FROM needs the profile file's contents as a quoted string")
		}
	case p.words("DROP", "PROVIDER"):
		c.kind, c.tag = shDropProvider, "DROP PROVIDER"
		c.ifExists = p.words("IF", "EXISTS")
		if !name() {
			return syntax("DROP PROVIDER needs a provider name")
		}
	case p.words("SHOW", "PROVIDERS"):
		c.kind, c.tag = shShowProviders, "SHOW"
	case p.words("CREATE", "EXTERNAL", "TABLE"):
		c.kind, c.tag = shCreateExternal, "CREATE EXTERNAL TABLE"
		c.ifNotExists = p.words("IF", "NOT", "EXISTS")
		if c.table = p.name(); c.table == nil || len(c.table) > 3 {
			return syntax("CREATE EXTERNAL TABLE needs a table name")
		}
		if !p.words("FROM", "SHARE") {
			return syntax("expected CREATE EXTERNAL TABLE name FROM SHARE provider.share.schema.table")
		}
		if c.remote = p.name(); len(c.remote) != 4 {
			return syntax("expected FROM SHARE provider.share.schema.table")
		}
		// The parts are names already, folded or quoted.
		if c.name, err = principalName(token{kind: tokQuoted, text: c.remote[0]}); err != nil {
			return nil, true, err
		}
	case p.words("DROP", "EXTERNAL", "TABLE"):
		c.kind, c.tag = shDropExternal, "DROP EXTERNAL TABLE"
		c.ifExists = p.words("IF", "EXISTS")
		if c.table = p.name(); c.table == nil || len(c.table) > 3 {
			return syntax("DROP EXTERNAL TABLE needs a table name")
		}
	case p.words("SHOW", "EXTERNAL", "TABLES"):
		c.kind, c.tag = shShowExternal, "SHOW"
	default:
		return nil, false, nil
	}
	if p.i < len(p.toks) {
		return syntax("unexpected %q at the end of %s", p.toks[p.i].text, c.tag)
	}
	return c, true, nil
}

// tokParser reads a statement's tokens in order.
type tokParser struct {
	toks []token
	i    int
}

// words consumes the keywords if they come next.
func (p *tokParser) words(words ...string) bool {
	if p.i+len(words) > len(p.toks) {
		return false
	}
	for j, w := range words {
		if p.toks[p.i+j].upper() != w {
			return false
		}
	}
	p.i += len(words)
	return true
}

// str consumes a string literal.
func (p *tokParser) str() (string, bool) {
	if p.i < len(p.toks) && p.toks[p.i].kind == tokString {
		p.i++
		return p.toks[p.i-1].text, true
	}
	return "", false
}

// name consumes a possibly qualified name.
func (p *tokParser) name() []string {
	parts, n := tokenName(p.toks[p.i:])
	p.i += n
	return parts
}

// duration consumes a quoted duration such as '90d', '36h' or '30 days'.
func (p *tokParser) duration() (time.Duration, error) {
	s, ok := p.str()
	if !ok {
		return 0, pgerror.WithCode(errors.New("expected a duration as a quoted string, such as '90 days'"), codes.Syntax)
	}
	d, err := parseShareDuration(s)
	if err != nil {
		return 0, pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	return d, nil
}

// parseShareDuration reads durations such as 90d, 36h, 30m, '30 days' or '12 hours'.
func parseShareDuration(s string) (time.Duration, error) {
	bad := fmt.Errorf("invalid duration %q; use for example '90 days', '36h' or '30m'", s)
	s = strings.ToLower(strings.TrimSpace(s))
	if num, unit, ok := strings.Cut(s, " "); ok {
		n, err := strconv.Atoi(num)
		if err != nil || n < 0 {
			return 0, bad
		}
		per := map[string]time.Duration{"day": 24 * time.Hour, "hour": time.Hour, "minute": time.Minute, "second": time.Second}[strings.TrimSuffix(strings.TrimSpace(unit), "s")]
		if per == 0 {
			return 0, bad
		}
		return time.Duration(n) * per, nil
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil || n < 0 {
			return 0, bad
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, bad
	}
	return d, nil
}

// sharingError gives sharing errors PostgreSQL codes.
func sharingError(err error, what string) error {
	if err == nil || pgerror.GetCode(err) != codes.Uncategorized {
		return err
	}
	msg := errors.New(sharing.Describe(err, what))
	switch {
	case errors.Is(err, sharing.ErrNotFound):
		return pgerror.WithCode(msg, codes.UndefinedObject)
	case errors.Is(err, sharing.ErrExists):
		return pgerror.WithCode(msg, codes.DuplicateObject)
	case errors.Is(err, sharing.ErrInUse):
		return pgerror.WithCode(msg, codes.DependentObjectsStillExist)
	case sharing.IsNotShareable(err), sharing.IsProtected(err):
		return pgerror.WithCode(msg, codes.FeatureNotSupported)
	case sharing.IsProviderError(err):
		return pgerror.WithCode(msg, codeExternal)
	}
	return pgerror.WithCode(msg, codes.InvalidParameterValue)
}

// codeExternal is fdw_error, for failures reading an external table from
// its provider.
const codeExternal = codes.Code("HV000")

// errSharingUnavailable is returned when Delta Sharing is not set up.
var errSharingUnavailable = pgerror.WithCode(sharing.ErrUnavailable, codes.FeatureNotSupported)

// runShareCommand carries out a Delta Sharing statement for sess.
func (s *Server) runShareCommand(ctx context.Context, sess *session, c *shareCommand, w wire.DataWriter) error {
	me := sess.currentUser()
	if me.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: managing %s requires the admin role", kindNoun(c)),
			codes.InsufficientPrivilege)
	}
	if s.shares == nil {
		return errSharingUnavailable
	}
	svc := s.shares
	done := func(err error, what string) error {
		if err != nil {
			return sharingError(err, what)
		}
		s.log.Info("delta sharing changed", "statement", c.tag, "name", c.name, "by", me.Name)
		return w.Complete(c.tag)
	}
	share := fmt.Sprintf("share %q", c.name)
	switch c.kind {
	case shCreateShare:
		err := svc.CreateShare(ctx, c.name, c.comment, me.Name)
		if errors.Is(err, sharing.ErrExists) && c.ifNotExists {
			err = nil
		}
		return done(err, share)
	case shDropShare:
		err := svc.DropShare(ctx, c.name)
		if errors.Is(err, sharing.ErrNotFound) && c.ifExists {
			err = nil
		}
		return done(err, share)
	case shAddTable:
		t, err := s.shareSource(ctx, sess, c)
		if err != nil {
			return err
		}
		return done(svc.AddTable(ctx, t), fmt.Sprintf("%s or a table %s.%s in it", share, t.Schema, t.Name))
	case shRemoveTable:
		return done(s.removeSharedTable(ctx, c), fmt.Sprintf("table %s in %s", strings.Join(c.alias, "."), share))
	case shShowShares:
		shares, err := svc.Shares(ctx)
		if err != nil {
			return sharingError(err, "")
		}
		for _, sh := range shares {
			if err := w.Row([]any{sh.Name, sh.Comment, sh.CreatedBy, sh.CreatedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case shShowShareTables:
		tables, err := svc.ShareTables(ctx, c.name)
		if err != nil {
			return sharingError(err, share)
		}
		for _, t := range tables {
			source := s.catalogName() + "." + t.SourceSchema + "." + t.SourceTable
			if err := w.Row([]any{t.Share, t.Schema, t.Name, source, t.History, t.AddedBy, t.AddedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case shShowShareGrants:
		if _, err := svc.ShareTables(ctx, c.name); err != nil {
			return sharingError(err, share)
		}
		grants, err := svc.ShareGrants(ctx, c.name)
		if err != nil {
			return sharingError(err, share)
		}
		for _, g := range grants {
			if err := w.Row([]any{g.Share, g.Recipient, "SELECT", g.GrantedBy, g.GrantedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case shCreateRecipient:
		a, err := svc.CreateRecipient(ctx, c.name, c.comment, c.lifetime, me.Name)
		if errors.Is(err, sharing.ErrExists) && c.ifNotExists {
			return w.Complete(c.tag)
		}
		if err != nil {
			return sharingError(err, fmt.Sprintf("recipient %q", c.name))
		}
		s.log.Info("recipient created", "recipient", c.name, "by", me.Name)
		if err := w.Row([]any{c.name, a.Link, a.Expires}); err != nil {
			return err
		}
		return w.Complete(c.tag)
	case shRotateToken:
		a, err := svc.RotateToken(ctx, c.name, c.keep)
		if err != nil {
			return sharingError(err, fmt.Sprintf("recipient %q", c.name))
		}
		s.log.Info("recipient token rotated", "recipient", c.name, "old_token_for", c.keep, "by", me.Name)
		if err := w.Row([]any{c.name, a.Link, a.Expires}); err != nil {
			return err
		}
		return w.Complete(c.tag)
	case shDropRecipient:
		err := svc.DropRecipient(ctx, c.name)
		if errors.Is(err, sharing.ErrNotFound) && c.ifExists {
			err = nil
		}
		return done(err, fmt.Sprintf("recipient %q", c.name))
	case shShowRecipients:
		return s.showRecipients(ctx, w)
	case shGrantShare:
		return done(svc.GrantShare(ctx, c.name, c.recipient, me.Name), share)
	case shRevokeShare:
		return done(svc.RevokeShare(ctx, c.name, c.recipient), share)
	case shCreateProvider:
		err := svc.Consumer().CreateProvider(ctx, c.name, c.profile, me.Name, c.replace)
		if errors.Is(err, sharing.ErrExists) && c.ifNotExists {
			err = nil
		}
		return done(err, fmt.Sprintf("provider %q", c.name))
	case shDropProvider:
		err := svc.Consumer().DropProvider(ctx, c.name)
		if errors.Is(err, sharing.ErrNotFound) && c.ifExists {
			err = nil
		}
		return done(err, fmt.Sprintf("provider %q", c.name))
	case shShowProviders:
		providers, err := svc.Consumer().Providers(ctx)
		if err != nil {
			return sharingError(err, "")
		}
		for _, p := range providers {
			if err := w.Row([]any{p.Name, p.Endpoint, p.TokenExpires, p.CreatedBy, p.CreatedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case shShowProviderTables:
		tables, err := svc.Consumer().RemoteTables(ctx, c.name)
		if err != nil {
			return sharingError(err, fmt.Sprintf("provider %q", c.name))
		}
		for _, t := range tables {
			if err := w.Row([]any{t.Share, t.Schema, t.Name}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case shCreateExternal:
		return s.createExternalTable(ctx, sess, me, c, w)
	case shDropExternal:
		return s.dropExternalTable(ctx, sess, c, w)
	case shShowExternal:
		tables, err := svc.Consumer().ExternalTables(ctx)
		if err != nil {
			return sharingError(err, "")
		}
		for _, t := range tables {
			var version, checked any
			if t.Version >= 0 {
				version, checked = t.Version, t.Checked
			}
			row := []any{t.Schema, t.Name, t.Provider, t.Share, t.RemoteSchema, t.RemoteTable, version, checked, t.CreatedBy, t.CreatedAt}
			if err := w.Row(row); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	}
	return pgerror.WithCode(errors.New("unsupported sharing statement"), codes.FeatureNotSupported)
}

// kindNoun names what a statement manages, for permission errors.
func kindNoun(c *shareCommand) string {
	switch c.kind {
	case shCreateRecipient, shRotateToken, shDropRecipient, shShowRecipients:
		return "recipients"
	case shCreateProvider, shDropProvider, shShowProviders, shShowProviderTables:
		return "providers"
	case shCreateExternal, shDropExternal, shShowExternal:
		return "external tables"
	}
	return "shares"
}

// shareSource resolves the catalog table of ALTER SHARE ... ADD TABLE and
// the name recipients will see it under.
func (s *Server) shareSource(ctx context.Context, sess *session, c *shareCommand) (sharing.SharedTable, error) {
	if s.catalogName() == "" {
		return sharing.SharedTable{}, errSharingUnavailable
	}
	o, err := s.resolveGrantTable(ctx, sess, c.table)
	if err != nil {
		return sharing.SharedTable{}, err
	}
	// duckdb_tables() walks the catalog's in-memory set; hold the exclusive
	// side so it does not run while a table is renamed or dropped on this
	// instance (reentrant; see holdCatalog).
	defer s.holdCatalog(sess)()
	tables, err := queryStrings(ctx, sess, `SELECT table_name FROM system.main.duckdb_tables()
		WHERE database_name = $1 AND schema_name = $2 AND table_name = $3`, s.catalogName(), o.Schema, o.Table)
	if err != nil {
		return sharing.SharedTable{}, err
	}
	if len(tables) == 0 {
		return sharing.SharedTable{}, pgerror.WithCode(fmt.Errorf("%s is not a table of the Iceberg catalog; only tables can be shared", o),
			codes.WrongObjectType)
	}
	t := sharing.SharedTable{Share: c.name, Schema: o.Schema, Name: o.Table, SourceSchema: o.Schema, SourceTable: o.Table,
		History: c.histo, AddedBy: sess.currentUser().Name}
	switch len(c.alias) {
	case 1:
		t.Name = c.alias[0]
	case 2:
		t.Schema, t.Name = c.alias[0], c.alias[1]
	}
	return t, nil
}

// removeSharedTable removes a table from a share by the name recipients see,
// or by its table name alone when only one of the share's tables has it.
func (s *Server) removeSharedTable(ctx context.Context, c *shareCommand) error {
	if len(c.alias) == 2 {
		return s.shares.RemoveTable(ctx, c.name, c.alias[0], c.alias[1])
	}
	tables, err := s.shares.ShareTables(ctx, c.name)
	if err != nil {
		return err
	}
	var matches []sharing.SharedTable
	for _, t := range tables {
		if strings.EqualFold(t.Name, c.alias[0]) {
			matches = append(matches, t)
		}
	}
	switch len(matches) {
	case 0:
		return sharing.ErrNotFound
	case 1:
		return s.shares.RemoveTable(ctx, c.name, matches[0].Schema, matches[0].Name)
	}
	return pgerror.WithCode(fmt.Errorf("share %q has several tables named %q; name the schema too", c.name, c.alias[0]), codes.AmbiguousAlias)
}

func (s *Server) showRecipients(ctx context.Context, w wire.DataWriter) error {
	recipients, err := s.shares.Recipients(ctx)
	if err != nil {
		return sharingError(err, "")
	}
	for _, r := range recipients {
		state := "none: rotate to issue one"
		switch {
		case r.TokenExpires != nil && r.Pending:
			state = "active, new link pending"
		case r.TokenExpires != nil:
			state = "active"
		case r.Pending:
			state = "awaiting activation"
		}
		var expires any
		if r.TokenExpires != nil {
			expires = *r.TokenExpires
		}
		row := []any{r.Name, r.Comment, strings.Join(r.Shares, ", "), state, expires, lifetimeText(r.TokenLifetime), r.CreatedBy, r.CreatedAt}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// lifetimeText renders a token lifetime, in days when whole.
func lifetimeText(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%d days", d/(24*time.Hour))
	}
	return d.String()
}

// externalNamed returns the external table name means, if it is one:
// written as the catalog's tables are, schema.table or table, or with the
// catalog's name or the local database's in front.
func (s *Server) externalNamed(ctx context.Context, sess *session, name []string) (schema, table string, ok bool) {
	if s.external == nil || len(name) == 0 || len(name) > 3 {
		return "", "", false
	}
	switch len(name) {
	case 3:
		if !strings.EqualFold(name[0], s.catalogName()) && !strings.EqualFold(name[0], s.engine.LocalDatabase()) {
			return "", "", false
		}
		schema = name[1]
	case 2:
		schema = name[0]
	default:
		db, current, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return "", "", false
		}
		schema = current
		if !strings.EqualFold(db, s.catalogName()) {
			schema = "public"
		}
	}
	table = name[len(name)-1]
	_, ok = s.external.IsExternal(schema, table)
	return schema, table, ok
}

// externalTarget resolves the name of an external table: the schema of the
// Iceberg catalog it lives in, which must exist, and its name there.
func (s *Server) externalTarget(ctx context.Context, sess *session, name []string) (schema, table string, err error) {
	catalogName := s.catalogName()
	if catalogName == "" {
		return "", "", errSharingUnavailable
	}
	switch len(name) {
	case 3:
		if !strings.EqualFold(name[0], catalogName) {
			return "", "", pgerror.WithCode(fmt.Errorf("external tables live in the %s catalog, not %q", catalogName, name[0]), codes.FeatureNotSupported)
		}
		schema = name[1]
	case 2:
		schema = name[0]
	default:
		db, current, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return "", "", err
		}
		schema = current
		if !strings.EqualFold(db, catalogName) {
			schema = "public"
		}
	}
	actual, ok, err := s.catalogSchema(ctx, sess, schema)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "", pgerror.WithCode(fmt.Errorf("schema %q does not exist; create it first", schema), codes.InvalidSchemaName)
	}
	return actual, name[len(name)-1], nil
}

func (s *Server) createExternalTable(ctx context.Context, sess *session, me auth.User, c *shareCommand, w wire.DataWriter) error {
	schema, name, err := s.externalTarget(ctx, sess, c.table)
	if err != nil {
		return err
	}
	if _, ok := s.shares.Consumer().IsExternal(schema, name); ok && c.ifNotExists {
		return w.Complete(c.tag)
	}
	if s.gov.snapshot(ctx).Tagged(schema, name) != nil {
		// Policies stay with a table's name after it is dropped, and could
		// not apply to an external table there.
		return pgerror.WithCode(fmt.Errorf("%s.%s has policies or tags, left by a table of that name; policies cannot apply to an external table, so unset them first",
			schema, name), codes.ObjectNotInPrerequisiteState)
	}
	relations, err := s.catalogRelations(ctx, sess, schema)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(relations, func(r string) bool { return strings.EqualFold(r, name) }) {
		return pgerror.WithCode(fmt.Errorf("relation %s.%s already exists", schema, name), codes.DuplicateRelation)
	}
	def := sharing.ExternalTable{
		Schema: schema, Name: name, Provider: c.name, Share: c.remote[1], RemoteSchema: c.remote[2], RemoteTable: c.remote[3],
		CreatedBy: me.Name,
	}
	err = s.shares.Consumer().CreateExternalTable(ctx, def)
	s.catalogChanged()
	s.sessions.expireAll()
	if err != nil {
		return sharingError(err, fmt.Sprintf("provider %q", c.name))
	}
	s.recordOwner(ctx, me, &createdObject{object: auth.Object{Schema: schema, Table: name}})
	s.log.Info("external table created", "table", schema+"."+name, "remote", def.Remote(), "by", me.Name)
	return w.Complete(c.tag)
}

func (s *Server) dropExternalTable(ctx context.Context, sess *session, c *shareCommand, w wire.DataWriter) error {
	schema, name, err := s.externalTarget(ctx, sess, c.table)
	if err != nil {
		if c.ifExists && pgerror.GetCode(err) == codes.InvalidSchemaName {
			return w.Complete(c.tag)
		}
		return err
	}
	err = s.shares.Consumer().DropExternalTable(ctx, schema, name)
	if errors.Is(err, sharing.ErrNotFound) && c.ifExists {
		return w.Complete(c.tag)
	}
	if err != nil {
		return sharingError(err, fmt.Sprintf("external table %s.%s", schema, name))
	}
	// Grants follow names, so a table created later under this name must
	// not inherit these.
	if ferr := s.grants.ForgetObject(ctx, auth.Object{Schema: schema, Table: name}); ferr != nil {
		s.log.Warn("could not remove a dropped external table's grants", "table", schema+"."+name, "err", ferr)
	}
	s.catalogChanged()
	s.sessions.expireAll()
	s.log.Info("external table dropped", "table", schema+"."+name, "by", sess.currentUser().Name)
	return w.Complete(c.tag)
}
