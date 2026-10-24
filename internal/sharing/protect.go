package sharing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
)

// Masking and row access policies (package policy) apply to what Igloo
// reads for its users: Igloo rewrites their statements to read protected
// tables through the policies, which are evaluated for the user's roles.
// Delta Sharing recipients download a table's data files and read them with
// their own engines, which cannot apply Igloo's policies, and they have no
// Igloo roles the policies could be evaluated for. So, as Databricks does
// for open sharing, a table with a row access policy, or a masking policy
// on any column (set on it or through a tag), cannot be added to a share,
// and a shared table that gains one is refused to recipients from then on:
// its version, metadata and files, and every download of a file listed
// before.

// Protection names the masking and row access policies that protect the
// catalog table schema.table, and none when nothing does. Igloo's SQL
// server provides it (pgwire.Server.ProtectedBy), from the policies in
// force on this replica.
type Protection func(ctx context.Context, schema, table string) []string

// ProtectedError says a table has policies that recipients cannot apply.
type ProtectedError struct {
	// Schema and Table name the catalog table.
	Schema, Table string
	// Policies name the policies that protect it.
	Policies []string
}

func (e *ProtectedError) Error() string {
	return fmt.Sprintf("table %s.%s has what Delta Sharing recipients cannot be given (%s): masking or row access policies, "+
		"which they cannot apply as they read the table's files with their own tools, or columns of ID domains' ids, which are "+
		"Igloo's own. Share a table that holds only what recipients may see instead, for example one made with CREATE TABLE ... "+
		"AS SELECT the rows and columns they may read", e.Schema, e.Table, strings.Join(e.Policies, ", "))
}

// IsProtected reports whether err says a table has policies.
func IsProtected(err error) bool {
	var pe *ProtectedError
	return errors.As(err, &pe)
}

// errPoliciesUnknown is returned while no Protection is set: Igloo cannot
// tell which tables have policies, so it serves none.
var errPoliciesUnknown = errors.New("the provider cannot check the table's policies yet; try again shortly")

// protection holds the Protection of a Server; it is set once the SQL
// server that keeps the policies is up.
type protection struct{ p atomic.Pointer[Protection] }

func (pr *protection) set(p Protection) {
	if p == nil {
		pr.p.Store(nil)
		return
	}
	pr.p.Store(&p)
}

// check returns a ProtectedError if policies protect schema.table, and
// errPoliciesUnknown if no Protection is set.
func (pr *protection) check(ctx context.Context, schema, table string) error {
	p := pr.p.Load()
	if p == nil {
		return errPoliciesUnknown
	}
	if names := (*p)(ctx, schema, table); len(names) > 0 {
		return &ProtectedError{Schema: schema, Table: table, Policies: names}
	}
	return nil
}

// known reports whether a Protection is set.
func (pr *protection) known() bool { return pr.p.Load() != nil }

// SetProtection gives the endpoint the policies' Protection. Until it is
// set, every request about a table is refused.
func (s *Server) SetProtection(p Protection) { s.protection.set(p) }

// refusal is what a recipient is told about a protected table. It does
// not name the policies, which are the provider's business; the history
// does.
func refusal(shared SharedTable) *apiError {
	return &apiError{status: http.StatusForbidden, code: "PERMISSION_DENIED", msg: fmt.Sprintf(
		"table %s.%s is protected by the provider's masking or row access policies, which Delta Sharing recipients "+
			"cannot apply, so it cannot be read through this share; ask the provider", shared.Schema, shared.Name)}
}

// checkTable refuses a shared table whose catalog table has policies, and
// notes them for the history.
func (s *Server) checkTable(ctx context.Context, c *call, t SharedTable) error {
	err := s.protection.check(ctx, t.SourceSchema, t.SourceTable)
	var pe *ProtectedError
	switch {
	case errors.As(err, &pe):
		if c != nil {
			c.policies = pe.Policies
		}
		s.log.Warn("refused a shared table that has policies", "recipient", recipientName(c), "share", t.Share,
			"table", t.SourceSchema+"."+t.SourceTable, "policies", pe.Policies)
		return refusal(t)
	case err != nil:
		return &apiError{status: http.StatusServiceUnavailable, code: "TEMPORARILY_UNAVAILABLE", msg: err.Error()}
	}
	return nil
}

func recipientName(c *call) string {
	if c == nil {
		return ""
	}
	return c.recipient
}
