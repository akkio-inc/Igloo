package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jeroenrinzema/psql-wire/codes"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// The admin pages issue the same SQL an administrator would type in psql,
// as the signed-in administrator, so the SQL server checks and logs it as
// usual; these handlers only build it from form fields.

// writeSQLError sends a failed statement's error and SQLSTATE.
func writeSQLError(w http.ResponseWriter, err error) {
	e := toSQLError(err)
	writeJSON(w, statusFor(e), map[string]any{"error": e.Message, "code": e.Code})
}

func (c *Console) listUsers(w http.ResponseWriter, r *http.Request, req *request) {
	res, err := c.lookup(r.Context(), req.user, r, "SHOW USERS")
	if err != nil {
		writeSQLError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type userChange struct {
	// Action is create, service, role, password, owner, disable, enable,
	// unlink (from single sign-on) or drop.
	Action   string `json:"action"`
	Name     string `json:"name"`
	Password string `json:"password"`
	Role     string `json:"role"`
	// Email, for create, is how single sign-on recognises a user created
	// without a password.
	Email string `json:"email"`
	// Owner is a service account's owner, for service and owner.
	Owner string `json:"owner"`
}

func (c *Console) changeUser(w http.ResponseWriter, r *http.Request, req *request) {
	var in userChange
	if !readJSON(w, r, 16<<10, &in) {
		return
	}
	if !config.ValidName(in.Name) {
		writeError(w, http.StatusBadRequest, "a user name is lowercase letters, digits and underscores, starting with a letter")
		return
	}
	var role auth.Role
	if in.Action == "create" || in.Action == "role" || in.Action == "service" {
		var err error
		if role, err = auth.ParseRole(in.Role); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if (in.Action == "password" || in.Action == "create" && in.Email == "") && in.Password == "" {
		writeError(w, http.StatusBadRequest, "enter a password, or for a user who signs in with single sign-on, their email")
		return
	}
	if (in.Action == "owner" || in.Action == "service" && in.Owner != "") && !config.ValidName(in.Owner) {
		writeError(w, http.StatusBadRequest, "the owner must be a user name")
		return
	}
	var sql string
	switch in.Action {
	case "create":
		password := "NULL"
		if in.Password != "" {
			password = quoteLiteral(in.Password)
		}
		sql = "CREATE USER " + in.Name + " WITH PASSWORD " + password + " ROLE " + string(role)
		if in.Email != "" {
			sql += " EMAIL " + quoteLiteral(in.Email)
		}
	case "service":
		sql = "CREATE USER " + in.Name + " TYPE SERVICE ROLE " + string(role)
		if in.Owner != "" {
			sql += " OWNER " + in.Owner
		}
	case "role":
		sql = "ALTER USER " + in.Name + " ROLE " + string(role)
	case "password":
		sql = "ALTER USER " + in.Name + " PASSWORD " + quoteLiteral(in.Password)
	case "owner":
		sql = "ALTER USER " + in.Name + " OWNER " + in.Owner
	case "disable":
		sql = "ALTER USER " + in.Name + " NOLOGIN"
	case "enable":
		sql = "ALTER USER " + in.Name + " LOGIN"
	case "unlink":
		sql = "ALTER USER " + in.Name + " SSO NULL"
	case "drop":
		sql = "DROP USER " + in.Name
	default:
		writeError(w, http.StatusBadRequest, "action must be create, service, role, password, owner, disable, enable, unlink or drop")
		return
	}
	res, err := c.run(r.Context(), req.user, r, sql)
	if err != nil {
		writeSQLError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"tag": res.Tag})
}

// grantsResult is one of SHOW ROLES and SHOW GRANTS: its rows, or why it
// failed.
type grantsResult struct {
	*result
	Error string `json:"error,omitempty"`
	Code  string `json:"code,omitempty"`
}

// unsupported are the SQLSTATEs of a statement this version of Igloo does
// not have: DuckDB reads SHOW ROLES as a request to describe a table named
// roles, for instance.
var unsupported = []string{
	string(codes.FeatureNotSupported), string(codes.Syntax), string(codes.UndefinedTable),
	string(codes.UndefinedObject), string(codes.InvalidSchemaName),
}

func (c *Console) listGrants(w http.ResponseWriter, r *http.Request, req *request) {
	out := map[string]any{}
	available := false
	for key, sql := range map[string]string{"roles": "SHOW ROLES", "grants": "SHOW GRANTS"} {
		res, err := c.lookup(r.Context(), req.user, r, sql)
		if err != nil {
			e := toSQLError(err)
			out[key] = grantsResult{Error: e.Message, Code: e.Code}
			available = available || !slices.Contains(unsupported, e.Code)
			continue
		}
		out[key] = grantsResult{result: &res}
		available = true
	}
	out["available"] = available
	writeJSON(w, http.StatusOK, out)
}

// grantStatements are what the roles and grants page may run.
var grantStatements = [][]string{
	{"GRANT"}, {"REVOKE"}, {"CREATE", "ROLE"}, {"DROP", "ROLE"}, {"ALTER", "ROLE"},
}

func (c *Console) changeGrants(w http.ResponseWriter, r *http.Request, req *request) {
	var in queryRequest
	if !readJSON(w, r, 64<<10, &in) {
		return
	}
	stmts := sqllex.Split(in.SQL)
	if len(stmts) != 1 {
		writeError(w, http.StatusBadRequest, "send one statement")
		return
	}
	words := sqllex.CodeWords(stmts[0].Text)
	if !slices.ContainsFunc(grantStatements, func(prefix []string) bool {
		return len(words) >= len(prefix) && slices.Equal(words[:len(prefix)], prefix)
	}) {
		writeError(w, http.StatusBadRequest, "this page runs GRANT, REVOKE, CREATE ROLE, ALTER ROLE and DROP ROLE; use a worksheet for anything else")
		return
	}
	res, err := c.run(r.Context(), req.user, r, stmts[0].Text)
	if err != nil {
		writeSQLError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"tag": res.Tag})
}

// sessionView is a session on the compute page.
type sessionView struct {
	PID       int32      `json:"pid"`
	User      string     `json:"user"`
	Role      auth.Role  `json:"role"`
	Client    string     `json:"client"`
	Connected time.Time  `json:"connected"`
	Statement string     `json:"statement,omitempty"`
	Started   *time.Time `json:"started,omitempty"`
}

// maxStatementText caps the statement text the compute page shows.
const maxStatementText = 4 << 10

func (c *Console) compute(w http.ResponseWriter, r *http.Request, _ *request) {
	sessions := c.sql.Sessions()
	views := make([]sessionView, len(sessions))
	for i, s := range sessions {
		v := sessionView{PID: s.PID, User: s.User, Role: s.Role, Client: s.Client, Connected: s.Connected}
		if !s.Started.IsZero() {
			started := s.Started
			v.Started = &started
			v.Statement = s.Statement
			if len(v.Statement) > maxStatementText {
				v.Statement = truncate(v.Statement, maxStatementText) + "…"
			}
		}
		views[i] = v
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"replica":  c.replica,
		"now":      time.Now(),
		"status":   c.status(r.Context()),
		"sessions": views,
	})
}

type cancelRequest struct {
	PID json.Number `json:"pid"`
	// Replica is the replica the page listed the session on. Behind a
	// load balancer this request may reach another one, which must not
	// cancel a session that merely has the same id.
	Replica string `json:"replica"`
}

// parsePID reads a session id sent as a JSON number or string.
func parsePID(v json.Number) (int32, error) {
	n, err := strconv.ParseInt(string(v), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad session id %q", v)
	}
	return int32(n), nil
}

func (c *Console) cancel(w http.ResponseWriter, r *http.Request, req *request) {
	var in cancelRequest
	if !readJSON(w, r, 4<<10, &in) {
		return
	}
	pid, err := parsePID(in.PID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !strings.EqualFold(in.Replica, c.replica) {
		writeError(w, http.StatusConflict, "this request reached replica "+c.replica+", not "+in.Replica+
			", where the statement runs; refresh to see this replica's sessions")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"canceled": c.sql.CancelStatement(pid, req.user.Name)})
}
