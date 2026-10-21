package pgwire

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sharing"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Clean room statements (package cleanroom). Igloo carries them out itself:
//
//	CREATE CLEAN ROOM [IF NOT EXISTS] room [COMMENT 'text']
//	DROP CLEAN ROOM [IF EXISTS] room
//	SHOW CLEAN ROOMS
//	SHOW {COLLABORATORS | DATASETS | TEMPLATES | RUNS} IN CLEAN ROOM room
//	ALTER CLEAN ROOM room ADD COLLABORATOR name {USER user | ROLE role | RECIPIENT recipient [PROVIDER provider]}
//	ALTER CLEAN ROOM room REMOVE COLLABORATOR name
//	ALTER CLEAN ROOM room {ADD | REPLACE} DATASET name OF collaborator FROM [schema.]table
//	    JOIN ON (column, ...) [GROUP BY (column, ...)] [AGGREGATE (function(column), ...)]
//	    [THRESHOLD n] [MAX ROWS n] [RUNS PER DAY n] [NOISE epsilon]
//	ALTER CLEAN ROOM room REMOVE DATASET name
//	ALTER CLEAN ROOM room {ADD | REPLACE} TEMPLATE name [PARAMETERS (name type, ...)] RUN BY (collaborator, ...) AS query
//	ALTER CLEAN ROOM room {ADD | REPLACE} TEMPLATE name FROM BUILTIN builtin (argument => value, ...) RUN BY (collaborator, ...)
//	ALTER CLEAN ROOM room REMOVE TEMPLATE name
//	ALTER CLEAN ROOM room APPROVE TEMPLATE name VERSION n [AS collaborator]
//	ALTER CLEAN ROOM room REVOKE APPROVAL OF TEMPLATE name [AS collaborator]
//	RUN CLEAN ROOM room TEMPLATE name [(parameter => value, ...)] [AS collaborator]
//
// Who may run which is the service's to decide (see cleanroom.Service);
// Igloo tells it who asks: the user, their custom roles, and whether they
// are an admin.

type roomKind int

const (
	crCreate roomKind = iota
	crDrop
	crShowRooms
	crShowCollaborators
	crShowDatasets
	crShowTemplates
	crShowRuns
	crAddCollaborator
	crRemoveCollaborator
	crPutDataset
	crRemoveDataset
	crPutTemplate
	crRemoveTemplate
	crApprove
	crUnapprove
	crRun
)

// roomCommand is a parsed clean room statement.
type roomCommand struct {
	kind roomKind
	tag  string
	room string
	// ifExists, ifNotExists and replace are the statement's clauses.
	ifExists, ifNotExists, replace bool
	comment                        string
	// name is the collaborator, dataset or template.
	name string
	// kind and principal of a collaborator, and a partner's provider.
	collaborator, principal, provider string
	// of and source name a dataset's collaborator and source.
	of     string
	source []string
	policy cleanroom.Policy
	// params, runBy and body define a template, or builtin and args.
	params  []cleanroom.Parameter
	runBy   []string
	body    string
	builtin string
	args    map[string]string
	version int
	as      string
	values  map[string]any
}

var (
	roomColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "comment", Oid: pgtype.TextOID, Width: -1},
		{Name: "collaborators", Oid: pgtype.TextOID, Width: -1},
		{Name: "yours", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	collaboratorColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "kind", Oid: pgtype.TextOID, Width: -1},
		{Name: "principal", Oid: pgtype.TextOID, Width: -1},
		{Name: "provider", Oid: pgtype.TextOID, Width: -1},
		{Name: "yours", Oid: pgtype.BoolOID, Width: 1},
		{Name: "added_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "added_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	datasetColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "collaborator", Oid: pgtype.TextOID, Width: -1},
		{Name: "source", Oid: pgtype.TextOID, Width: -1},
		{Name: "kind", Oid: pgtype.TextOID, Width: -1},
		{Name: "join_on", Oid: pgtype.TextOID, Width: -1},
		{Name: "group_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "aggregates", Oid: pgtype.TextOID, Width: -1},
		{Name: "bounds", Oid: pgtype.TextOID, Width: -1},
		{Name: "threshold", Oid: pgtype.Int8OID, Width: 8},
		{Name: "max_rows", Oid: pgtype.Int8OID, Width: 8},
		{Name: "runs_per_day", Oid: pgtype.Int8OID, Width: 8},
		{Name: "noise", Oid: pgtype.Float8OID, Width: 8},
		{Name: "added_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "updated_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	templateColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "version", Oid: pgtype.Int8OID, Width: 8},
		{Name: "builtin", Oid: pgtype.TextOID, Width: -1},
		{Name: "parameters", Oid: pgtype.TextOID, Width: -1},
		{Name: "run_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "needs", Oid: pgtype.TextOID, Width: -1},
		{Name: "approved", Oid: pgtype.TextOID, Width: -1},
		{Name: "runnable", Oid: pgtype.BoolOID, Width: 1},
		{Name: "problem", Oid: pgtype.TextOID, Width: -1},
		{Name: "definition", Oid: pgtype.TextOID, Width: -1},
		{Name: "digest", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	runColumns = wire.Columns{
		{Name: "id", Oid: pgtype.Int8OID, Width: 8},
		{Name: "template", Oid: pgtype.TextOID, Width: -1},
		{Name: "version", Oid: pgtype.Int8OID, Width: 8},
		{Name: "requester", Oid: pgtype.TextOID, Width: -1},
		{Name: "requested_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "parameters", Oid: pgtype.TextOID, Width: -1},
		{Name: "approvals", Oid: pgtype.TextOID, Width: -1},
		{Name: "status", Oid: pgtype.TextOID, Width: -1},
		{Name: "error", Oid: pgtype.TextOID, Width: -1},
		{Name: "row_count", Oid: pgtype.Int8OID, Width: 8},
		{Name: "truncated", Oid: pgtype.BoolOID, Width: 1},
		{Name: "noise", Oid: pgtype.Float8OID, Width: 8},
		{Name: "result_table", Oid: pgtype.TextOID, Width: -1},
		{Name: "started_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "finished_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	versionColumns = wire.Columns{
		{Name: "template", Oid: pgtype.TextOID, Width: -1},
		{Name: "version", Oid: pgtype.Int8OID, Width: 8},
	}
	runOutcomeColumns = wire.Columns{
		{Name: "run_id", Oid: pgtype.Int8OID, Width: 8},
		{Name: "status", Oid: pgtype.TextOID, Width: -1},
		{Name: "row_count", Oid: pgtype.Int8OID, Width: 8},
		{Name: "truncated", Oid: pgtype.BoolOID, Width: 1},
		{Name: "result_table", Oid: pgtype.TextOID, Width: -1},
	}
)

func (c *roomCommand) columns() wire.Columns {
	return map[roomKind]wire.Columns{
		crShowRooms: roomColumns, crShowCollaborators: collaboratorColumns, crShowDatasets: datasetColumns,
		crShowTemplates: templateColumns, crShowRuns: runColumns, crPutTemplate: versionColumns, crRun: runOutcomeColumns,
	}[c.kind]
}

// parseRoomCommand parses a clean room statement. ok is false for any
// other statement.
func parseRoomCommand(query string) (c *roomCommand, ok bool, err error) {
	sc := &scanner{s: query}
	c = &roomCommand{}
	syntax := func(format string, args ...any) (*roomCommand, bool, error) {
		return nil, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	ident := func(into *string) bool {
		n, ok := sc.name()
		if !ok || len(n) != 1 {
			return false
		}
		*into = n[0]
		return true
	}
	inRoom := func(kind roomKind) (*roomCommand, bool, error) {
		c.kind, c.tag = kind, "SHOW"
		if !sc.keywords("IN", "CLEAN", "ROOM") || !ident(&c.room) {
			return syntax("expected IN CLEAN ROOM room")
		}
		return nil, false, nil
	}
	switch {
	case sc.keywords("CREATE", "CLEAN", "ROOM"):
		c.kind, c.tag = crCreate, "CREATE CLEAN ROOM"
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
		if !ident(&c.room) {
			return syntax("CREATE CLEAN ROOM needs a name")
		}
		if sc.keywords("COMMENT") {
			v, ok := sc.literal()
			if !ok || v == nil {
				return syntax("COMMENT needs a quoted string")
			}
			c.comment = *v
		}
	case sc.keywords("DROP", "CLEAN", "ROOM"):
		c.kind, c.tag = crDrop, "DROP CLEAN ROOM"
		c.ifExists = sc.keywords("IF", "EXISTS")
		if !ident(&c.room) {
			return syntax("DROP CLEAN ROOM needs a name")
		}
	case sc.keywords("SHOW", "CLEAN", "ROOMS"):
		c.kind, c.tag = crShowRooms, "SHOW"
	case sc.keywords("SHOW", "COLLABORATORS"):
		if _, ok, err := inRoom(crShowCollaborators); ok {
			return nil, true, err
		}
	case sc.keywords("SHOW", "DATASETS"):
		if _, ok, err := inRoom(crShowDatasets); ok {
			return nil, true, err
		}
	case sc.keywords("SHOW", "TEMPLATES"):
		if _, ok, err := inRoom(crShowTemplates); ok {
			return nil, true, err
		}
	case sc.keywords("SHOW", "RUNS"):
		if _, ok, err := inRoom(crShowRuns); ok {
			return nil, true, err
		}
	case sc.keywords("RUN", "CLEAN", "ROOM"):
		c.kind, c.tag = crRun, "RUN CLEAN ROOM"
		if !ident(&c.room) || !sc.keywords("TEMPLATE") || !ident(&c.name) {
			return syntax("expected RUN CLEAN ROOM room TEMPLATE name [(parameter => value, ...)] [AS collaborator]")
		}
		c.values = map[string]any{}
		if sc.punct('(') {
			for {
				var name string
				if !ident(&name) || !fatArrow(sc) {
					return syntax("expected parameter => value")
				}
				v, ok := runValue(sc)
				if !ok {
					return syntax("parameter %s takes a string, a number, TRUE, FALSE or NULL", name)
				}
				if _, dup := c.values[name]; dup {
					return syntax("parameter %s is given twice", name)
				}
				c.values[name] = v
				if !sc.punct(',') {
					break
				}
			}
			if !sc.punct(')') {
				return syntax("expected ) after the parameters")
			}
		}
		if sc.keywords("AS") && !ident(&c.as) {
			return syntax("AS needs a collaborator")
		}
	case sc.keywords("ALTER", "CLEAN", "ROOM"):
		c.tag = "ALTER CLEAN ROOM"
		if !ident(&c.room) {
			return syntax("ALTER CLEAN ROOM needs a name")
		}
		if err := parseRoomAlter(sc, c, ident); err != nil {
			return nil, true, err
		}
	default:
		return nil, false, nil
	}
	if !sc.done() {
		return syntax("unexpected %q at the end of %s", firstWords(sc.rest()), c.tag)
	}
	return c, true, nil
}

func firstWords(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

// parseRoomAlter parses what follows ALTER CLEAN ROOM room.
func parseRoomAlter(sc *scanner, c *roomCommand, ident func(*string) bool) error {
	syntax := func(format string, args ...any) error {
		return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	switch {
	case sc.keywords("REPLACE", "DATASET"):
		c.replace = true
		fallthrough
	case sc.keywords("ADD", "DATASET"):
		c.kind = crPutDataset
		if !ident(&c.name) || !sc.keywords("OF") || !ident(&c.of) || !sc.keywords("FROM") {
			return syntax("expected ADD DATASET name OF collaborator FROM table JOIN ON (column, ...) ...")
		}
		var ok bool
		if c.source, ok = sc.name(); !ok || len(c.source) > 3 {
			return syntax("expected the dataset's table after FROM")
		}
		return parsePolicy(sc, &c.policy)
	case sc.keywords("REPLACE", "TEMPLATE"):
		c.replace = true
		fallthrough
	case sc.keywords("ADD", "TEMPLATE"):
		c.kind = crPutTemplate
		if !ident(&c.name) {
			return syntax("ADD TEMPLATE needs a name")
		}
		return parseTemplate(sc, c, ident)
	case sc.keywords("ADD", "COLLABORATOR"):
		c.kind = crAddCollaborator
		if !ident(&c.name) {
			return syntax("ADD COLLABORATOR needs a name")
		}
		switch {
		case sc.keywords("USER"):
			c.collaborator = cleanroom.KindUser
		case sc.keywords("ROLE"):
			c.collaborator = cleanroom.KindRole
		case sc.keywords("RECIPIENT"):
			c.collaborator = cleanroom.KindPartner
		default:
			return syntax("expected ADD COLLABORATOR name USER user, ROLE role or RECIPIENT recipient [PROVIDER provider]")
		}
		if !ident(&c.principal) {
			return syntax("expected the %s's name", strings.ToLower(c.collaborator))
		}
		if c.collaborator == cleanroom.KindPartner && sc.keywords("PROVIDER") && !ident(&c.provider) {
			return syntax("PROVIDER needs a provider's name")
		}
	case sc.keywords("REMOVE", "COLLABORATOR"):
		c.kind = crRemoveCollaborator
		if !ident(&c.name) {
			return syntax("REMOVE COLLABORATOR needs a name")
		}
	case sc.keywords("REMOVE", "DATASET"):
		c.kind = crRemoveDataset
		if !ident(&c.name) {
			return syntax("REMOVE DATASET needs a name")
		}
	case sc.keywords("REMOVE", "TEMPLATE"):
		c.kind = crRemoveTemplate
		if !ident(&c.name) {
			return syntax("REMOVE TEMPLATE needs a name")
		}
	case sc.keywords("APPROVE", "TEMPLATE"):
		c.kind = crApprove
		if !ident(&c.name) || !sc.keywords("VERSION") {
			return syntax("expected APPROVE TEMPLATE name VERSION n [AS collaborator]")
		}
		n, ok := number(sc)
		v, err := strconv.Atoi(n)
		if !ok || err != nil || v < 1 {
			return syntax("VERSION takes the version you approve, such as VERSION 1")
		}
		c.version = v
		if sc.keywords("AS") && !ident(&c.as) {
			return syntax("AS needs a collaborator")
		}
	case sc.keywords("REVOKE", "APPROVAL", "OF", "TEMPLATE"):
		c.kind = crUnapprove
		if !ident(&c.name) {
			return syntax("REVOKE APPROVAL OF TEMPLATE needs a name")
		}
		if sc.keywords("AS") && !ident(&c.as) {
			return syntax("AS needs a collaborator")
		}
	default:
		return syntax("expected ADD, REPLACE or REMOVE COLLABORATOR, DATASET or TEMPLATE, APPROVE TEMPLATE or REVOKE APPROVAL after ALTER CLEAN ROOM %s", c.room)
	}
	return nil
}

// parsePolicy parses a dataset's policy clauses.
func parsePolicy(sc *scanner, p *cleanroom.Policy) error {
	syntax := func(format string, args ...any) error {
		return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	seen := map[string]bool{}
	integer := func(what string, into *int) error {
		n, ok := number(sc)
		v, err := strconv.Atoi(n)
		if !ok || err != nil || v < 1 {
			return syntax("%s takes a positive whole number", what)
		}
		*into = v
		return nil
	}
	for {
		var clause string
		var err error
		switch {
		case sc.keywords("JOIN", "ON"):
			clause = "JOIN ON"
			p.Join, err = nameList(sc, clause)
		case sc.keywords("GROUP", "BY"):
			clause = "GROUP BY"
			p.Group, err = nameList(sc, clause)
		case sc.keywords("AGGREGATE"):
			clause = "AGGREGATE"
			if !sc.punct('(') {
				return syntax("expected AGGREGATE (function(column), ...)")
			}
			for {
				fn := strings.ToLower(sc.word())
				sc.i += len(fn)
				col, ok := []string(nil), fn != ""
				if ok && sc.punct('(') {
					col, ok = sc.name()
				}
				if !ok || len(col) != 1 || !sc.punct(')') {
					return syntax("expected AGGREGATE (function(column), ...), such as AGGREGATE (sum(spend), avg(spend))")
				}
				p.Aggregates = append(p.Aggregates, cleanroom.Aggregate{Function: fn, Column: col[0]})
				if !sc.punct(',') {
					break
				}
			}
			if !sc.punct(')') {
				return syntax("expected ) after AGGREGATE's aggregates")
			}
		case sc.keywords("BOUNDS"):
			clause = "BOUNDS"
			usage := syntax("expected BOUNDS (column BETWEEN lo AND hi, ...), such as BOUNDS (spend BETWEEN 0 AND 1000)")
			if !sc.punct('(') {
				return usage
			}
			p.Bounds = map[string]cleanroom.Bound{}
			for {
				col, ok := sc.name()
				if !ok || len(col) != 1 || !sc.keywords("BETWEEN") {
					return usage
				}
				lo, ok := number(sc)
				if !ok || !sc.keywords("AND") {
					return usage
				}
				hi, ok := number(sc)
				if !ok {
					return usage
				}
				var b cleanroom.Bound
				var lerr, herr error
				b.Lo, lerr = strconv.ParseFloat(lo, 64)
				b.Hi, herr = strconv.ParseFloat(hi, 64)
				if lerr != nil || herr != nil {
					return usage
				}
				if _, dup := p.Bounds[strings.ToLower(col[0])]; dup {
					return syntax("column %s has BOUNDS twice", col[0])
				}
				p.Bounds[strings.ToLower(col[0])] = b
				if !sc.punct(',') {
					break
				}
			}
			if !sc.punct(')') {
				return usage
			}
		case sc.keywords("THRESHOLD"):
			clause = "THRESHOLD"
			err = integer(clause, &p.Threshold)
		case sc.keywords("MAX", "ROWS"):
			clause = "MAX ROWS"
			err = integer(clause, &p.MaxRows)
		case sc.keywords("RUNS", "PER", "DAY"):
			clause = "RUNS PER DAY"
			err = integer(clause, &p.RunsPerDay)
		case sc.keywords("NOISE"):
			clause = "NOISE"
			n, ok := number(sc)
			v, perr := strconv.ParseFloat(n, 64)
			if !ok || perr != nil || v <= 0 {
				return syntax("NOISE takes an epsilon, such as NOISE 1")
			}
			p.Noise = v
		default:
			return nil
		}
		if err != nil {
			return err
		}
		if seen[clause] {
			return syntax("%s is given twice", clause)
		}
		seen[clause] = true
	}
}

// parseTemplate parses a template's clauses: PARAMETERS and RUN BY, then
// AS query or FROM BUILTIN.
func parseTemplate(sc *scanner, c *roomCommand, ident func(*string) bool) error {
	syntax := func(format string, args ...any) error {
		return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	for {
		switch {
		case sc.keywords("PARAMETERS"):
			if c.params != nil || !sc.punct('(') {
				return syntax("expected PARAMETERS (name type, ...) once")
			}
			c.params = []cleanroom.Parameter{}
			for {
				var p cleanroom.Parameter
				if !ident(&p.Name) {
					return syntax("expected PARAMETERS (name type, ...)")
				}
				if p.Type = typeText(sc); p.Type == "" {
					return syntax("parameter %s needs a type", p.Name)
				}
				c.params = append(c.params, p)
				if !sc.punct(',') {
					break
				}
			}
			if !sc.punct(')') {
				return syntax("expected ) after the parameters")
			}
		case sc.keywords("RUN", "BY"):
			if c.runBy != nil {
				return syntax("RUN BY is given twice")
			}
			var err error
			if c.runBy, err = nameList(sc, "RUN BY"); err != nil {
				return err
			}
		case sc.keywords("FROM", "BUILTIN"):
			if c.builtin != "" || !ident(&c.builtin) {
				return syntax("expected FROM BUILTIN name (argument => value, ...)")
			}
			c.args = map[string]string{}
			if sc.punct('(') {
				for {
					var name string
					if !ident(&name) || !fatArrow(sc) {
						return syntax("expected argument => value")
					}
					v, ok := builtinValue(sc)
					if !ok {
						return syntax("%s takes dataset.column, a string or a number", name)
					}
					if _, dup := c.args[name]; dup {
						return syntax("%s is given twice", name)
					}
					c.args[name] = v
					if !sc.punct(',') {
						break
					}
				}
				if !sc.punct(')') {
					return syntax("expected ) after the arguments")
				}
			}
		case sc.keywords("AS"):
			if c.builtin != "" {
				return syntax("a template is FROM BUILTIN or AS query, not both")
			}
			c.body = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
			sc.i = len(sc.s)
			if c.body == "" {
				return syntax("AS needs the template's query")
			}
			return nil
		default:
			if c.builtin == "" {
				return syntax("expected [PARAMETERS (...)] RUN BY (collaborator, ...) AS query, or FROM BUILTIN name (...) RUN BY (...)")
			}
			if c.params != nil {
				return syntax("built-in templates declare their own parameters")
			}
			return nil
		}
	}
}

// nameList reads (name, ...).
func nameList(sc *scanner, what string) ([]string, error) {
	if !sc.punct('(') {
		return nil, pgerror.WithCode(fmt.Errorf("expected %s (name, ...)", what), codes.Syntax)
	}
	var out []string
	for {
		n, ok := sc.name()
		if !ok || len(n) != 1 {
			return nil, pgerror.WithCode(fmt.Errorf("expected %s (name, ...)", what), codes.Syntax)
		}
		out = append(out, n[0])
		if !sc.punct(',') {
			break
		}
	}
	if !sc.punct(')') {
		return nil, pgerror.WithCode(fmt.Errorf("expected ) after %s's names", what), codes.Syntax)
	}
	return out, nil
}

// fatArrow consumes =>.
func fatArrow(sc *scanner) bool {
	save := sc.i
	if sc.punct('=') && sc.i < len(sc.s) && sc.s[sc.i] == '>' {
		sc.i++
		return true
	}
	sc.i = save
	return false
}

// number reads a number, such as 50, -2 or 0.5, as written.
func number(sc *scanner) (string, bool) {
	sc.skip()
	j := sc.i
	if j < len(sc.s) && (sc.s[j] == '-' || sc.s[j] == '+') {
		j++
	}
	digits := j
	for j < len(sc.s) && (sc.s[j] >= '0' && sc.s[j] <= '9' || sc.s[j] == '.') {
		j++
	}
	if j == digits || j < len(sc.s) && sqllex.IdentByte(sc.s[j]) {
		return "", false
	}
	n := sc.s[sc.i:j]
	sc.i = j
	return n, true
}

// typeText reads a parameter's type, up to the next comma or closing
// parenthesis outside parentheses.
func typeText(sc *scanner) string {
	sc.skip()
	start, depth := sc.i, 0
	for ; sc.i < len(sc.s); sc.i++ {
		switch sc.s[sc.i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return strings.TrimSpace(sc.s[start:sc.i])
			}
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(sc.s[start:sc.i])
			}
		}
	}
	return strings.TrimSpace(sc.s[start:sc.i])
}

// runValue reads a parameter's value: a string, a number, TRUE, FALSE or
// NULL.
func runValue(sc *scanner) (any, bool) {
	switch {
	case sc.keywords("TRUE"):
		return true, true
	case sc.keywords("FALSE"):
		return false, true
	}
	if v, ok := sc.literal(); ok {
		if v == nil {
			return nil, true
		}
		return *v, true
	}
	if n, ok := number(sc); ok {
		return json.Number(n), true
	}
	return nil, false
}

// builtinValue reads a built-in's argument: dataset.column, a string, a
// number, TRUE or FALSE.
func builtinValue(sc *scanner) (string, bool) {
	switch {
	case sc.keywords("TRUE"):
		return "true", true
	case sc.keywords("FALSE"):
		return "false", true
	}
	if v, ok := sc.literal(); ok {
		return *v, v != nil
	}
	if n, ok := number(sc); ok {
		return n, true
	}
	if n, ok := sc.name(); ok && len(n) == 2 {
		return n[0] + "." + n[1], true
	}
	return "", false
}

// roomStatement prepares a clean room statement for a client. ok is false
// for any other statement.
func (s *Server) roomStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	parts := sqllex.Split(q.Query)
	if len(parts) == 0 {
		return nil, false, nil
	}
	c, ok, err := parseRoomCommand(parts[0].Text)
	if !ok {
		return nil, false, nil
	}
	// Several in one query run one after the other, as user management
	// statements do; never with other SQL.
	cmds := []*roomCommand{c}
	for _, p := range parts[1:] {
		if err != nil {
			break
		}
		var more *roomCommand
		if more, ok, err = parseRoomCommand(p.Text); !ok {
			err = pgerror.WithCode(errors.New("send clean room statements separately from other SQL"), codes.FeatureNotSupported)
		}
		cmds = append(cmds, more)
	}
	if err == nil && len(cmds) > 1 && !q.SimpleQuery {
		err = pgerror.WithCode(errors.New("cannot prepare several statements at once"), codes.Syntax)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var stmts wire.PreparedStatements
	for _, c := range cmds {
		stmts = append(stmts, wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			start := time.Now()
			var err error
			if s.rooms == nil {
				err = pgerror.WithCode(cleanroom.ErrUnavailable, codes.FeatureNotSupported)
			} else if c.kind == crRun {
				err = s.inNoTransaction(sess, c.tag)
				if err == nil {
					err = sess.runCancelable(ctx, q.Query, func(ctx context.Context) error { return s.runRoomCommand(ctx, sess, c, w) })
				}
			} else {
				err = s.runRoomCommand(ctx, sess, c, w)
			}
			s.metrics.query(start, err)
			return roomError(err)
		}), wire.WithColumns(c.columns())))
	}
	return stmts, true, nil
}

// roomError gives the service's errors PostgreSQL codes.
func roomError(err error) error {
	if err == nil || pgerror.GetCode(err) != codes.Uncategorized {
		return err
	}
	var refused *cleanroom.RefusedError
	switch {
	case errors.Is(err, cleanroom.ErrNotFound):
		return pgerror.WithCode(err, codes.UndefinedObject)
	case errors.Is(err, cleanroom.ErrExists):
		return pgerror.WithCode(err, codes.DuplicateObject)
	case errors.Is(err, cleanroom.ErrInUse):
		return pgerror.WithCode(err, codes.DependentObjectsStillExist)
	case errors.Is(err, cleanroom.ErrUnavailable):
		return pgerror.WithCode(err, codes.FeatureNotSupported)
	case cleanroom.IsDenied(err), errors.As(err, &refused):
		return pgerror.WithCode(fmt.Errorf("permission denied: %w", err), codes.InsufficientPrivilege)
	case cleanroom.IsBudget(err):
		return pgerror.WithCode(err, codes.ConfigurationLimitExceeded)
	case cleanroom.IsInvalid(err):
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	return pgError(err)
}

// actor is who runs a statement, as the clean room service sees them.
func (s *Server) actor(ctx context.Context, sess *session) (cleanroom.Actor, error) {
	me := sess.currentUser()
	privs, err := s.privileges(ctx, sess, me)
	if err != nil {
		return cleanroom.Actor{}, denied("could not load roles: %v", err)
	}
	return cleanroom.Actor{User: me.Name, Roles: privs.Roles, Admin: me.Role == auth.RoleAdmin}, nil
}

// runRoomCommand carries out a clean room statement for sess.
func (s *Server) runRoomCommand(ctx context.Context, sess *session, c *roomCommand, w wire.DataWriter) error {
	svc := s.rooms
	a, err := s.actor(ctx, sess)
	if err != nil {
		return err
	}
	// room names the room in a bare "does not exist".
	room := func(err error) error {
		if err == cleanroom.ErrNotFound {
			return fmt.Errorf("clean room %s %w", c.room, err)
		}
		return err
	}
	done := func(err error) error {
		if err != nil {
			return room(err)
		}
		s.log.Info("clean room changed", "statement", c.tag, "room", c.room, "name", c.name, "by", a.Name())
		return w.Complete(c.tag)
	}
	switch c.kind {
	case crCreate:
		err := svc.CreateRoom(ctx, a, c.room, c.comment)
		if errors.Is(err, cleanroom.ErrExists) && c.ifNotExists {
			err = nil
		}
		if errors.Is(err, cleanroom.ErrExists) {
			err = fmt.Errorf("clean room %s %w", c.room, err)
		}
		return done(err)
	case crDrop:
		err := svc.DropRoom(ctx, a, c.room)
		if errors.Is(err, cleanroom.ErrNotFound) && c.ifExists {
			err = nil
		}
		return done(err)
	case crShowRooms:
		rooms, err := svc.Rooms(ctx, a)
		if err != nil {
			return err
		}
		for _, r := range rooms {
			if err := w.Row([]any{r.Name, r.Comment, strings.Join(r.Collaborators, ", "), strings.Join(r.Yours, ", "), r.CreatedBy, r.CreatedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case crShowCollaborators, crShowDatasets, crShowTemplates:
		v, err := svc.Describe(ctx, a, c.room)
		if err != nil {
			return room(err)
		}
		return showRoom(c.kind, v, w)
	case crShowRuns:
		runs, err := svc.Runs(ctx, a, c.room, 1000)
		if err != nil {
			return room(err)
		}
		for _, r := range runs {
			params, _ := json.Marshal(r.Parameters)
			var finished any
			if r.Finished != nil {
				finished = *r.Finished
			}
			row := []any{r.ID, r.Template, int64(r.Version), r.Requester, r.RequestedBy, string(params), strings.Join(r.Approvals, ", "),
				r.Status, nullText(r.Error), r.Rows, r.Truncated, r.Noise, nullText(r.ResultTable), r.Started, finished}
			if err := w.Row(row); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case crAddCollaborator:
		if err := s.checkPrincipal(ctx, c); err != nil {
			return err
		}
		err := svc.AddCollaborator(ctx, a, cleanroom.Collaborator{Room: c.room, Name: c.name, Kind: c.collaborator,
			Principal: c.principal, Provider: c.provider})
		if errors.Is(err, cleanroom.ErrExists) {
			err = fmt.Errorf("clean room %s has a collaborator %s, or one for %s %s: %w", c.room, c.name, c.collaborator, c.principal, err)
		}
		return done(err)
	case crRemoveCollaborator:
		err := svc.RemoveCollaborator(ctx, a, c.room, c.name)
		if errors.Is(err, cleanroom.ErrNotFound) {
			err = fmt.Errorf("collaborator %s of clean room %s %w", c.name, c.room, err)
		}
		return done(err)
	case crPutDataset:
		src, err := s.roomSource(ctx, sess, c.source)
		if err != nil {
			return err
		}
		if err := s.ownsSource(ctx, sess, src); err != nil {
			return err
		}
		err = svc.AddDataset(ctx, a, cleanroom.Dataset{Room: c.room, Name: c.name, Collaborator: c.of, Policy: c.policy}, src, c.replace)
		switch {
		case errors.Is(err, cleanroom.ErrExists):
			err = fmt.Errorf("clean room %s has a dataset %s, or one of %s: %w; REPLACE DATASET changes one", c.room, c.name, src.Schema+"."+src.Table, err)
		case errors.Is(err, cleanroom.ErrNotFound) && c.replace:
			err = fmt.Errorf("dataset %s of collaborator %s in clean room %s %w", c.name, c.of, c.room, err)
		}
		if err == nil {
			s.ddlGen.Add(1)
		}
		return done(err)
	case crRemoveDataset:
		err := svc.RemoveDataset(ctx, a, c.room, c.name)
		if errors.Is(err, cleanroom.ErrNotFound) {
			err = fmt.Errorf("dataset %s of clean room %s %w", c.name, c.room, err)
		}
		if err == nil {
			s.ddlGen.Add(1)
		}
		return done(err)
	case crPutTemplate:
		t := cleanroom.Template{Room: c.room, Name: c.name, SQL: c.body, Parameters: c.params, RunBy: c.runBy}
		var version int
		var err error
		if c.builtin != "" {
			version, err = svc.AddBuiltin(ctx, a, t, c.builtin, c.args, c.replace)
		} else {
			version, err = svc.AddTemplate(ctx, a, t, c.replace)
		}
		switch {
		case errors.Is(err, cleanroom.ErrExists):
			err = fmt.Errorf("clean room %s has a template %s %w; REPLACE TEMPLATE makes a new version", c.room, c.name, err)
		case errors.Is(err, cleanroom.ErrNotFound) && c.replace:
			err = fmt.Errorf("template %s of clean room %s %w", c.name, c.room, err)
		}
		if err != nil {
			return room(err)
		}
		s.log.Info("clean room template added", "room", c.room, "template", c.name, "version", version, "by", a.Name())
		if err := w.Row([]any{c.name, int64(version)}); err != nil {
			return err
		}
		return w.Complete(c.tag)
	case crRemoveTemplate:
		err := svc.RemoveTemplate(ctx, a, c.room, c.name)
		if errors.Is(err, cleanroom.ErrNotFound) {
			err = fmt.Errorf("template %s of clean room %s %w", c.name, c.room, err)
		}
		return done(err)
	case crApprove:
		_, err := svc.Approve(ctx, a, c.room, c.name, c.version, c.as)
		if errors.Is(err, cleanroom.ErrNotFound) {
			err = fmt.Errorf("template %s of clean room %s %w", c.name, c.room, err)
		}
		return done(err)
	case crUnapprove:
		_, err := svc.Unapprove(ctx, a, c.room, c.name, c.as)
		return done(err)
	case crRun:
		run, err := svc.Run(ctx, a, c.room, c.name, c.values, c.as)
		if err != nil {
			return room(err)
		}
		if err := w.Row([]any{run.ID, run.Status, run.Rows, run.Truncated, nullText(run.ResultTable)}); err != nil {
			return err
		}
		return w.Complete(c.tag)
	}
	return pgerror.WithCode(errors.New("unsupported clean room statement"), codes.FeatureNotSupported)
}

func showRoom(kind roomKind, v *cleanroom.RoomView, w wire.DataWriter) error {
	var rows [][]any
	switch kind {
	case crShowCollaborators:
		for _, c := range v.Collaborators {
			rows = append(rows, []any{c.Name, c.Kind, c.Principal, nullText(c.Provider), slices.Contains(v.Yours, c.Name), c.AddedBy, c.AddedAt})
		}
	case crShowDatasets:
		for _, d := range v.Datasets {
			aggs := make([]string, len(d.Policy.Aggregates))
			for i, a := range d.Policy.Aggregates {
				aggs[i] = a.String()
			}
			var maxRows, noise any
			if d.Policy.MaxRows > 0 {
				maxRows = int64(d.Policy.MaxRows)
			}
			if d.Policy.Noise > 0 {
				noise = d.Policy.Noise
			}
			rows = append(rows, []any{d.Name, d.Collaborator, d.Source(), d.Kind, strings.Join(d.Policy.Join, ", "),
				nullText(strings.Join(d.Policy.Group, ", ")), nullText(strings.Join(aggs, ", ")), nullText(boundsText(d.Policy.Bounds)), int64(d.Policy.Threshold), maxRows,
				int64(d.Policy.RunsPerDay), noise, d.AddedBy, d.UpdatedAt})
		}
	case crShowTemplates:
		for _, t := range v.Templates {
			params := make([]string, len(t.Parameters))
			for i, p := range t.Parameters {
				params[i] = p.Name + " " + p.Type
			}
			rows = append(rows, []any{t.Name, int64(t.Version), nullText(t.Builtin), nullText(strings.Join(params, ", ")),
				strings.Join(t.RunBy, ", "), strings.Join(t.Needs, ", "), strings.Join(t.Approved, ", "), t.Runnable(),
				nullText(t.Problem), t.SQL, t.Approves, t.CreatedBy, t.CreatedAt})
		}
	}
	for _, r := range rows {
		if err := w.Row(r); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// checkPrincipal checks that a collaborator's user, custom role, or
// recipient and provider exist.
func (s *Server) checkPrincipal(ctx context.Context, c *roomCommand) error {
	missing := func(what string) error {
		return pgerror.WithCode(fmt.Errorf("%s %s does not exist", what, c.principal), codes.UndefinedObject)
	}
	switch c.collaborator {
	case cleanroom.KindUser:
		if _, err := s.users.Get(ctx, c.principal); errors.Is(err, auth.ErrNotFound) {
			return missing("user")
		} else if err != nil {
			return err
		}
	case cleanroom.KindRole:
		if _, err := auth.ParseRole(c.principal); err == nil {
			return pgerror.WithCode(fmt.Errorf("%s is a built-in role, which every %[1]s has; a collaborator is a custom role (CREATE ROLE) or a user", c.principal),
				codes.InvalidParameterValue)
		}
		roles, err := s.grants.Roles(ctx)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(roles, func(r auth.CustomRole) bool { return r.Name == c.principal }) {
			return missing("role")
		}
	case cleanroom.KindPartner:
		if s.partners == nil {
			return pgerror.WithCode(errors.New("partners are Delta Sharing recipients, and Delta Sharing is not set up"), codes.FeatureNotSupported)
		}
		ok, err := s.partners.RecipientExists(ctx, c.principal)
		if err != nil {
			return err
		}
		if !ok {
			return missing("recipient")
		}
		if c.provider != "" {
			if ok, err = s.partners.ProviderExists(ctx, c.provider); err != nil {
				return err
			}
			if !ok {
				return pgerror.WithCode(fmt.Errorf("provider %s does not exist", c.provider), codes.UndefinedObject)
			}
		}
	}
	return nil
}

// partnerDirectory says which Delta Sharing recipients and providers exist.
type partnerDirectory interface {
	RecipientExists(ctx context.Context, name string) (bool, error)
	ProviderExists(ctx context.Context, name string) (bool, error)
}

// sharingPartners are Delta Sharing's recipients and providers.
type sharingPartners struct{ svc *sharing.Service }

func (p sharingPartners) RecipientExists(ctx context.Context, name string) (bool, error) {
	recipients, err := p.svc.Recipients(ctx)
	return slices.ContainsFunc(recipients, func(r sharing.Recipient) bool { return r.Name == name }), err
}

func (p sharingPartners) ProviderExists(ctx context.Context, name string) (bool, error) {
	providers, err := p.svc.Consumer().Providers(ctx)
	return slices.ContainsFunc(providers, func(r sharing.Provider) bool { return r.Name == name }), err
}

// roomSource resolves a dataset's source for the user adding it: what it
// is, its columns and the tables it reads, which they must be able to read
// with no masking or row access policies applying.
// sourceIdentity is a hash of what a dataset's source is (see
// cleanroom.Dataset.Identity): its Iceberg table UUID, or an external
// table's provider, and its columns and their types, as sess sees them.
func (s *Server) sourceIdentity(ctx context.Context, sess *session, kind, schema, table string) (string, error) {
	db, origin := s.catalogName(), ""
	switch {
	case kind == cleanroom.SourceExternal:
		db, origin = s.engine.LocalDatabase(), "provider "+s.external.ProviderOf(schema, table)
	case s.clones != nil:
		uuid, err := s.clones.TableUUID(ctx, identOf(auth.Object{Schema: schema, Table: table}))
		if err != nil {
			return "", err
		}
		origin = "table " + uuid
	}
	// DESCRIBE lists the catalog (see holdCatalog).
	release := s.holdCatalog(sess)
	cols, err := describeTable(ctx, sess, quoteName([]string{db, schema, table}))
	release()
	if err != nil {
		return "", pgError(err)
	}
	parts := []string{kind, strings.ToLower(schema) + "." + strings.ToLower(table), origin}
	for _, c := range cols {
		parts = append(parts, c.name+" "+c.typ)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

// boundsText writes a policy's bounds as BOUNDS takes them, sorted.
func boundsText(bounds map[string]cleanroom.Bound) string {
	var out []string
	for column, b := range bounds {
		out = append(out, fmt.Sprintf("%s BETWEEN %g AND %g", column, b.Lo, b.Hi))
	}
	slices.Sort(out)
	return strings.Join(out, ", ")
}

// ownsSource refuses a dataset's source to anyone but its owner in the
// catalog, a member of the role that owns it, or an admin: a room keeps
// its dataset from everyone else, which reading the table, as the reader
// role does every table, must not be enough to decide.
func (s *Server) ownsSource(ctx context.Context, sess *session, src cleanroom.Source) error {
	me := sess.currentUser()
	if me.Role == auth.RoleAdmin {
		return nil
	}
	table := auth.Object{Schema: src.Schema, Table: src.Table}
	owners, err := s.owners.ObjectOwners(ctx)
	if err != nil {
		return denied("could not read the owners of the catalog's objects: %v", err)
	}
	privs, err := s.privileges(ctx, sess, me)
	if err != nil {
		return denied("could not load roles: %v", err)
	}
	w, ok := auth.OwnerOf(owners, table)
	switch {
	case ok && w.Owns(me.Name, privs.Roles):
		return nil
	case !ok:
		return denied("%s has no owner, so only an admin may put it in a clean room, which keeps it from everyone else; "+
			"or an admin gives it one (ALTER TABLE %[1]s OWNER TO ...)", table)
	case w.OwnerType == auth.GranteeRole:
		return denied("only members of role %s, which owns %s, or an admin may put it in a clean room, which keeps it from everyone else",
			w.Owner, table)
	}
	return denied("only %s, who owns %s, or an admin may put it in a clean room, which keeps it from everyone else", w.Owner, table)
}

func (s *Server) roomSource(ctx context.Context, sess *session, name []string) (cleanroom.Source, error) {
	catalogName := s.catalogName()
	if catalogName == "" {
		return cleanroom.Source{}, pgerror.WithCode(cleanroom.ErrUnavailable, codes.FeatureNotSupported)
	}
	me := sess.currentUser()
	if schema, table, ok := s.externalNamed(ctx, sess, name); ok {
		// A partner's data: Igloo's own view over files its proxy serves,
		// which policies cannot attach to. Admins add partners' datasets,
		// which no local user may read once they are in a room; anyone
		// else must read it as they would.
		qualified := quoteName([]string{s.engine.LocalDatabase(), schema, table})
		if me.Role != auth.RoleAdmin {
			st, err := s.prepareChecked(ctx, sess, me, "SELECT * FROM "+qualified, nil)
			if err != nil {
				return cleanroom.Source{}, err
			}
			_ = st.close()
		}
		cols, err := describeTable(ctx, sess, qualified)
		if err != nil {
			return cleanroom.Source{}, pgError(err)
		}
		src := cleanroom.Source{Schema: schema, Table: table, Kind: cleanroom.SourceExternal, Provider: s.external.ProviderOf(schema, table)}
		for _, c := range cols {
			src.Columns = append(src.Columns, c.name)
		}
		src.Identity, err = s.sourceIdentity(ctx, sess, src.Kind, schema, table)
		return src, err
	}
	if len(name) == 3 && s.connectionCatalogs()[strings.ToLower(name[0])] != "" {
		// Its database keeps it: whoever has access there, or another
		// connection to it, reads it, so no room can keep it to a party.
		return cleanroom.Source{}, pgerror.WithCode(fmt.Errorf("%s is a table of the connection %s, whose database no clean room can keep "+
			"to one party; contribute a table of the catalog with what the room may read (CREATE TABLE ... AS)", strings.Join(name, "."), name[0]),
			codes.FeatureNotSupported)
	}
	o, err := s.resolveGrantTable(ctx, sess, name)
	if err != nil {
		return cleanroom.Source{}, err
	}
	src := cleanroom.Source{Schema: o.Schema, Table: o.Table, Kind: cleanroom.SourceTable}
	release := s.holdCatalog(sess)
	tables, err := queryStrings(ctx, sess, `SELECT table_name FROM system.main.duckdb_tables()
		WHERE database_name = $1 AND schema_name = $2 AND table_name = $3`, catalogName, o.Schema, o.Table)
	release()
	if err != nil {
		return src, err
	}
	if len(tables) == 0 {
		// A view's rows are those of the tables it reads, which the room
		// could keep to the party only by taking them from everyone else.
		return src, pgerror.WithCode(fmt.Errorf("%s is a view; contribute a table of what the room may read "+
			"(CREATE TABLE ... AS, or a materialized view, which Igloo keeps fresh)", o), codes.FeatureNotSupported)
	}
	st, err := s.prepareChecked(ctx, sess, me, "SELECT * FROM "+quoteName([]string{catalogName, o.Schema, o.Table}), nil)
	if err != nil {
		return src, err
	}
	defer st.close() //nolint:errcheck
	if applied := st.policies(); len(applied) > 0 {
		return src, pgerror.WithCode(fmt.Errorf("%s has masking or row access policies (%s); contribute a table of what the room may read instead",
			o, strings.Join(applied, ", ")), codes.FeatureNotSupported)
	}
	for _, c := range st.columns {
		src.Columns = append(src.Columns, c.Name)
	}
	src.Identity, err = s.sourceIdentity(ctx, sess, src.Kind, o.Schema, o.Table)
	return src, err
}

// --- runs: package cleanroom's Engine ------------------------------------------

// Confined applies a change of clean rooms' confinements on this replica
// at once.
func (s *Server) Confined(ctx context.Context) error {
	defer s.ddlGen.Add(1)
	return s.gov.reload(ctx)
}

// Temporary tables of a run's session.
const (
	roomGroups = "temp.main.__igloo_groups"
	roomResult = "temp.main.__igloo_result"
)

// Execute runs a clean room's plan, in a new session of the room's own
// identity: role none, with SELECT on the plan's tables and nothing else,
// which signs in nowhere. Its statements take the same checks as any
// user's, and the result never holds a group below its thresholds.
func (s *Server) Execute(ctx context.Context, room string, plan *cleanroom.Plan, result *cleanroom.ResultTarget) (*cleanroom.Outcome, error) {
	// The newest confinements, so the room's tables are its own here.
	if err := s.gov.reload(ctx); err != nil {
		return nil, fmt.Errorf("load the policies: %w", err)
	}
	es, err := s.openRoomSession(ctx, room, plan.Reads)
	if err != nil {
		return nil, err
	}
	defer es.Close() //nolint:errcheck
	sess := es.sess
	args := make([]driver.NamedValue, len(plan.Args))
	for i, a := range plan.Args {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}

	// Each dataset's source is still the table it was when it was added,
	// and when the approvals were given (cleanroom.Dataset.Identity).
	for _, d := range plan.Datasets {
		if id, err := s.sourceIdentity(ctx, sess, d.Kind, d.Schema, d.Table); err != nil || id != d.Identity {
			return nil, pgerror.WithCode(fmt.Errorf("the source of dataset %s, %s, is not what it was when it was added: made again, "+
				"or with other columns; its party replaces the dataset (REPLACE DATASET), and every party approves again", d.Name, d.Source()),
				codes.ObjectNotInPrerequisiteState)
		}
	}
	// A value of the wrong type fails here, where its error can only
	// quote the requester's own value.
	if plan.Values != "" {
		if err := execOn(ctx, sess, plan.Values, args); err != nil {
			return nil, trimContext(err)
		}
	}
	if plan.Check != "" {
		checked, _, err := s.roomChecked(ctx, sess, plan.Check)
		if err != nil {
			return nil, err
		}
		counts, err := roomCounts(ctx, sess, checked, args)
		if err != nil {
			return nil, s.hideRunError(room, err)
		}
		if err := plan.Narrowing(counts); err != nil {
			return nil, err
		}
	}
	// From here on, whether and how the run fails could depend on the
	// datasets' rows, so every failure ends the same way.
	out, err := s.runGroups(ctx, sess, plan, args, result)
	if err != nil {
		return nil, s.hideRunError(room, err)
	}
	return out, nil
}

// runGroups computes a run's groups, keeps those that meet the thresholds,
// and writes them to result.
func (s *Server) runGroups(ctx context.Context, sess *session, plan *cleanroom.Plan, args []driver.NamedValue, result *cleanroom.ResultTarget) (*cleanroom.Outcome, error) {
	start := time.Now()
	groups, acc, err := s.roomChecked(ctx, sess, plan.Groups)
	if err != nil {
		s.recordQuery(sess, queryRecord{text: plan.Groups, start: start, err: err, rows: -1})
		return nil, err
	}
	groupArgs := args
	if plan.Seed != "" {
		groupArgs = append(slices.Clone(args), driver.NamedValue{Ordinal: len(args) + 1, Value: plan.Seed})
	}
	err = execOn(ctx, sess, "CREATE TEMP TABLE __igloo_groups AS "+groups, groupArgs)
	s.recordQuery(sess, queryRecord{text: plan.Groups, start: start, err: err, rows: -1, acc: acc})
	if err != nil {
		return nil, err
	}
	out := &cleanroom.Outcome{}
	if err := execOn(ctx, sess, "CREATE TEMP TABLE __igloo_result AS "+plan.Keep(roomGroups), nil); err != nil {
		return nil, err
	}
	if err := execOn(ctx, sess, "DROP TABLE "+roomGroups, nil); err != nil {
		return nil, err
	}
	n, err := scalarOn(ctx, sess, "SELECT count(*) FROM "+roomResult)
	if err != nil {
		return nil, err
	}
	out.Truncated = n > int64(plan.Cap)
	kept := "SELECT * FROM " + roomResult + " LIMIT " + strconv.Itoa(plan.Cap)
	if out.Columns, out.Rows, err = textRows(ctx, sess, kept); err != nil {
		return nil, err
	}
	if result != nil {
		if err := s.writeResult(ctx, sess, kept, result); err != nil {
			return nil, fmt.Errorf("write the result to %s.%s: %w", result.Schema, result.Table, err)
		}
	}
	return out, nil
}

// openRoomSession opens a session of a room's identity, which may read
// reads and nothing else.
func (s *Server) openRoomSession(ctx context.Context, room string, reads []auth.Object) (*EmbeddedSession, error) {
	user := auth.User{Name: policy.RoomUser(room), Role: auth.RoleNone, Kind: auth.KindService}
	privs := &auth.Privileges{}
	for _, o := range reads {
		privs.Grants = append(privs.Grants, auth.Grant{Grantee: user.Name, GranteeType: auth.GranteeUser,
			Privilege: auth.PrivilegeSelect, Object: o})
	}
	es, err := s.openEmbedded(ctx, user, nil, "clean room "+room)
	if err != nil {
		return nil, err
	}
	es.sess.mu.Lock()
	es.sess.fixed = privs
	es.sess.mu.Unlock()
	return es, nil
}

// roomChecked checks query as the session's statement, as any user's
// statement is checked, and returns what may run of it.
func (s *Server) roomChecked(ctx context.Context, sess *session, query string) (string, *access, error) {
	st, err := s.prepareChecked(ctx, sess, sess.currentUser(), query, nil)
	if err != nil {
		return "", nil, err
	}
	defer st.close() //nolint:errcheck
	if st.stmtType != duckdb.STATEMENT_TYPE_SELECT {
		return "", nil, fmt.Errorf("a clean room run is a query")
	}
	return st.query, st.access, nil
}

// hideRunError replaces an error from reading a room's datasets, whose
// message, code or very happening could depend on another party's values
// (a failed cast, an overflow), with the refusal every such run ends with
// (cleanroom.Unfinished); the log keeps it for admins.
func (s *Server) hideRunError(room string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	s.log.Warn("a clean room run failed", "room", room, "err", err)
	return cleanroom.Unfinished
}

// roomCounts runs the population check and returns its counts.
func roomCounts(ctx context.Context, sess *session, query string, args []driver.NamedValue) ([]int64, error) {
	rows, err := sess.conn.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil {
		return nil, err
	}
	counts := make([]int64, len(dest))
	for i, v := range dest {
		counts[i], _ = v.(int64)
	}
	return counts, nil
}

func execOn(ctx context.Context, sess *session, query string, args []driver.NamedValue) error {
	_, err := sess.conn.ExecContext(ctx, query, args)
	return err
}

func scalarOn(ctx context.Context, sess *session, query string) (int64, error) {
	rows, err := sess.conn.QueryContext(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return 0, err
	}
	n, _ := dest[0].(int64)
	return n, nil
}

// roomRows collects a result's rows as text.
type roomRows struct {
	columns []cleanroom.Column
	rows    [][]*string
}

func (r *roomRows) Columns(cols []Column) error {
	for _, c := range cols {
		r.columns = append(r.columns, cleanroom.Column{Name: c.Name, Type: c.Type})
	}
	return nil
}

func (r *roomRows) Row(values []any) error {
	row := make([]*string, len(values))
	for i, v := range values {
		if s, ok := v.(string); ok {
			row[i] = &s
		}
	}
	r.rows = append(r.rows, row)
	return nil
}

func (r *roomRows) Complete(string) error { return nil }

// textRows runs query on the session's connection and returns its rows in
// PostgreSQL's text format, as SQL clients see them.
func textRows(ctx context.Context, sess *session, query string) ([]cleanroom.Column, [][]*string, error) {
	st, err := prepare(ctx, sess.conn, query, nil)
	if err != nil {
		return nil, nil, err
	}
	defer st.close() //nolint:errcheck
	out := &roomRows{}
	if err := out.Columns(columnsOf(st.columns)); err != nil {
		return nil, nil, err
	}
	w := &embeddedWriter{out: out, columns: st.columns, types: pgtype.NewMap(), values: make([]any, len(st.columns))}
	if err := st.run(ctx, nil, w); err != nil {
		return nil, nil, err
	}
	if out.rows == nil {
		out.rows = [][]*string{}
	}
	return out.columns, out.rows, nil
}

// icebergTypes are the DuckDB types an Iceberg table cannot have, and the
// type a result's column of one is written as.
var icebergTypes = map[string]string{
	"TINYINT": "INTEGER", "SMALLINT": "INTEGER", "UTINYINT": "INTEGER", "USMALLINT": "INTEGER", "UINTEGER": "BIGINT",
	"UBIGINT": "DECIMAL(20,0)", "HUGEINT": "DECIMAL(38,0)", "UHUGEINT": "DECIMAL(38,0)", "INTERVAL": "VARCHAR",
}

// writeResult replaces a local collaborator's result table with the rows
// of query, and lets its user or role read it. The room's confinement of
// the table, in place since the template was added, keeps everyone else
// out.
func (s *Server) writeResult(ctx context.Context, sess *session, query string, t *cleanroom.ResultTarget) error {
	catalogName := s.catalogName()
	cols, err := describeTable(ctx, sess, roomResult)
	if err != nil {
		return err
	}
	items := make([]string, len(cols))
	for i, c := range cols {
		items[i] = quoteIdent(c.name)
		if to, ok := icebergTypes[strings.ToUpper(c.typ)]; ok {
			items[i] = "CAST(" + quoteIdent(c.name) + " AS " + to + ") AS " + quoteIdent(c.name)
		}
	}
	table := quoteName([]string{catalogName, t.Schema, t.Table})
	// DDL changes the catalog (see holdCatalog); a result is a few rows.
	release := s.holdCatalog(sess)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + quoteName([]string{catalogName, t.Schema}),
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " AS SELECT " + strings.Join(items, ", ") + " FROM (" + query + ")",
	} {
		if err := execOn(ctx, sess, q, nil); err != nil {
			release()
			return trimContext(err)
		}
	}
	release()
	s.ddlGen.Add(1)
	err = s.grants.Grant(ctx, []auth.Grant{{Grantee: t.Grantee, GranteeType: t.GranteeType, Privilege: auth.PrivilegeSelect,
		Object: auth.Object{Schema: t.Schema, Table: t.Table}, GrantedBy: sess.currentUser().Name}})
	// Sessions here read their grants again, as after GRANT.
	s.sessions.expireAll()
	return err
}

// DropResult drops a clean room's result table, if it exists, with its
// grants.
func (s *Server) DropResult(ctx context.Context, schema, table string) error {
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck
	// Outside a session: listing and DDL take the engine's catalog lock
	// (see engine.Engine.CatalogLock). Room commands hold none when they
	// get here.
	release := s.engine.CatalogLock()
	defer release()
	rows, err := conn.QueryContext(ctx, `SELECT count(*) FROM system.main.duckdb_tables()
		WHERE database_name = $1 AND schema_name = $2 AND table_name = $3`,
		[]driver.NamedValue{{Ordinal: 1, Value: s.catalogName()}, {Ordinal: 2, Value: schema}, {Ordinal: 3, Value: table}})
	if err != nil {
		return err
	}
	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	_ = rows.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n, _ := dest[0].(int64); n > 0 {
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteName([]string{s.catalogName(), schema, table}), nil); err != nil {
			return err
		}
		s.ddlGen.Add(1)
	}
	return s.grants.ForgetObject(ctx, auth.Object{Schema: schema, Table: table})
}
