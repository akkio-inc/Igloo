package icebergrest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
)

// caller is who sent a request: the token's user as the users store has
// them now, and their grants.
type caller struct {
	user auth.User
	// privs are the user's grants; nil for writers and admins, whose role
	// allows everything a grant could.
	privs   *auth.Privileges
	session string
	// client names the engine, for the history.
	client string
}

func (c *caller) admin() bool { return c.user.Role == auth.RoleAdmin }

// writer reports whether c's role allows every table operation here.
func (c *caller) writer() bool {
	return c.user.Role == auth.RoleAdmin || c.user.Role == auth.RoleWriter
}

// may reports whether c may use priv on o, a table, or with CREATE a
// schema, by their role or their grants, as in SQL.
func (c *caller) may(priv auth.Privilege, o auth.Object) bool {
	switch {
	case c.writer():
		return true
	case priv == auth.PrivilegeSelect && c.user.Role == auth.RoleReader:
		return true
	case o.IsSchema():
		return c.privs.AllowsSchema(priv, o.Schema)
	}
	return c.privs.Allows(priv, o.Schema, o.Table)
}

// writesData reports whether c may change every row of table o. Only then
// do they get credentials that write its files: with those, an engine can
// rewrite or delete any of them.
func (c *caller) writesData(o auth.Object) bool {
	return c.may(auth.PrivilegeInsert, o) && c.may(auth.PrivilegeUpdate, o) && c.may(auth.PrivilegeDelete, o)
}

// sees reports whether c may know of table o: some privilege on it, or
// CREATE on its schema.
func (c *caller) sees(o auth.Object) bool {
	if c.user.Role == auth.RoleReader || c.writer() {
		return true
	}
	return slices.ContainsFunc(auth.TablePrivileges, func(p auth.Privilege) bool { return c.may(p, o) }) ||
		c.may(auth.PrivilegeCreate, auth.Object{Schema: o.Schema})
}

// seesSchema reports whether c may know of a schema: some grant in it.
// Grants on connections to other databases (Catalog set) are on their
// schemas, not the catalog's.
func (c *caller) seesSchema(schema string) bool {
	if c.user.Role == auth.RoleReader || c.writer() {
		return true
	}
	return c.privs != nil && slices.ContainsFunc(c.privs.Grants, func(g auth.Grant) bool {
		return g.Object.Catalog == "" && strings.EqualFold(g.Object.Schema, schema)
	})
}

// denied refuses a request that needs priv on o, naming both as a refused
// SQL statement does.
func (c *caller) denied(priv auth.Privilege, o auth.Object) *apiError {
	var msg string
	switch priv {
	case auth.PrivilegeCreate:
		msg = fmt.Sprintf("creating, changing or dropping tables in schema %s needs the writer role or CREATE on the schema", o.Schema)
	case auth.PrivilegeSelect:
		msg = fmt.Sprintf("SELECT on table %s needs the reader role or a SELECT grant", o)
	case auth.PrivilegeInsert, auth.PrivilegeUpdate:
		msg = fmt.Sprintf("%s on table %s needs the writer role or an %[1]s grant", priv, o)
	default:
		msg = fmt.Sprintf("%s on table %s needs the writer role or a %[1]s grant", priv, o)
	}
	return c.deny(msg)
}

// deny refuses a request with msg as the reason.
func (c *caller) deny(msg string) *apiError {
	return errorf(http.StatusForbidden, errForbidden, "permission denied for %s: %s", c.user.Name, msg)
}

// require refuses the request unless c may use priv on o.
func (c *caller) require(priv auth.Privilege, o auth.Object) error {
	if c.may(priv, o) {
		return nil
	}
	return c.denied(priv, o)
}

// requireSees refuses the request unless c may know of table o.
func (c *caller) requireSees(o auth.Object) error {
	if c.sees(o) {
		return nil
	}
	return c.deny(fmt.Sprintf("table %s needs a grant on it or CREATE on its schema", o))
}

// requireSeesSchema refuses the request unless c may know of schema.
func (c *caller) requireSeesSchema(schema string) error {
	if c.seesSchema(schema) {
		return nil
	}
	return c.deny(fmt.Sprintf("schema %s needs a grant on it or on a table in it", schema))
}

// requireWriter refuses the request unless c has the writer role; what
// names what needs it.
func (c *caller) requireWriter(what string) error {
	if c.writer() {
		return nil
	}
	return c.deny(what + " needs the writer role")
}

// dataPathProperties send an engine's writes outside the table's location,
// where credentials limited to it would not reach and another table's files
// could be. Lakekeeper refuses them too.
var dataPathProperties = []string{"write.data.path", "write.object-storage.path", "write.folder-storage.path", "write.metadata.path"}

// checkProperties refuses table properties that move a table's files.
func checkProperties(keys []string) error {
	for _, k := range keys {
		for _, p := range dataPathProperties {
			if strings.HasPrefix(strings.ToLower(k), p) {
				return errorf(http.StatusBadRequest, errBadRequest,
					"table property %s is not allowed: a table's files stay in its location", k)
			}
		}
	}
	return nil
}

// commitRequest is what Igloo reads of a table commit
// (CommitTableRequest).
type commitRequest struct {
	Identifier   *identifier   `json:"identifier"`
	Requirements []requirement `json:"requirements"`
	Updates      []tableUpdate `json:"updates"`
}

// requirement is one of a commit's requirements.
type requirement struct {
	Type string `json:"type"`
}

// tableUpdate is one update of a commit, with the fields Igloo reads.
type tableUpdate struct {
	Action string `json:"action"`
	// Snapshot is add-snapshot's, whose declared operation the history
	// shows.
	Snapshot *struct {
		Summary map[string]string `json:"summary"`
	} `json:"snapshot"`
	// Updates are set-properties', and Removals remove-properties'.
	Updates  map[string]string `json:"updates"`
	Removals []string          `json:"removals"`
	// Schema is add-schema's, and SchemaID set-current-schema's, -1
	// for the schema the commit added last.
	Schema   *schemaJSON `json:"schema"`
	SchemaID *int        `json:"schema-id"`
}

// schemaJSON is a schema, with the fields Igloo reads.
type schemaJSON struct {
	ID     int `json:"schema-id"`
	Fields []struct {
		ID   int             `json:"id"`
		Name string          `json:"name"`
		Type json.RawMessage `json:"type"`
	} `json:"fields"`
}

// field returns the top-level field of the schema with id, or by name,
// without regard to case, if id is 0.
func (s *schemaJSON) field(id int, name string) (int, string, string, bool) {
	for _, f := range s.Fields {
		if id != 0 && f.ID == id || id == 0 && strings.EqualFold(f.Name, name) {
			return f.ID, f.Name, string(f.Type), true
		}
	}
	return 0, "", "", false
}

// commitNeeds is what a commit needs of its user.
type commitNeeds struct {
	// table are privileges on the table.
	table []auth.Privilege
	// create is CREATE on the table's schema, as ALTER TABLE needs, and
	// CREATE TABLE AS for the commit that creates a staged table.
	create bool
	// creates is set for that commit, which fills the new table with
	// CREATE alone, as CREATE TABLE AS does.
	creates bool
	// writer is the writer role, which SQL requires for table properties,
	// and so for snapshot removal, which they govern.
	writer bool
	// admin are update actions only admins may commit: moving a table or
	// changing its UUID, which could point it at another table's files, and
	// actions Igloo does not know.
	admin []string
	// summary describes the commit for the history.
	summary []string
}

// rowChanges are the privileges a change of snapshots needs: it may change
// any row.
var rowChanges = []auth.Privilege{auth.PrivilegeInsert, auth.PrivilegeUpdate, auth.PrivilegeDelete}

// stagedUpdates restate the UUID and location Lakekeeper gave a staged
// table, in the commit that creates it.
var stagedUpdates = []string{"assign-uuid", "set-location"}

// classify works out what a commit needs. Any change to the table's
// snapshots needs INSERT, UPDATE and DELETE, whatever operation the engine
// declares: a snapshot is a list of files that Igloo would have to read to
// know what it does, and one declared an "append" could name an older
// snapshot's files and so undo later changes without writing a byte.
// Engines need those privileges for credentials that write the table's
// files anyway.
func classify(req commitRequest) commitNeeds {
	var n commitNeeds
	// The commit that creates a staged table, as in CREATE TABLE AS.
	if slices.ContainsFunc(req.Requirements, func(r requirement) bool { return r.Type == "assert-create" }) {
		n.create, n.creates = true, true
		n.summary = append(n.summary, "create")
	}
	for _, u := range req.Updates {
		action := u.Action
		switch u.Action {
		case "add-snapshot", "set-snapshot-ref", "set-statistics", "remove-statistics",
			"set-partition-statistics", "remove-partition-statistics":
			if !n.creates {
				n.table = slices.Clone(rowChanges)
			}
			if u.Snapshot != nil {
				action += " (" + u.Snapshot.Summary["operation"] + ")"
			}
		case "add-schema", "set-current-schema", "add-spec", "set-default-spec", "add-sort-order",
			"set-default-sort-order", "upgrade-format-version", "remove-partition-specs", "remove-schemas":
			n.create = true
		case "assign-uuid", "set-location":
			// A new table restates its own; updateTable leaves them out.
			if !n.creates {
				n.admin = append(n.admin, u.Action)
			}
		case "set-properties", "remove-properties":
			// A new table's properties are its creator's, as in CREATE TABLE.
			if !n.creates {
				n.writer = true
			}
		case "remove-snapshots", "remove-snapshot-ref":
			n.writer = true
		default:
			n.admin = append(n.admin, u.Action)
		}
		n.summary = append(n.summary, action)
	}
	return n
}

// propertyKeys are the table properties a commit sets.
func (req commitRequest) propertyKeys() []string {
	var keys []string
	for _, u := range req.Updates {
		if u.Action == "set-properties" {
			for k := range u.Updates {
				keys = append(keys, k)
			}
		}
	}
	return keys
}

// removedKeys are the table properties a commit removes.
func (req commitRequest) removedKeys() []string {
	var keys []string
	for _, u := range req.Updates {
		if u.Action == "remove-properties" {
			keys = append(keys, u.Removals...)
		}
	}
	return keys
}

// allowsCommit refuses a commit to table o that c may not make.
func (c *caller) allowsCommit(o auth.Object, n commitNeeds) error {
	if c.admin() {
		return nil
	}
	if len(n.admin) > 0 {
		return c.deny(fmt.Sprintf("only admins may commit %s", strings.Join(n.admin, ", ")))
	}
	if n.writer {
		if err := c.requireWriter("changing table properties or removing snapshots"); err != nil {
			return err
		}
	}
	if n.create {
		if err := c.require(auth.PrivilegeCreate, auth.Object{Schema: o.Schema}); err != nil {
			return err
		}
	}
	for _, p := range n.table {
		if err := c.require(p, o); err != nil {
			return err
		}
	}
	return c.requireSees(o)
}
