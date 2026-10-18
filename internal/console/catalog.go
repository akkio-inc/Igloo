package console

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// The catalog pages: search over the catalog's tables and views, and a page
// for each with its owner, comments, columns, tags, size, freshness, usage
// and lineage. They read igloo.tables, igloo.columns, igloo.lineage and the
// other catalog views as the signed-in user, who sees only what the server
// lets them (see docs/catalog.md), and change comments with COMMENT ON,
// which the server allows owners, writers and admins.

const (
	// maxResults caps a search's results.
	maxResults = 200
	// maxDepth is how many steps up and down the lineage a page shows.
	maxDepth = 3
	// maxComment caps a comment's length, in bytes.
	maxComment = 16 << 10
)

// lookupAll runs lookups one after another as user, in one session, and
// returns their results.
func (c *Console) lookupAll(ctx context.Context, user auth.User, r *http.Request, sqls ...string) ([]result, error) {
	es, err := c.open(ctx, user, r, appLookup)
	if err != nil {
		return nil, err
	}
	defer es.Close() //nolint:errcheck
	out := make([]result, len(sqls))
	for i, sql := range sqls {
		var res collect
		if err := es.Run(ctx, sql, &res); err != nil {
			return nil, err
		}
		if res.Rows == nil {
			res.Rows = [][]any{}
		}
		out[i] = res.result
	}
	return out, nil
}

// objectsOf maps a result's rows to objects keyed by column name.
func objectsOf(res result) []map[string]any {
	out := make([]map[string]any, len(res.Rows))
	for i, row := range res.Rows {
		m := make(map[string]any, len(row))
		for j, v := range row {
			m[res.Columns[j]] = v
		}
		out[i] = m
	}
	return out
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// match is a search hit: a table or view and why it matched.
type match struct {
	Catalog      string   `json:"catalog"`
	Schema       string   `json:"schema"`
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Owner        any      `json:"owner"`
	Comment      any      `json:"comment"`
	Provider     any      `json:"provider"`
	Rows         any      `json:"rows"`
	Bytes        any      `json:"bytes"`
	LastAltered  any      `json:"last_altered"`
	Reads        any      `json:"reads_30d"`
	Users        any      `json:"users_30d"`
	Columns      []string `json:"columns,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	MatchedName  bool     `json:"matched_name"`
	MatchedOther []string `json:"matched,omitempty"`
}

// searchSQL finds the tables and views whose schema, name, comment, owner,
// provider, columns, column comments or tags contain every term, or for
// a connection's table its connection's name.
func searchSQL(terms []string) string {
	where := "true"
	for _, term := range terms {
		where += " AND contains(lower(concat_ws(' ', t.table_schema, t.table_name, t.comment, t.table_owner, t.provider," +
			" CASE WHEN t.table_type = 'FOREIGN' THEN t.table_catalog END, s.comment, c.names, c.comments, g.tags))," +
			" lower(" + quoteLiteral(term) + "))"
	}
	nameHit := "false"
	for _, term := range terms {
		nameHit += " OR contains(lower(t.table_name), lower(" + quoteLiteral(term) + "))"
	}
	return `WITH c AS (
	SELECT lower(table_catalog) AS d, lower(table_schema) AS s, lower(table_name) AS n,
		string_agg(column_name, ' ') AS names, string_agg(comment, ' ') AS comments,
		to_json(list(column_name ORDER BY ordinal_position)) AS column_list,
		to_json(list(coalesce(comment, '') ORDER BY ordinal_position)) AS comment_list
	FROM igloo.columns GROUP BY 1, 2, 3
), g AS (
	SELECT lower(object_database) AS d, lower(object_schema) AS s, lower(object_name) AS n,
		string_agg(tag_name || '=' || tag_value || ' ' || column_name, ' ') AS tags,
		to_json(list(DISTINCT tag_name || ' = ' || tag_value)) AS tag_list
	FROM igloo.tag_references GROUP BY 1, 2, 3
)
SELECT t.table_catalog, t.table_schema, t.table_name, t.table_type, t.table_owner, t.comment, t.provider, t.row_count, t.bytes,
	t.last_altered, u.reads_30d, u.users_30d, c.column_list, c.comment_list, g.tag_list, s.comment AS schema_comment,
	(` + nameHit + `) AS name_hit
FROM igloo.tables t
LEFT JOIN igloo.schemata s ON s.catalog_name = t.table_catalog AND s.schema_name = t.table_schema
LEFT JOIN c ON c.d = lower(t.table_catalog) AND c.s = lower(t.table_schema) AND c.n = lower(t.table_name)
LEFT JOIN g ON g.d = lower(t.table_catalog) AND g.s = lower(t.table_schema) AND g.n = lower(t.table_name)
LEFT JOIN igloo.table_usage u ON lower(u.table_catalog) = lower(t.table_catalog) AND lower(u.table_schema) = lower(t.table_schema)
	AND lower(u.table_name) = lower(t.table_name)
WHERE ` + where + `
ORDER BY name_hit DESC, coalesce(u.reads_30d, 0) DESC, t.table_type = 'FOREIGN', lower(t.table_schema), lower(t.table_name)
LIMIT ` + strconv.Itoa(maxResults)
}

// searchTerms splits a search into its words, at most eight.
func searchTerms(q string) []string {
	terms := strings.Fields(q)
	if len(terms) > 8 {
		terms = terms[:8]
	}
	return terms
}

// search finds tables and views by their names, comments, owners,
// columns and tags: GET /api/catalog/search?q=words.
func (c *Console) search(w http.ResponseWriter, r *http.Request, req *request) {
	terms := searchTerms(r.URL.Query().Get("q"))
	res, err := c.lookup(r.Context(), req.user, r, searchSQL(terms))
	if err != nil {
		writeSQLError(w, err)
		return
	}
	matches := []match{}
	for _, o := range objectsOf(res) {
		m := match{Catalog: str(o["table_catalog"]), Schema: str(o["table_schema"]), Name: str(o["table_name"]), Type: str(o["table_type"]),
			Owner: o["table_owner"], Comment: o["comment"], Provider: o["provider"], Rows: o["row_count"], Bytes: o["bytes"],
			LastAltered: o["last_altered"], Reads: o["reads_30d"], Users: o["users_30d"], MatchedName: str(o["name_hit"]) == "t"}
		var columns, comments []string
		_ = json.Unmarshal([]byte(str(o["column_list"])), &columns)
		_ = json.Unmarshal([]byte(str(o["comment_list"])), &comments)
		_ = json.Unmarshal([]byte(str(o["tag_list"])), &m.Tags)
		// Say what matched, besides the name.
		for _, term := range terms {
			t := strings.ToLower(term)
			has := func(s string) bool { return strings.Contains(strings.ToLower(s), t) }
			switch {
			case has(m.Name) || has(m.Schema) || m.Type == "FOREIGN" && has(m.Catalog):
			case has(str(m.Comment)):
				m.MatchedOther = appendNew(m.MatchedOther, "description")
			case has(str(m.Owner)):
				m.MatchedOther = appendNew(m.MatchedOther, "owner")
			case has(str(m.Provider)):
				m.MatchedOther = appendNew(m.MatchedOther, "provider")
			default:
				for i, col := range columns {
					if has(col) || i < len(comments) && has(comments[i]) {
						m.MatchedOther = appendNew(m.MatchedOther, "column "+col)
					}
				}
				for _, tag := range m.Tags {
					if has(tag) {
						m.MatchedOther = appendNew(m.MatchedOther, "tag "+tag)
					}
				}
				if has(str(o["schema_comment"])) {
					m.MatchedOther = appendNew(m.MatchedOther, "schema description")
				}
			}
		}
		if len(m.MatchedOther) > 5 {
			m.MatchedOther = m.MatchedOther[:5]
		}
		m.Columns = columns
		matches = append(matches, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": matches, "limit": maxResults})
}

func appendNew(s []string, v string) []string {
	if slices.Contains(s, v) {
		return s
	}
	return append(s, v)
}

// node and edge are the lineage graph around a table.
type node struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"` // table, location or hidden
	Label string `json:"label"`
	// Catalog, Schema and Name name a table.
	Catalog string `json:"catalog,omitempty"`
	Schema  string `json:"schema,omitempty"`
	Name    string `json:"name,omitempty"`
	// Depth is how many steps upstream (negative) or downstream
	// (positive) of the table it is.
	Depth int `json:"depth"`
	// Count is how many tables a hidden node stands for.
	Count int `json:"count,omitempty"`
}

type edge struct {
	From       string `json:"from"`
	To         string `json:"to"`
	LastSeen   any    `json:"last_seen,omitempty"`
	Statements any    `json:"statements,omitempty"`
	Job        any    `json:"job,omitempty"`
}

// lineageGraph is what the lineage says about the table db.schema.name:
// the tables and locations its data came from and the tables made from
// it, up to maxDepth steps each way, with each table's ends the user may
// not see counted, not named. Tables of other databases than db, such as
// a connection's, are labelled with their database.
func lineageGraph(rows []map[string]any, db, schema, name string) ([]node, []edge) {
	key := func(db, schema, name string) string { return "table:" + strings.ToLower(db+"."+schema+"."+name) }
	table := func(d, s, n string) node {
		label := s + "." + n
		if !strings.EqualFold(d, db) {
			label = d + "." + label
		}
		return node{Kind: "table", Catalog: d, Schema: s, Name: n, Label: label}
	}
	type end struct {
		id    string
		n     node
		known bool
	}
	from := func(o map[string]any) end {
		switch {
		case str(o["upstream_type"]) == "LOCATION" && o["upstream_name"] != nil:
			l := str(o["upstream_name"])
			return end{id: "location:" + l, n: node{Kind: "location", Label: l}, known: true}
		case o["upstream_name"] == nil:
			return end{}
		}
		d, s, n := str(o["upstream_database"]), str(o["upstream_schema"]), str(o["upstream_name"])
		return end{id: key(d, s, n), n: table(d, s, n), known: true}
	}
	to := func(o map[string]any) end {
		if o["downstream_name"] == nil {
			return end{}
		}
		d, s, n := str(o["downstream_database"]), str(o["downstream_schema"]), str(o["downstream_name"])
		return end{id: key(d, s, n), n: table(d, s, n), known: true}
	}

	root := key(db, schema, name)
	rootNode := table(db, schema, name)
	rootNode.ID = root
	nodes := map[string]*node{root: &rootNode}
	order := []string{root}
	var edges []edge
	seen := map[[2]string]bool{}
	add := func(e end, depth int) string {
		if n, ok := nodes[e.id]; ok {
			return n.ID
		}
		n := e.n
		n.ID, n.Depth = e.id, depth
		nodes[e.id] = &n
		order = append(order, e.id)
		return e.id
	}
	link := func(a, b string, o map[string]any) {
		if seen[[2]string{a, b}] {
			return
		}
		seen[[2]string{a, b}] = true
		edges = append(edges, edge{From: a, To: b, LastSeen: o["last_seen"], Statements: o["statements"], Job: o["last_job"]})
	}
	// Walk upstream (dir -1) and downstream (dir +1) breadth first.
	for _, dir := range []int{-1, 1} {
		frontier := []string{root}
		for depth := 1; depth <= maxDepth && len(frontier) > 0; depth++ {
			var next []string
			hidden := map[string]int{}
			for _, o := range rows {
				a, b := from(o), to(o)
				near, far := b, a
				if dir > 0 {
					near, far = a, b
				}
				if !near.known || !slices.Contains(frontier, near.id) {
					continue
				}
				if !far.known {
					hidden[near.id]++
					continue
				}
				_, existed := nodes[far.id]
				id := add(far, dir*depth)
				if dir < 0 {
					link(id, near.id, o)
				} else {
					link(near.id, id, o)
				}
				if !existed && far.n.Kind == "table" {
					next = append(next, id)
				}
			}
			for _, id := range frontier {
				if n := hidden[id]; n > 0 {
					h := "hidden:" + strconv.Itoa(dir) + ":" + id
					what := "table"
					if n > 1 {
						what = "tables"
					}
					side := "upstream"
					if dir > 0 {
						side = "downstream"
					}
					nodes[h] = &node{ID: h, Kind: "hidden", Count: n, Depth: dir * depth,
						Label: strconv.Itoa(n) + " hidden " + side + " " + what}
					order = append(order, h)
					if dir < 0 {
						edges = append(edges, edge{From: h, To: id})
					} else {
						edges = append(edges, edge{From: id, To: h})
					}
				}
			}
			frontier = next
		}
	}
	out := make([]node, len(order))
	for i, id := range order {
		out[i] = *nodes[id]
	}
	return out, edges
}

// object describes a table or view: GET
// /api/catalog/object?catalog=c&schema=s&name=n, where the catalog is the
// Iceberg catalog's or a connection's name. A table the user may not see
// is not found, as one that does not exist.
func (c *Console) object(w http.ResponseWriter, r *http.Request, req *request) {
	db, schema, name := r.URL.Query().Get("catalog"), r.URL.Query().Get("schema"), r.URL.Query().Get("name")
	if db == "" || schema == "" || name == "" {
		writeError(w, http.StatusBadRequest, "send a catalog, a schema and a name")
		return
	}
	d, s, n := quoteLiteral(db), quoteLiteral(schema), quoteLiteral(name)
	is := func(dbCol, schemaCol, nameCol string) string {
		return "lower(" + dbCol + ") = lower(" + d + ") AND lower(" + schemaCol + ") = lower(" + s + ") AND lower(" + nameCol + ") = lower(" + n + ")"
	}
	results, err := c.lookupAll(r.Context(), req.user, r,
		`SELECT table_catalog, table_schema, table_name, table_type, table_owner, owner_type, comment, provider, row_count, bytes,
			file_count, snapshot_count, last_altered FROM igloo.tables WHERE `+is("table_catalog", "table_schema", "table_name"),
		"SELECT schema_owner, owner_type, comment FROM igloo.schemata WHERE lower(catalog_name) = lower("+d+") AND lower(schema_name) = lower("+s+")",
		"SELECT column_name, data_type, comment FROM igloo.columns WHERE "+is("table_catalog", "table_schema", "table_name")+" ORDER BY ordinal_position",
		"SELECT column_name, tag_name, tag_value FROM igloo.tag_references WHERE "+is("object_database", "object_schema", "object_name")+" ORDER BY 1, 2",
		"SELECT policy_name, policy_kind, ref_column_name, tag_name FROM igloo.policy_references WHERE "+
			is("ref_database_name", "ref_schema_name", "ref_entity_name")+" ORDER BY 3 NULLS FIRST, 1",
		"SELECT reads_30d, writes_30d, users_30d, last_read, last_written FROM igloo.table_usage WHERE "+
			is("table_catalog", "table_schema", "table_name"),
		`SELECT upstream_type, upstream_database, upstream_schema, upstream_name, downstream_database, downstream_schema,
			downstream_name, last_seen, statements, last_job FROM igloo.lineage`,
		"SELECT sql FROM duckdb_views() WHERE NOT internal AND lower(schema_name) = lower("+s+") AND lower(view_name) = lower("+n+") LIMIT 1",
	)
	if err != nil {
		writeSQLError(w, err)
		return
	}
	objects := objectsOf(results[0])
	if len(objects) == 0 {
		writeError(w, http.StatusNotFound, "no table or view "+db+"."+schema+"."+name+" that you may see")
		return
	}
	obj := objects[0]
	// The names as the catalog has them.
	db, schema, name = str(obj["table_catalog"]), str(obj["table_schema"]), str(obj["table_name"])
	nodes, edges := lineageGraph(objectsOf(results[6]), db, schema, name)
	out := map[string]any{
		"object":   obj,
		"columns":  objectsOf(results[2]),
		"tags":     objectsOf(results[3]),
		"policies": objectsOf(results[4]),
		"lineage":  map[string]any{"nodes": nodes, "edges": edges},
	}
	if s := objectsOf(results[1]); len(s) > 0 {
		out["schema"] = s[0]
	}
	if u := objectsOf(results[5]); len(u) > 0 {
		out["usage"] = u[0]
	}
	if str(obj["table_type"]) == "VIEW" && len(results[7].Rows) > 0 {
		out["definition"] = results[7].Rows[0][0]
	}
	// Comments are kept in the Iceberg metadata, which external tables and
	// connections' tables have none of.
	switch str(obj["table_type"]) {
	case "BASE TABLE", "VIEW":
		out["can_comment"] = c.mayComment(r.Context(), req.user, r, schema, str(obj["table_owner"]), str(obj["owner_type"]))
	default:
		out["can_comment"] = false
	}
	writeJSON(w, http.StatusOK, out)
}

// mayComment says whether to offer user to change the comments of a table
// in schema with the owner given: writers and admins, its owner, and
// those with CREATE on its schema may. The server decides when they try.
func (c *Console) mayComment(ctx context.Context, user auth.User, r *http.Request, schema, owner, ownerType string) bool {
	if user.Role == auth.RoleAdmin || user.Role == auth.RoleWriter || ownerType == auth.GranteeUser && owner == user.Name {
		return true
	}
	results, err := c.lookupAll(ctx, user, r, "SHOW ROLES", "SHOW GRANTS")
	if err != nil {
		return false
	}
	if ownerType == auth.GranteeRole && slices.ContainsFunc(objectsOf(results[0]), func(o map[string]any) bool { return str(o["name"]) == owner }) {
		return true
	}
	return slices.ContainsFunc(objectsOf(results[1]), func(o map[string]any) bool {
		return str(o["privilege"]) == string(auth.PrivilegeCreate) && str(o["object_type"]) == "schema" &&
			strings.EqualFold(str(o["object_name"]), schema)
	})
}

type commentChange struct {
	// Kind is schema, table (or view) or column.
	Kind    string  `json:"kind"`
	Schema  string  `json:"schema"`
	Name    string  `json:"name"`
	Column  string  `json:"column"`
	Comment *string `json:"comment"`
}

// comment sets or removes a comment with COMMENT ON, as the user: POST
// /api/catalog/comment. An empty comment removes it.
func (c *Console) comment(w http.ResponseWriter, r *http.Request, req *request) {
	var in commentChange
	if !readJSON(w, r, 2*maxComment, &in) {
		return
	}
	if in.Comment != nil && len(*in.Comment) > maxComment {
		writeError(w, http.StatusBadRequest, "a comment is at most 16 KB")
		return
	}
	text := "NULL"
	if in.Comment != nil && strings.TrimSpace(*in.Comment) != "" {
		text = quoteLiteral(*in.Comment)
	}
	var sql string
	switch {
	case in.Kind == "schema" && in.Schema != "":
		sql = "COMMENT ON SCHEMA " + ident(in.Schema)
	case in.Kind == "table" && in.Schema != "" && in.Name != "":
		sql = "COMMENT ON TABLE " + ident(in.Schema) + "." + ident(in.Name)
	case in.Kind == "column" && in.Schema != "" && in.Name != "" && in.Column != "":
		sql = "COMMENT ON COLUMN " + ident(in.Schema) + "." + ident(in.Name) + "." + ident(in.Column)
	default:
		writeError(w, http.StatusBadRequest, "send kind (schema, table or column) and the names it needs")
		return
	}
	sql += " IS " + text
	if n := len(sqllex.Split(sql)); n != 1 {
		writeError(w, http.StatusBadRequest, "send one comment")
		return
	}
	res, err := c.run(r.Context(), req.user, r, sql)
	if err != nil {
		writeSQLError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"tag": res.Tag})
}

// ident quotes a name for SQL.
func ident(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
