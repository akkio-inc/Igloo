// Package policy keeps Igloo's data governance rules: masking policies,
// row access policies and tags, and which tables and columns they are
// attached to. Package pgwire enforces them by rewriting statements; this
// package only stores them and answers what applies to a table.
//
// Everything is kept as one document (Catalog) with a version number, so a
// change is one atomic update and replicas tell that something changed by
// comparing versions.
package policy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// Kind says what a policy does.
type Kind string

const (
	// Masking policies replace a column's values with what their
	// expression returns for them.
	Masking Kind = "MASKING POLICY"
	// RowAccess policies hide the rows their expression is not true for.
	RowAccess Kind = "ROW ACCESS POLICY"
)

var (
	ErrNotFound = errors.New("does not exist")
	ErrExists   = errors.New("already exists")
	// ErrInUse refuses to drop or change something that protects data.
	ErrInUse = errors.New("is in use")
	// ErrInvalid is a change that does not fit what it refers to, such as a
	// tag value the tag does not allow.
	ErrInvalid = errors.New("invalid")
)

// Arg is a policy's argument: a column's value when the policy runs.
type Arg struct {
	Name string `json:"name"`
	// Type is a DuckDB type as DuckDB prints it, such as VARCHAR or
	// DECIMAL(18,3).
	Type string `json:"type"`
}

// Policy is a masking or row access policy. Its body is a SQL expression of
// its arguments; table names in it are fully qualified, so they mean the
// same in every session.
type Policy struct {
	Kind      Kind      `json:"kind"`
	Name      string    `json:"name"`
	Args      []Arg     `json:"args"`
	Returns   string    `json:"returns"`
	Body      string    `json:"body"`
	Comment   string    `json:"comment,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Signature is how the policy is called, such as (val VARCHAR) RETURNS
// VARCHAR.
func (p Policy) Signature() string {
	args := make([]string, len(p.Args))
	for i, a := range p.Args {
		args[i] = a.Name + " " + a.Type
	}
	return "(" + strings.Join(args, ", ") + ") RETURNS " + p.Returns
}

// Tag classifies columns, such as pii = 'email'. Masking policies set on a
// tag mask every column that has the tag, one policy per data type.
type Tag struct {
	Name string `json:"name"`
	// AllowedValues, if any, are the only values the tag may have.
	AllowedValues []string  `json:"allowed_values,omitempty"`
	Comment       string    `json:"comment,omitempty"`
	CreatedBy     string    `json:"created_by,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	// Policies are the masking policies set on the tag, by name.
	Policies []string `json:"policies,omitempty"`
}

// ColumnMask attaches a masking policy to a column. Catalog, in this and
// the other attachments, is empty for a table of the Iceberg catalog and
// names the connection, lower-cased, for a table of a connection to
// another database.
type ColumnMask struct {
	Catalog string    `json:"catalog,omitempty"`
	Schema  string    `json:"schema"`
	Table   string    `json:"table"`
	Column  string    `json:"column"`
	Policy  string    `json:"policy"`
	SetBy   string    `json:"set_by,omitempty"`
	SetAt   time.Time `json:"set_at"`
}

// RowFilter attaches a row access policy to a table, with the columns that
// are its arguments.
type RowFilter struct {
	Catalog string    `json:"catalog,omitempty"`
	Schema  string    `json:"schema"`
	Table   string    `json:"table"`
	Policy  string    `json:"policy"`
	Columns []string  `json:"columns"`
	SetBy   string    `json:"set_by,omitempty"`
	SetAt   time.Time `json:"set_at"`
}

// ColumnTag sets a tag on a column.
type ColumnTag struct {
	Catalog string    `json:"catalog,omitempty"`
	Schema  string    `json:"schema"`
	Table   string    `json:"table"`
	Column  string    `json:"column"`
	Tag     string    `json:"tag"`
	Value   string    `json:"value"`
	SetBy   string    `json:"set_by,omitempty"`
	SetAt   time.Time `json:"set_at"`
}

// TableName is a table's name as attachments show it: schema.table, or
// connection.schema.table for a table of a connection.
func TableName(catalog, schema, table string) string {
	if catalog != "" {
		return catalog + "." + schema + "." + table
	}
	return schema + "." + table
}

// DataRetention limits how long a table that holds personal data keeps
// old versions of its rows (ALTER TABLE ... SET (data_retention_days = N)):
// table maintenance expires its older snapshots and rewrites files whose
// deleted rows are older, so neither time travel nor the files reach back
// further.
type DataRetention struct {
	Schema string    `json:"schema"`
	Table  string    `json:"table"`
	Days   int       `json:"days"`
	SetBy  string    `json:"set_by,omitempty"`
	SetAt  time.Time `json:"set_at"`
}

// Catalog is every policy, tag and attachment. Attachments name tables and
// columns, which compare case-insensitively as DuckDB resolves them. They
// stay when a table or column is dropped, so one created again under the
// name is protected as soon as it exists.
type Catalog struct {
	Policies   []Policy        `json:"policies,omitempty"`
	Tags       []Tag           `json:"tags,omitempty"`
	Masks      []ColumnMask    `json:"masks,omitempty"`
	RowFilters []RowFilter     `json:"row_filters,omitempty"`
	ColumnTags []ColumnTag     `json:"column_tags,omitempty"`
	Retentions []DataRetention `json:"retentions,omitempty"`
}

func (c *Catalog) clone() Catalog {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	var out Catalog
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

func sameTable(catalog, schema, table, k, s, t string) bool {
	return strings.EqualFold(catalog, k) && strings.EqualFold(schema, s) && strings.EqualFold(table, t)
}

func sameColumn(catalog, schema, table, column, k, s, t, c string) bool {
	return sameTable(catalog, schema, table, k, s, t) && strings.EqualFold(column, c)
}

func (c *Catalog) policyIndex(name string) int {
	return slices.IndexFunc(c.Policies, func(p Policy) bool { return p.Name == name })
}

func (c *Catalog) tagIndex(name string) int {
	return slices.IndexFunc(c.Tags, func(t Tag) bool { return t.Name == name })
}

// Policy returns the policy called name.
func (c *Catalog) Policy(name string) (Policy, bool) {
	if i := c.policyIndex(name); i >= 0 {
		return c.Policies[i], true
	}
	return Policy{}, false
}

// Tag returns the tag called name.
func (c *Catalog) Tag(name string) (Tag, bool) {
	if i := c.tagIndex(name); i >= 0 {
		return c.Tags[i], true
	}
	return Tag{}, false
}

// uses lists where the policy called name is attached.
func (c *Catalog) uses(name string) []string {
	var out []string
	for _, m := range c.Masks {
		if m.Policy == name {
			out = append(out, "column "+TableName(m.Catalog, m.Schema, m.Table)+"."+m.Column)
		}
	}
	for _, f := range c.RowFilters {
		if f.Policy == name {
			out = append(out, "table "+TableName(f.Catalog, f.Schema, f.Table))
		}
	}
	for _, t := range c.Tags {
		if slices.Contains(t.Policies, name) {
			out = append(out, "tag "+t.Name)
		}
	}
	return out
}

func inUse(what string, uses []string) error {
	const shown = 3
	more := ""
	if len(uses) > shown {
		more = fmt.Sprintf(" and %d more", len(uses)-shown)
		uses = uses[:shown]
	}
	return fmt.Errorf("%s %w: set on %s%s", what, ErrInUse, strings.Join(uses, ", "), more)
}

// PutPolicy creates a policy or, with replace, replaces the body and
// comment of one of the same kind. A policy in use keeps its signature, as
// the columns it is attached to were checked against it.
func (c *Catalog) PutPolicy(p Policy, replace bool, now time.Time) error {
	i := c.policyIndex(p.Name)
	if i < 0 {
		p.CreatedAt, p.UpdatedAt = now, now
		c.Policies = append(c.Policies, p)
		return nil
	}
	old := c.Policies[i]
	switch {
	case !replace:
		return fmt.Errorf("policy %q %w", p.Name, ErrExists)
	case old.Kind != p.Kind:
		return fmt.Errorf("a %s named %q %w", strings.ToLower(string(old.Kind)), p.Name, ErrExists)
	case old.Signature() != p.Signature():
		if uses := c.uses(p.Name); len(uses) > 0 {
			return inUse(fmt.Sprintf("policy %q takes %s, so its signature cannot change while it", p.Name, old.Signature()), uses)
		}
	}
	p.CreatedBy, p.CreatedAt, p.UpdatedAt = old.CreatedBy, old.CreatedAt, now
	c.Policies[i] = p
	return nil
}

// DropPolicy drops a policy of kind. One that is attached anywhere stays:
// dropping it would unprotect what it protects.
func (c *Catalog) DropPolicy(kind Kind, name string) error {
	i := c.policyIndex(name)
	if i < 0 || c.Policies[i].Kind != kind {
		return fmt.Errorf("%s %q %w", strings.ToLower(string(kind)), name, ErrNotFound)
	}
	if uses := c.uses(name); len(uses) > 0 {
		return inUse(fmt.Sprintf("policy %q", name), uses)
	}
	c.Policies = slices.Delete(c.Policies, i, i+1)
	return nil
}

func (c *Catalog) policyOfKind(kind Kind, name string) (Policy, error) {
	p, ok := c.Policy(name)
	if !ok || p.Kind != kind {
		return Policy{}, fmt.Errorf("%s %q %w", strings.ToLower(string(kind)), name, ErrNotFound)
	}
	return p, nil
}

// SetMask attaches a masking policy to a column. A column has at most one;
// force replaces it.
func (c *Catalog) SetMask(m ColumnMask, force bool, now time.Time) error {
	if _, err := c.policyOfKind(Masking, m.Policy); err != nil {
		return err
	}
	m.SetAt = now
	for i, old := range c.Masks {
		if sameColumn(old.Catalog, old.Schema, old.Table, old.Column, m.Catalog, m.Schema, m.Table, m.Column) {
			if old.Policy != m.Policy && !force {
				return fmt.Errorf("column %s.%s has masking policy %q %w; add FORCE to replace it",
					TableName(m.Catalog, m.Schema, m.Table), m.Column, old.Policy, ErrExists)
			}
			c.Masks[i] = m
			return nil
		}
	}
	c.Masks = append(c.Masks, m)
	return nil
}

// UnsetMask detaches a column's masking policy.
func (c *Catalog) UnsetMask(catalog, schema, table, column string) error {
	n := len(c.Masks)
	c.Masks = slices.DeleteFunc(c.Masks, func(m ColumnMask) bool {
		return sameColumn(m.Catalog, m.Schema, m.Table, m.Column, catalog, schema, table, column)
	})
	if len(c.Masks) == n {
		return fmt.Errorf("column %s.%s has no masking policy: %w", TableName(catalog, schema, table), column, ErrNotFound)
	}
	return nil
}

// AddRowFilter attaches a row access policy to a table, which may have
// only one.
func (c *Catalog) AddRowFilter(f RowFilter, now time.Time) error {
	p, err := c.policyOfKind(RowAccess, f.Policy)
	if err != nil {
		return err
	}
	if len(f.Columns) != len(p.Args) {
		return fmt.Errorf("row access policy %q takes %d arguments, not %d: %w", p.Name, len(p.Args), len(f.Columns), ErrInvalid)
	}
	for _, old := range c.RowFilters {
		if sameTable(old.Catalog, old.Schema, old.Table, f.Catalog, f.Schema, f.Table) {
			return fmt.Errorf("table %s has row access policy %q %w; drop it first", TableName(f.Catalog, f.Schema, f.Table), old.Policy, ErrExists)
		}
	}
	f.SetAt = now
	c.RowFilters = append(c.RowFilters, f)
	return nil
}

// DropRowFilter detaches a table's row access policy, which must be name
// unless name is empty.
func (c *Catalog) DropRowFilter(catalog, schema, table, name string) error {
	n := len(c.RowFilters)
	c.RowFilters = slices.DeleteFunc(c.RowFilters, func(f RowFilter) bool {
		return sameTable(f.Catalog, f.Schema, f.Table, catalog, schema, table) && (name == "" || f.Policy == name)
	})
	if len(c.RowFilters) == n {
		if name == "" {
			return fmt.Errorf("table %s has no row access policy: %w", TableName(catalog, schema, table), ErrNotFound)
		}
		return fmt.Errorf("table %s has no row access policy %q: %w", TableName(catalog, schema, table), name, ErrNotFound)
	}
	return nil
}

// PutTag creates a tag or, with replace, changes its allowed values and
// comment. Its masking policies and the columns it is set on stay.
func (c *Catalog) PutTag(t Tag, replace bool, now time.Time) error {
	i := c.tagIndex(t.Name)
	if i < 0 {
		t.CreatedAt = now
		t.Policies = nil
		c.Tags = append(c.Tags, t)
		return nil
	}
	if !replace {
		return fmt.Errorf("tag %q %w", t.Name, ErrExists)
	}
	old := c.Tags[i]
	t.CreatedBy, t.CreatedAt, t.Policies = old.CreatedBy, old.CreatedAt, old.Policies
	c.Tags[i] = t
	return nil
}

// DropTag drops a tag that is set on no column.
func (c *Catalog) DropTag(name string) error {
	i := c.tagIndex(name)
	if i < 0 {
		return fmt.Errorf("tag %q %w", name, ErrNotFound)
	}
	var uses []string
	for _, ct := range c.ColumnTags {
		if ct.Tag == name {
			uses = append(uses, "column "+TableName(ct.Catalog, ct.Schema, ct.Table)+"."+ct.Column)
		}
	}
	if len(uses) > 0 {
		return inUse(fmt.Sprintf("tag %q", name), uses)
	}
	c.Tags = slices.Delete(c.Tags, i, i+1)
	return nil
}

// SetTagPolicy sets a masking policy on a tag. A tag has at most one
// policy per argument type, which decides the columns it masks.
func (c *Catalog) SetTagPolicy(tag, name string) error {
	i := c.tagIndex(tag)
	if i < 0 {
		return fmt.Errorf("tag %q %w", tag, ErrNotFound)
	}
	p, err := c.policyOfKind(Masking, name)
	if err != nil {
		return err
	}
	t := &c.Tags[i]
	if slices.Contains(t.Policies, name) {
		return nil
	}
	for _, other := range t.Policies {
		if q, ok := c.Policy(other); ok && q.Args[0].Type == p.Args[0].Type {
			return fmt.Errorf("tag %q has masking policy %q for %s columns %w; unset it first", tag, other, q.Args[0].Type, ErrExists)
		}
	}
	t.Policies = append(t.Policies, name)
	return nil
}

// UnsetTagPolicy removes a masking policy from a tag.
func (c *Catalog) UnsetTagPolicy(tag, name string) error {
	i := c.tagIndex(tag)
	if i < 0 {
		return fmt.Errorf("tag %q %w", tag, ErrNotFound)
	}
	t := &c.Tags[i]
	j := slices.Index(t.Policies, name)
	if j < 0 {
		return fmt.Errorf("tag %q has no masking policy %q: %w", tag, name, ErrNotFound)
	}
	t.Policies = slices.Delete(t.Policies, j, j+1)
	return nil
}

// SetColumnTag sets a tag on a column, replacing its value if it has the
// tag already.
func (c *Catalog) SetColumnTag(ct ColumnTag, now time.Time) error {
	t, ok := c.Tag(ct.Tag)
	if !ok {
		return fmt.Errorf("tag %q %w", ct.Tag, ErrNotFound)
	}
	if len(t.AllowedValues) > 0 && !slices.Contains(t.AllowedValues, ct.Value) {
		return fmt.Errorf("tag %q allows only %s, not %q: %w", t.Name, quoteList(t.AllowedValues), ct.Value, ErrInvalid)
	}
	ct.SetAt = now
	for i, old := range c.ColumnTags {
		if old.Tag == ct.Tag && sameColumn(old.Catalog, old.Schema, old.Table, old.Column, ct.Catalog, ct.Schema, ct.Table, ct.Column) {
			c.ColumnTags[i] = ct
			return nil
		}
	}
	c.ColumnTags = append(c.ColumnTags, ct)
	return nil
}

// UnsetColumnTag removes a tag from a column.
func (c *Catalog) UnsetColumnTag(catalog, schema, table, column, tag string) error {
	n := len(c.ColumnTags)
	c.ColumnTags = slices.DeleteFunc(c.ColumnTags, func(ct ColumnTag) bool {
		return ct.Tag == tag && sameColumn(ct.Catalog, ct.Schema, ct.Table, ct.Column, catalog, schema, table, column)
	})
	if len(c.ColumnTags) == n {
		return fmt.Errorf("column %s.%s has no tag %q: %w", TableName(catalog, schema, table), column, tag, ErrNotFound)
	}
	return nil
}

// SetRetention sets a table's data retention, replacing the one it has.
func (c *Catalog) SetRetention(r DataRetention, now time.Time) error {
	if r.Days < 0 {
		return fmt.Errorf("data_retention_days must not be negative: %w", ErrInvalid)
	}
	r.SetAt = now
	for i, old := range c.Retentions {
		if sameTable("", old.Schema, old.Table, "", r.Schema, r.Table) {
			c.Retentions[i] = r
			return nil
		}
	}
	c.Retentions = append(c.Retentions, r)
	return nil
}

// UnsetRetention removes a table's data retention.
func (c *Catalog) UnsetRetention(schema, table string) error {
	n := len(c.Retentions)
	c.Retentions = slices.DeleteFunc(c.Retentions, func(r DataRetention) bool {
		return sameTable("", r.Schema, r.Table, "", schema, table)
	})
	if len(c.Retentions) == n {
		return fmt.Errorf("table %s.%s has no data retention: %w", schema, table, ErrNotFound)
	}
	return nil
}

func quoteList(values []string) string {
	q := make([]string, len(values))
	for i, v := range values {
		q[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return strings.Join(q, ", ")
}

// --- snapshots ------------------------------------------------------------

// TagMask is a masking policy that applies to a column through a tag.
type TagMask struct {
	Tag, Value string
	Policy     Policy
}

// ColumnRules is what protects one column.
type ColumnRules struct {
	Column string
	// Mask is the policy attached to the column itself, which takes
	// precedence over its tags'.
	Mask *Policy
	// TagMasks are the policies of the column's tags, for every data type.
	TagMasks []TagMask
	// Tags are all the column's tags.
	Tags []ColumnTag
}

// Masked reports whether some masking policy applies to the column.
func (r *ColumnRules) Masked() bool { return r != nil && (r.Mask != nil || len(r.TagMasks) > 0) }

// TableRules is what protects one table. Catalog is empty for a table of
// the Iceberg catalog and names the connection for a table of one.
type TableRules struct {
	Catalog, Schema, Table string
	// Filter and FilterPolicy are the table's row access policy, if any.
	Filter       *RowFilter
	FilterPolicy *Policy
	// Columns are the columns with masking policies or tags, by lower-case
	// name.
	Columns map[string]*ColumnRules
	// Retention is the table's data retention, if any.
	Retention *DataRetention
}

// Protected reports whether a policy applies to the table: a row access
// policy, or a masking policy on a column.
func (r *TableRules) Protected() bool {
	if r == nil {
		return false
	}
	if r.Filter != nil {
		return true
	}
	for _, c := range r.Columns {
		if c.Masked() {
			return true
		}
	}
	return false
}

// Column returns the rules for a column, or nil.
func (r *TableRules) Column(name string) *ColumnRules {
	if r == nil {
		return nil
	}
	return r.Columns[strings.ToLower(name)]
}

type tableKey struct{ catalog, schema, table string }

func keyOf(catalog, schema, table string) tableKey {
	return tableKey{strings.ToLower(catalog), strings.ToLower(schema), strings.ToLower(table)}
}

// Snapshot is a Catalog as of a version, indexed by table. It must not be
// changed.
type Snapshot struct {
	Version int64
	Catalog Catalog
	tables  map[tableKey]*TableRules
	names   map[string]bool
	// connections is set when a policy protects a table of a connection.
	connections bool
}

// NewSnapshot indexes c.
func NewSnapshot(version int64, c Catalog) *Snapshot {
	s := &Snapshot{Version: version, Catalog: c, tables: map[tableKey]*TableRules{}, names: map[string]bool{}}
	table := func(catalog, schema, name string) *TableRules {
		k := keyOf(catalog, schema, name)
		t := s.tables[k]
		if t == nil {
			t = &TableRules{Catalog: strings.ToLower(catalog), Schema: schema, Table: name, Columns: map[string]*ColumnRules{}}
			s.tables[k] = t
		}
		return t
	}
	column := func(t *TableRules, name string) *ColumnRules {
		k := strings.ToLower(name)
		col := t.Columns[k]
		if col == nil {
			col = &ColumnRules{Column: name}
			t.Columns[k] = col
		}
		return col
	}
	for _, f := range c.RowFilters {
		if p, ok := c.Policy(f.Policy); ok {
			t := table(f.Catalog, f.Schema, f.Table)
			t.Filter, t.FilterPolicy = &f, &p
		}
	}
	for _, m := range c.Masks {
		if p, ok := c.Policy(m.Policy); ok {
			column(table(m.Catalog, m.Schema, m.Table), m.Column).Mask = &p
		}
	}
	for _, ct := range c.ColumnTags {
		col := column(table(ct.Catalog, ct.Schema, ct.Table), ct.Column)
		col.Tags = append(col.Tags, ct)
		if tag, ok := c.Tag(ct.Tag); ok {
			for _, name := range tag.Policies {
				if p, ok := c.Policy(name); ok {
					col.TagMasks = append(col.TagMasks, TagMask{Tag: ct.Tag, Value: ct.Value, Policy: p})
				}
			}
		}
	}
	for _, r := range c.Retentions {
		table("", r.Schema, r.Table).Retention = &r
	}
	for k, t := range s.tables {
		if t.Protected() {
			s.names[k.table] = true
			s.connections = s.connections || k.catalog != ""
		}
	}
	return s
}

// ProtectsConnections reports whether a policy protects a table of a
// connection to another database.
func (s *Snapshot) ProtectsConnections() bool { return s != nil && s.connections }

// Empty reports whether no policy protects anything.
func (s *Snapshot) Empty() bool { return s == nil || len(s.names) == 0 }

// Classified reports whether any table has policies, tags or a data
// retention, which all stay with tables' names.
func (s *Snapshot) Classified() bool { return s != nil && len(s.tables) > 0 }

// StagingMark is in the name of the table that CREATE OR REPLACE TABLE
// writes before renaming it into place: name__igloo_new_<random>. It has
// the policies of the table it replaces, so the new rows are protected
// before the rename too.
const StagingMark = "__igloo_new_"

// BaseName is the name of the table a staging table replaces, or table.
func BaseName(table string) string {
	if i := strings.Index(strings.ToLower(table), StagingMark); i > 0 {
		return table[:i]
	}
	return table
}

// Rules returns what protects schema.table of the Iceberg catalog, or nil
// when nothing does.
func (s *Snapshot) Rules(schema, table string) *TableRules {
	if s == nil {
		return nil
	}
	if t := s.tables[keyOf("", schema, BaseName(table))]; t.Protected() {
		return t
	}
	return nil
}

// ConnectionRules returns what protects schema.table of the connection
// called catalog, or nil when nothing does.
func (s *Snapshot) ConnectionRules(catalog, schema, table string) *TableRules {
	if s == nil || catalog == "" {
		return nil
	}
	if t := s.tables[keyOf(catalog, schema, table)]; t.Protected() {
		return t
	}
	return nil
}

// Tagged returns the tags and policies of schema.table of the Iceberg
// catalog, protecting it or not, or nil.
func (s *Snapshot) Tagged(schema, table string) *TableRules {
	if s == nil {
		return nil
	}
	return s.tables[keyOf("", schema, table)]
}

// ProtectedNames are the lower-case names of the protected tables, without
// their schemas or catalogs.
func (s *Snapshot) ProtectedNames() map[string]bool {
	if s == nil {
		return nil
	}
	return s.names
}

// Tables lists every table with rules or tags, by catalog, schema and
// name: the Iceberg catalog's first.
func (s *Snapshot) Tables() []*TableRules {
	if s == nil {
		return nil
	}
	out := make([]*TableRules, 0, len(s.tables))
	for _, t := range s.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := keyOf(out[i].Catalog, out[i].Schema, out[i].Table), keyOf(out[j].Catalog, out[j].Schema, out[j].Table)
		if a.catalog != b.catalog {
			return a.catalog < b.catalog
		}
		return a.schema < b.schema || a.schema == b.schema && a.table < b.table
	})
	return out
}

// --- stores ---------------------------------------------------------------

// Store keeps the Catalog. Implementations must be safe for concurrent
// use.
type Store interface {
	// Load returns the current catalog.
	Load(ctx context.Context) (*Snapshot, error)
	// Version returns the current version, which changes with every
	// update.
	Version(ctx context.Context) (int64, error)
	// Update applies change to the current catalog atomically. If change
	// fails, nothing changes.
	Update(ctx context.Context, change func(*Catalog) error) error
	Close()
}
