// Package iddomain keeps Igloo's ID domains: named dictionaries that give
// the values of identifier columns, across the tables that hold them, dense
// integer ids. Counting distinct people over integers, and keeping a set of
// them as a bitmap, is far cheaper than over the text identifiers tables
// hold, and ids of one domain mean the same person in every table.
//
// A domain has a type, the DuckDB type of the values it assigns ids to, and
// tags, which the dictionary's value column carries and every member column
// must carry too, so that a request to erase a person (package erasure)
// finds the dictionary as it finds the tables. Each member column gets a
// column beside it, HiddenColumn of its name, that holds its values' ids.
//
// Ids start at 0 and are never handed out twice: Allocate takes them from a
// counter in the metadata database that only goes up, so an id an erasure
// retired stays retired. They stay below 2^32, the range a 32-bit bitmap
// holds; a domain that has assigned them all assigns no more (ErrExhausted),
// and the columns of values it has not seen stay NULL.
//
// This package only stores domains and members and hands out ids and the
// lease that serializes a domain's assignments; package pgwire carries out
// the statements that change them, and the dictionary itself is an Iceberg
// table in DictionarySchema.
package iddomain

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// MaxIDs is how many ids a domain can assign: ids are below 2^32.
const MaxIDs int64 = 1 << 32

// DictionarySchema is the catalog schema that holds the dictionaries, one
// table a domain, named after it. It is Igloo's own.
const DictionarySchema = "igloo_domains"

// DictionaryPolicy is the row access policy, Igloo's own, that every
// dictionary carries: it is false, so no user's statement reads a
// dictionary's rows, admins' included, and the dictionaries count as
// protected tables wherever Igloo serves files or tables (Delta Sharing,
// clean rooms, its Iceberg REST endpoint, reads of the warehouse's files).
// Igloo reads them itself, outside users' statements.
const DictionaryPolicy = "igloo_id_domain_dictionary"

// hiddenPrefix starts the name of the column that holds a member column's
// ids.
const hiddenPrefix = "_igloo_id_"

var (
	ErrNotFound = errors.New("does not exist")
	ErrExists   = errors.New("already exists")
	// ErrInUse refuses to drop a domain that columns are members of.
	ErrInUse = errors.New("is in use")
	// ErrInvalid is a name, type or member that does not fit.
	ErrInvalid = errors.New("invalid")
	// ErrExhausted says a domain has assigned every id below 2^32.
	ErrExhausted = errors.New("has assigned every id below 2^32")
	// ErrBusy says another assignment holds the domain's lease.
	ErrBusy = errors.New("is busy with another assignment")
	// ErrErased says an erasure deleted from the domain's dictionary
	// while an assignment looked for new values, which it then appends
	// none of (see Store.Erased).
	ErrErased = errors.New("was erased from during the assignment")
)

// Types are the value types a domain may have.
var Types = []string{"VARCHAR", "UUID", "BIGINT", "INTEGER"}

// Domain is an ID domain.
type Domain struct {
	// Name is lower case.
	Name string `json:"name"`
	// Type is one of Types.
	Type string `json:"type"`
	// Tags are the tags (name to value) the dictionary's value column
	// carries and member columns must carry.
	Tags map[string]string `json:"tags,omitempty"`
	// Generation numbers the dictionary: it changes only when the
	// dictionary is made again from nothing, which makes every id of the
	// one before meaningless.
	Generation int       `json:"generation"`
	Created    time.Time `json:"created"`
	CreatedBy  string    `json:"created_by,omitempty"`
	Comment    string    `json:"comment,omitempty"`
}

// Member is a column of a table of the Iceberg catalog whose values a
// domain gives ids. Names are as the catalog has them; like policies'
// attachments, members stay with a table's name when it is dropped or
// replaced, so a table a sync creates again is a member at once.
type Member struct {
	Domain  string    `json:"domain"`
	Schema  string    `json:"schema"`
	Table   string    `json:"table"`
	Column  string    `json:"column"`
	Added   time.Time `json:"added"`
	AddedBy string    `json:"added_by,omitempty"`
}

// Is reports whether m is the column schema.table.column; names compare as
// DuckDB's do, ignoring case.
func (m Member) Is(schema, table, column string) bool {
	return m.OfTable(schema, table) && strings.EqualFold(m.Column, column)
}

// OfTable reports whether m is a column of schema.table.
func (m Member) OfTable(schema, table string) bool {
	return strings.EqualFold(m.Schema, schema) && strings.EqualFold(m.Table, table)
}

// String names the column as schema.table.column.
func (m Member) String() string { return m.Schema + "." + m.Table + "." + m.Column }

// HiddenColumn returns the name of the column that holds the ids of member
// column column's values.
func HiddenColumn(column string) string { return hiddenPrefix + column }

// IsHidden reports whether name is a column that holds ids, or a name a
// user may not give a column.
func IsHidden(name string) bool {
	return len(name) >= len(hiddenPrefix) && strings.EqualFold(name[:len(hiddenPrefix)], hiddenPrefix)
}

// IndexSchema is the catalog schema that holds bitmap indexes' tables, one
// table an index, named after it. It is Igloo's own.
const IndexSchema = "igloo_bitmaps"

// IndexPolicy is the row access policy, Igloo's own, that every bitmap
// index's table carries, as DictionaryPolicy does the dictionaries': it is
// false, so no user's statement reads the bitmaps, and the tables count as
// protected wherever Igloo serves files or tables. Igloo reads them
// itself, to answer counts.
const IndexPolicy = "igloo_bitmap_index"

// MaxIndexGroups bounds the group columns of a bitmap index: each
// combination of their values has a bitmap.
const MaxIndexGroups = 4

// BitmapIndex keeps, for a member column of a table, a roaring bitmap of
// its values' ids for each combination of the values of Groups (one bitmap
// for the whole table when there are none), in the table IndexSchema.Name,
// so that counts of distinct members by those columns read bitmaps rather
// than the table. Names are as the catalog has them.
type BitmapIndex struct {
	// Name is lower case, and names the index's table.
	Name    string    `json:"name"`
	Schema  string    `json:"schema"`
	Table   string    `json:"table"`
	Column  string    `json:"column"`
	Groups  []string  `json:"groups,omitempty"`
	Added   time.Time `json:"added"`
	AddedBy string    `json:"added_by,omitempty"`
}

// OfTable reports whether x is an index of schema.table.
func (x BitmapIndex) OfTable(schema, table string) bool {
	return strings.EqualFold(x.Schema, schema) && strings.EqualFold(x.Table, table)
}

// Catalog is every domain, member and bitmap index, kept as one document.
type Catalog struct {
	Domains []Domain      `json:"domains,omitempty"`
	Members []Member      `json:"members,omitempty"`
	Indexes []BitmapIndex `json:"indexes,omitempty"`
}

func (c *Catalog) clone() Catalog {
	out := Catalog{Domains: slices.Clone(c.Domains), Members: slices.Clone(c.Members), Indexes: slices.Clone(c.Indexes)}
	for i, x := range out.Indexes {
		out.Indexes[i].Groups = slices.Clone(x.Groups)
	}
	for i, d := range out.Domains {
		if d.Tags != nil {
			tags := make(map[string]string, len(d.Tags))
			for k, v := range d.Tags {
				tags[k] = v
			}
			out.Domains[i].Tags = tags
		}
	}
	return out
}

// Domain returns the domain named name, or nil.
func (c *Catalog) Domain(name string) *Domain {
	for i := range c.Domains {
		if c.Domains[i].Name == strings.ToLower(name) {
			return &c.Domains[i]
		}
	}
	return nil
}

// MemberOf returns the member that is schema.table.column, or nil.
func (c *Catalog) MemberOf(schema, table, column string) *Member {
	for i := range c.Members {
		if c.Members[i].Is(schema, table, column) {
			return &c.Members[i]
		}
	}
	return nil
}

// TableMembers returns the members that are columns of schema.table.
func (c *Catalog) TableMembers(schema, table string) []Member {
	var out []Member
	for _, m := range c.Members {
		if m.OfTable(schema, table) {
			out = append(out, m)
		}
	}
	return out
}

// DomainMembers returns domain's members.
func (c *Catalog) DomainMembers(domain string) []Member {
	var out []Member
	for _, m := range c.Members {
		if m.Domain == strings.ToLower(domain) {
			out = append(out, m)
		}
	}
	return out
}

var namePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// CheckName checks a domain's name: a lower-case identifier, so it names
// its dictionary table the same in every engine.
func CheckName(name string) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("%w: an ID domain's name is lower-case letters, digits and _, not starting with a digit (at most 63): %q", ErrInvalid, name)
	}
	return nil
}

// CheckType returns typ as one of Types, or an error.
func CheckType(typ string) (string, error) {
	t := strings.ToUpper(strings.TrimSpace(typ))
	switch t {
	case "TEXT", "STRING":
		t = "VARCHAR"
	case "INT", "INT4":
		t = "INTEGER"
	case "INT8":
		t = "BIGINT"
	}
	if !slices.Contains(Types, t) {
		return "", fmt.Errorf("%w: an ID domain's values are %s, not %s", ErrInvalid, strings.Join(Types, ", "), typ)
	}
	return t, nil
}

// AddDomain adds d, which must be new and valid.
func (c *Catalog) AddDomain(d Domain) error {
	if err := CheckName(d.Name); err != nil {
		return err
	}
	t, err := CheckType(d.Type)
	if err != nil {
		return err
	}
	d.Type = t
	if c.Domain(d.Name) != nil {
		return fmt.Errorf("ID domain %s %w", d.Name, ErrExists)
	}
	c.Domains = append(c.Domains, d)
	return nil
}

// DropDomain drops the domain named name, which no column may be a member
// of.
func (c *Catalog) DropDomain(name string) error {
	if c.Domain(name) == nil {
		return fmt.Errorf("ID domain %s %w", name, ErrNotFound)
	}
	if ms := c.DomainMembers(name); len(ms) > 0 {
		return fmt.Errorf("ID domain %s %w: %s is a member (UNSET ID DOMAIN first)", name, ErrInUse, ms[0])
	}
	c.Domains = slices.DeleteFunc(c.Domains, func(d Domain) bool { return d.Name == strings.ToLower(name) })
	return nil
}

// AddMember makes m a member of its domain. A column is a member of one
// domain at most, and a table's member columns must not be the column that
// holds another's ids.
func (c *Catalog) AddMember(m Member) error {
	m.Domain = strings.ToLower(m.Domain)
	if c.Domain(m.Domain) == nil {
		return fmt.Errorf("ID domain %s %w", m.Domain, ErrNotFound)
	}
	if IsHidden(m.Column) {
		return fmt.Errorf("%w: %s holds ids itself", ErrInvalid, m.Column)
	}
	if old := c.MemberOf(m.Schema, m.Table, m.Column); old != nil {
		if old.Domain == m.Domain {
			return nil
		}
		return fmt.Errorf("%w: %s is already a member of ID domain %s", ErrInvalid, m, old.Domain)
	}
	c.Members = append(c.Members, m)
	return nil
}

// Index returns the bitmap index named name, or nil.
func (c *Catalog) Index(name string) *BitmapIndex {
	for i := range c.Indexes {
		if c.Indexes[i].Name == strings.ToLower(name) {
			return &c.Indexes[i]
		}
	}
	return nil
}

// TableIndexes returns the bitmap indexes of schema.table.
func (c *Catalog) TableIndexes(schema, table string) []BitmapIndex {
	var out []BitmapIndex
	for _, x := range c.Indexes {
		if x.OfTable(schema, table) {
			out = append(out, x)
		}
	}
	return out
}

// AddIndex adds x, which must be new: a column of its table that is a
// member of a domain, grouped by up to MaxIndexGroups other columns, once
// each.
func (c *Catalog) AddIndex(x BitmapIndex) error {
	x.Name = strings.ToLower(x.Name)
	if !namePattern.MatchString(x.Name) {
		return fmt.Errorf("%w: a bitmap index's name is lower-case letters, digits and _, not starting with a digit (at most 63): %q", ErrInvalid, x.Name)
	}
	if c.Index(x.Name) != nil {
		return fmt.Errorf("bitmap index %s %w", x.Name, ErrExists)
	}
	if c.MemberOf(x.Schema, x.Table, x.Column) == nil {
		return fmt.Errorf("%w: %s.%s.%s is not a member of an ID domain (SET ID DOMAIN first)", ErrInvalid, x.Schema, x.Table, x.Column)
	}
	if len(x.Groups) > MaxIndexGroups {
		return fmt.Errorf("%w: a bitmap index groups by at most %d columns", ErrInvalid, MaxIndexGroups)
	}
	for i, g := range x.Groups {
		switch {
		case IsHidden(g):
			return fmt.Errorf("%w: %s holds ids", ErrInvalid, g)
		case strings.EqualFold(g, x.Column):
			return fmt.Errorf("%w: a bitmap index of %s does not group by it", ErrInvalid, x.Column)
		case slices.ContainsFunc(x.Groups[:i], func(o string) bool { return strings.EqualFold(o, g) }):
			return fmt.Errorf("%w: %s is named twice", ErrInvalid, g)
		}
	}
	c.Indexes = append(c.Indexes, x)
	return nil
}

// DropIndex drops the bitmap index named name, reporting whether there was
// one.
func (c *Catalog) DropIndex(name string) bool {
	n := len(c.Indexes)
	c.Indexes = slices.DeleteFunc(c.Indexes, func(x BitmapIndex) bool { return x.Name == strings.ToLower(name) })
	return len(c.Indexes) < n
}

// RemoveMember removes column schema.table.column from its domain,
// reporting whether it was a member.
func (c *Catalog) RemoveMember(schema, table, column string) bool {
	n := len(c.Members)
	c.Members = slices.DeleteFunc(c.Members, func(m Member) bool { return m.Is(schema, table, column) })
	return len(c.Members) < n
}

// Snapshot is a catalog as a store read it, with its version.
type Snapshot struct {
	Version int64
	Catalog
}

// Store keeps the catalog, the domains' id counters and their assignment
// leases.
type Store interface {
	// Load reads the catalog.
	Load(ctx context.Context) (*Snapshot, error)
	// Version is the catalog's version, which every change increments.
	Version(ctx context.Context) (int64, error)
	// Update changes the catalog with change, atomically; change's error
	// leaves it as it was.
	Update(ctx context.Context, change func(*Catalog) error) error
	// Allocate hands out n consecutive ids of domain's generation and
	// returns the first. Ids are never handed out twice. It returns
	// ErrExhausted, handing out none, when they would reach MaxIDs, and
	// ErrInvalid when generation is not the domain's counter's.
	Allocate(ctx context.Context, domain string, generation int, n int64) (int64, error)
	// Next returns the next id domain would hand out, and its counter's
	// generation (0 for a domain that has handed out none).
	Next(ctx context.Context, domain string) (next int64, generation int, err error)
	// Lock takes domain's assignment lease for holder, waiting up to wait
	// for another holder to let it go, and returns ErrBusy if it does not.
	// A lease lasts ttl, so a holder that stops does not hold it for good;
	// unlock lets it go early. Holding it again renews it.
	Lock(ctx context.Context, domain, holder string, ttl, wait time.Duration) (unlock func(), err error)
	// Erased returns how many times erasures have deleted from domain's
	// dictionary under its lease (MarkErased), which an assignment reads
	// before it looks for new values and again under the lease: a change
	// means an erasure may have deleted, meanwhile, rows the assignment
	// found values in, and it appends none of them.
	Erased(ctx context.Context, domain string) (int64, error)
	// MarkErased counts one more erasure of domain; an erasure calls it
	// holding the domain's lease.
	MarkErased(ctx context.Context, domain string) error
	Close()
}

// pollLease is how often Lock tries again for a lease another holds.
const pollLease = 50 * time.Millisecond

// waitLease calls try until it takes the lease, ctx ends or wait passes.
func waitLease(ctx context.Context, wait time.Duration, try func() (bool, error)) error {
	deadline := time.Now().Add(wait)
	for {
		ok, err := try()
		if err != nil || ok {
			return err
		}
		if time.Now().After(deadline) {
			return ErrBusy
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollLease):
		}
	}
}
