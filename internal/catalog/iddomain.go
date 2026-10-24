package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// IDDomainPropertyPrefix starts the table property that says a column is a
// member of an ID domain (package iddomain): igloo.id-domain.<column> =
// <domain>@<generation>, beside the column that holds its values' ids.
const IDDomainPropertyPrefix = "igloo.id-domain."

// ErrHiddenColumnTaken refuses a column joining an ID domain when its table
// already has a column, which Igloo did not add, of the name its ids would
// go in.
var ErrHiddenColumnTaken = errors.New("the name for the ids is taken")

// hiddenColumnDoc is the doc string of a column that holds a member column's
// ids, which other engines show as its comment.
const hiddenColumnDoc = "Igloo's ids of the values of %s in ID domain %s (do not write: Igloo fills it)"

// JoinIDDomain makes column a member of ID domain domain, at generation, in
// the table schema.name: in one commit it adds hidden, a nullable BIGINT,
// if the table lacks it, and sets the column's IDDomainPropertyPrefix
// property. Column names match case-insensitively, as in DuckDB. It
// reports whether it changed the table, which it does not when the column
// is a member already, and the table's current snapshot (0 for none).
func (c *Client) JoinIDDomain(ctx context.Context, schema, name, column, hidden, domain string, generation int) (changed bool, snapshot int64, err error) {
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return false, 0, err
	}
	if s := tbl.CurrentSnapshot(); s != nil {
		snapshot = s.SnapshotID
	}
	col, ok := tbl.Schema().FindFieldByNameCaseInsensitive(column)
	if !ok {
		return false, snapshot, fmt.Errorf("%w: column %q of table %q does not exist", ErrNoSuchColumn, column, name)
	}
	var updates []table.Update
	reqs := []table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID())}
	if f, ok := tbl.Schema().FindFieldByNameCaseInsensitive(hidden); !ok {
		us := tbl.NewTransaction().UpdateSchema(false, false).
			AddColumn([]string{hidden}, iceberg.PrimitiveTypes.Int64, fmt.Sprintf(hiddenColumnDoc, col.Name, domain), false, nil)
		u, r, err := us.BuildUpdates()
		if err != nil {
			return false, snapshot, err
		}
		updates, reqs = append(updates, u...), append(reqs, r...)
	} else if _, member := tbl.Metadata().Properties()[IDDomainPropertyPrefix+col.Name]; !member || !f.Type.Equals(iceberg.PrimitiveTypes.Int64) {
		// A column of that name that Igloo did not add holds something
		// else, which must not be taken for ids.
		return false, snapshot, fmt.Errorf("%w: table %q already has a column %s, the name Igloo's ids of %s go in", ErrHiddenColumnTaken, name, f.Name, col.Name)
	}
	key, value := IDDomainPropertyPrefix+col.Name, fmt.Sprintf("%s@%d", domain, generation)
	if tbl.Metadata().Properties()[key] != value {
		updates = append(updates, table.NewSetPropertiesUpdate(iceberg.Properties{key: value}))
	}
	if len(updates) == 0 {
		return false, snapshot, nil
	}
	_, _, err = c.CommitTable(ctx, tbl.Identifier(), reqs, updates)
	return err == nil, snapshot, err
}

// LeaveIDDomain undoes JoinIDDomain: it drops hidden, if the table has it,
// and column's property.
func (c *Client) LeaveIDDomain(ctx context.Context, schema, name, column, hidden string) error {
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return err
	}
	var updates []table.Update
	reqs := []table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID())}
	if f, ok := tbl.Schema().FindFieldByNameCaseInsensitive(hidden); ok {
		u, r, err := tbl.NewTransaction().UpdateSchema(false, false).DeleteColumn([]string{f.Name}).BuildUpdates()
		if err != nil {
			return err
		}
		updates, reqs = append(updates, u...), append(reqs, r...)
	}
	var props []string
	for k := range tbl.Metadata().Properties() {
		if strings.HasPrefix(k, IDDomainPropertyPrefix) && strings.EqualFold(k[len(IDDomainPropertyPrefix):], column) {
			props = append(props, k)
		}
	}
	if len(props) > 0 {
		updates = append(updates, table.NewRemovePropertiesUpdate(props))
	}
	if len(updates) == 0 {
		return nil
	}
	_, _, err = c.CommitTable(ctx, tbl.Identifier(), reqs, updates)
	return err
}

// IDDomainMembers returns, for a table's metadata, its columns that are
// members of ID domains and the domain and generation each is a member of
// (column to "domain@generation"), from its properties.
func IDDomainMembers(meta table.Metadata) map[string]string {
	out := map[string]string{}
	for k, v := range meta.Properties() {
		if strings.HasPrefix(k, IDDomainPropertyPrefix) {
			out[k[len(IDDomainPropertyPrefix):]] = v
		}
	}
	return out
}
