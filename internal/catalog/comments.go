package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
)

// commentProperty is where Iceberg engines (Spark, Trino) keep a table's,
// view's or namespace's comment. Column comments are the schema fields'
// doc strings.
const commentProperty = "comment"

// Comment is the comment on a schema, table, view or column.
type Comment struct {
	Schema string
	// Name is the table or view; empty for the schema's own comment.
	Name string
	// Column is empty for the comment on the table or view itself.
	Column string
	Text   string
	// View says whether Name is a view.
	View bool
}

// SetTableComment sets or, when text is nil, removes a table's comment.
func (c *Client) SetTableComment(ctx context.Context, schema, name string, text *string) error {
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return err
	}
	var update table.Update
	if text != nil {
		update = table.NewSetPropertiesUpdate(iceberg.Properties{commentProperty: *text})
	} else {
		update = table.NewRemovePropertiesUpdate([]string{commentProperty})
	}
	// Lakekeeper rejects a commit without requirements.
	_, _, err = c.CommitTable(ctx, tbl.Identifier(),
		[]table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID())}, []table.Update{update})
	return err
}

// SetColumnComment sets or, when text is nil, removes a column's comment.
// Column names match case-insensitively, as in DuckDB.
func (c *Client) SetColumnComment(ctx context.Context, schema, name, column string, text *string) error {
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return err
	}
	field, ok := tbl.Schema().FindFieldByNameCaseInsensitive(column)
	if !ok {
		return fmt.Errorf("column %q of table %q does not exist", column, name)
	}
	doc := ""
	if text != nil {
		doc = *text
	}
	tx := tbl.NewTransaction()
	err = tx.UpdateSchema(false, false).
		UpdateColumn([]string{field.Name}, table.ColumnUpdate{Doc: iceberg.Optional[string]{Valid: true, Val: doc}}).
		Commit()
	if err != nil {
		return err
	}
	_, err = tx.Commit(ctx)
	return err
}

// SetViewComment sets or, when text is nil, removes a view's comment.
func (c *Client) SetViewComment(ctx context.Context, schema, name string, text *string) error {
	ident := Ident(schema, name)
	v, err := c.LoadView(ctx, ident)
	if err != nil {
		return err
	}
	var update view.Update
	if text != nil {
		update = view.NewSetPropertiesUpdate(iceberg.Properties{commentProperty: *text})
	} else {
		update = view.NewRemovePropertiesUpdate([]string{commentProperty})
	}
	_, err = c.UpdateView(ctx, ident, []view.Requirement{view.AssertViewUUID(v.Metadata().ViewUUID())}, []view.Update{update})
	return err
}

// SetSchemaComment sets or, when text is nil, removes a schema's comment,
// the namespace's "comment" property.
func (c *Client) SetSchemaComment(ctx context.Context, schema string, text *string) error {
	var (
		removals []string
		updates  iceberg.Properties
	)
	if text != nil {
		updates = iceberg.Properties{commentProperty: *text}
	} else {
		removals = []string{commentProperty}
	}
	_, err := c.UpdateNamespaceProperties(ctx, strings.Split(schema, "."), removals, updates)
	return err
}

// splitIdent returns the namespace, joined with dots if nested, and the name.
func splitIdent(ident table.Identifier) (schema, name string) {
	return strings.Join(icecat.NamespaceFromIdent(ident), "."), icecat.TableNameFromIdent(ident)
}

// ignoreDropped treats a table or view dropped since it was listed as gone
// rather than as an error.
func ignoreDropped(err error) error {
	if errors.Is(err, icecat.ErrNoSuchTable) || errors.Is(err, icecat.ErrNoSuchView) {
		return nil
	}
	return err
}
