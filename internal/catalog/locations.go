package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	icecat "github.com/apache/iceberg-go/catalog"
)

// Locations returns the location of every table and view of the catalog:
// the files under them are theirs, and erasure requests delete the
// warehouse's files under none (see package erasure). A table or view
// dropped between listing and loading is left out; one that cannot be
// loaded is an error, as its files could be anywhere.
func (c *Client) Locations(ctx context.Context) ([]string, error) {
	tables, err := c.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	var out []string
	for _, id := range tables {
		tbl, err := c.LoadTable(ctx, id)
		switch {
		case errors.Is(err, icecat.ErrNoSuchTable):
		case err != nil:
			return nil, fmt.Errorf("load table %s: %w", strings.Join(id, "."), err)
		default:
			out = append(out, tbl.Location())
		}
	}
	views, err := c.Views(ctx)
	if err != nil {
		return nil, fmt.Errorf("list views: %w", err)
	}
	for _, id := range views {
		v, err := c.LoadView(ctx, id)
		switch {
		case errors.Is(err, icecat.ErrNoSuchView):
		case err != nil:
			return nil, fmt.Errorf("load view %s: %w", strings.Join(id, "."), err)
		default:
			out = append(out, v.Location())
		}
	}
	return out, nil
}
