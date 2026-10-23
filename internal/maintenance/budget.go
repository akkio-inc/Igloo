package maintenance

import (
	"context"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/filerewrite"
)

// DefaultMemory is the memory compaction may hold when Options.Memory is
// zero.
const DefaultMemory = filerewrite.DefaultMemory

// Compaction rewrites files within Options.Memory (see package filerewrite).
var (
	errOverBudget = filerewrite.ErrOverBudget
	metered       = filerewrite.Metered
)

// groupWriter writes the rows of a group's files into new files and returns
// them, as rewriteGroup and the writer of clustering do.
type groupWriter func(ctx context.Context, tbl *table.Table, fs iceio.IO, g table.CompactionTaskGroup, budget int64) ([]iceberg.DataFile, filerewrite.Layout, error)

// rewriteGroup writes the rows of g's files, with their deletes applied, into
// new files of the table's target size, and returns them, holding no more
// than budget bytes as counted by the allocator of ctx (see metered).
func rewriteGroup(ctx context.Context, tbl *table.Table, fs iceio.IO, g table.CompactionTaskGroup, budget int64) ([]iceberg.DataFile, filerewrite.Layout, error) {
	return filerewrite.Rewrite(ctx, tbl, fs, g.Tasks, budget, nil)
}
