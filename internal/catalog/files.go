package catalog

import (
	"context"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/config"
)

// FS opens the object store that holds location, normally the warehouse
// root, with Igloo's own storage credentials, as DuckDB reads it. The Delta
// Sharing endpoint serves recipients the data files of shared tables
// through it.
func FS(ctx context.Context, s config.Storage, location string) (iceio.IO, error) {
	return iceio.LoadFS(ctx, ioProperties(s), location)
}
