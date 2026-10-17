package catalog

import (
	"context"
	"errors"
	"io/fs"

	iceio "github.com/apache/iceberg-go/io"
	"gocloud.dev/gcerrors"

	"github.com/akkio-inc/igloo/internal/config"
)

// FS opens the object store that holds location, normally the warehouse
// root, with Igloo's own storage credentials, as DuckDB reads it. The Delta
// Sharing endpoint serves recipients the data files of shared tables
// through it.
func FS(ctx context.Context, s config.Storage, location string) (iceio.IO, error) {
	return iceio.LoadFS(ctx, ioProperties(s), location)
}

// Exists reports whether the file at path is in the object store, read
// with Igloo's own storage credentials.
func Exists(ctx context.Context, s config.Storage, path string) (bool, error) {
	fsys, err := FS(ctx, s, path)
	if err != nil {
		return false, err
	}
	f, err := fsys.Open(path)
	if errors.Is(err, fs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = f.Close()
	return true, nil
}
