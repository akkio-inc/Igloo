//go:build !unix

package modelpack

import (
	"io"
	"os"
)

// mapFile reads the file into memory where it cannot be mapped.
func mapFile(f *os.File, size int64) ([]byte, func() error, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, nil, err
	}
	return data, func() error { return nil }, nil
}
