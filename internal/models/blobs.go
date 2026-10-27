package models

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"strings"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/gcerrors"
)

// Dir is where models' blobs live under the warehouse's root: a file per
// blob, named by its SHA-256, among Igloo's own files (.igloo/), which no
// table owns and which neither tables' orphan scans nor erasure requests'
// sweeps of the warehouse delete.
const Dir = ".igloo/models"

// ErrNoBlob is a blob that is not there: its model was dropped or
// replaced.
var ErrNoBlob = errors.New("the model's blob is not in the warehouse")

// Blobs keeps model blobs by their SHA-256. Blobs never change: a model
// trained again is a new blob.
type Blobs interface {
	Put(ctx context.Context, sha string, data []byte) error
	Get(ctx context.Context, sha string) ([]byte, error)
	Delete(ctx context.Context, sha string) error
}

// Warehouse keeps blobs in object storage, under root's Dir, through a
// file system of the warehouse (catalog.FS): the server writes them, and
// workers read them with the access to the warehouse they have already.
type Warehouse struct {
	root string
	open func(ctx context.Context) (icebergio.IO, error)

	mu sync.Mutex
	fs icebergio.IO
}

// NewWarehouse returns the blobs under root (the warehouse's location,
// with a trailing slash), opened by open when first needed.
func NewWarehouse(root string, open func(ctx context.Context) (icebergio.IO, error)) *Warehouse {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}
	return &Warehouse{root: root, open: open}
}

// Path is where a blob is.
func (w *Warehouse) Path(sha string) string { return w.root + Dir + "/" + sha }

func (w *Warehouse) files(ctx context.Context) (icebergio.IO, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fs != nil {
		return w.fs, nil
	}
	fs, err := w.open(context.WithoutCancel(ctx))
	if err != nil {
		return nil, fmt.Errorf("open the warehouse for model blobs: %w", err)
	}
	w.fs = fs
	return fs, nil
}

func validSHA(sha string) error {
	if len(sha) != 64 || strings.Trim(sha, "0123456789abcdef") != "" {
		return fmt.Errorf("%q is not a model blob's SHA-256", sha)
	}
	return nil
}

func (w *Warehouse) Put(ctx context.Context, sha string, data []byte) error {
	if err := validSHA(sha); err != nil {
		return err
	}
	fs, err := w.files(ctx)
	if err != nil {
		return err
	}
	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return errors.New("the warehouse's file system cannot write model blobs")
	}
	err = wfs.WriteFile(w.Path(sha), data)
	if dir, local := localDir(w.root + Dir); local && errors.Is(err, iofs.ErrNotExist) {
		// A warehouse on a local disk has directories, which object
		// storage does not.
		if err = os.MkdirAll(dir, 0o750); err == nil {
			err = wfs.WriteFile(w.Path(sha), data)
		}
	}
	if err != nil {
		return fmt.Errorf("write model blob %s: %w", sha, err)
	}
	return nil
}

// localDir returns the directory path names when it is on a local disk.
func localDir(path string) (string, bool) {
	if p, ok := strings.CutPrefix(path, "file://"); ok {
		return p, true
	}
	return path, !strings.Contains(path, "://")
}

func (w *Warehouse) Get(ctx context.Context, sha string) ([]byte, error) {
	if err := validSHA(sha); err != nil {
		return nil, err
	}
	fs, err := w.files(ctx)
	if err != nil {
		return nil, err
	}
	f, err := fs.Open(w.Path(sha))
	if notFound(err) {
		return nil, ErrNoBlob
	}
	if err != nil {
		return nil, fmt.Errorf("read model blob %s: %w", sha, err)
	}
	defer f.Close() //nolint:errcheck
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read model blob %s: %w", sha, err)
	}
	return data, nil
}

func (w *Warehouse) Delete(ctx context.Context, sha string) error {
	if err := validSHA(sha); err != nil {
		return err
	}
	fs, err := w.files(ctx)
	if err != nil {
		return err
	}
	if err := fs.Remove(w.Path(sha)); err != nil && !notFound(err) {
		return fmt.Errorf("delete model blob %s: %w", sha, err)
	}
	return nil
}

func notFound(err error) bool {
	return err != nil && (errors.Is(err, iofs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound)
}

// Memory keeps blobs in this process, for a server without a warehouse
// (and so without workers) and for tests.
type Memory struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

func NewMemory() *Memory { return &Memory{blobs: map[string][]byte{}} }

func (m *Memory) Put(_ context.Context, sha string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[sha] = append([]byte(nil), data...)
	return nil
}

func (m *Memory) Get(_ context.Context, sha string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[sha]
	if !ok {
		return nil, ErrNoBlob
	}
	return b, nil
}

func (m *Memory) Delete(_ context.Context, sha string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, sha)
	return nil
}

// Len is the number of blobs kept.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.blobs)
}
