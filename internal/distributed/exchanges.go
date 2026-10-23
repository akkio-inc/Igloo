package distributed

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/akkio-inc/igloo/internal/config"
)

// Exchanges are files in the warehouse's object storage, under
// _igloo_shuffle/<statement>/ (see Stage). A statement removes its own
// once its stages have run, in the background, so that its rows do not
// wait for it. A statement whose replica stopped or was killed first
// leaves them, so every replica sweeps the exchanges of statements that no
// longer run, a minute after it starts and hourly (see sweep).
const (
	// staleExchanges is how long nothing must have been written under a
	// statement's exchanges before a sweep removes them. A statement keeps
	// writing under its own while it runs: its stages' files, and a
	// heartbeat every heartbeatEvery, however long its last stages take.
	staleExchanges = 6 * time.Hour
	heartbeatEvery = 30 * time.Minute
	// heartbeatFile is the heartbeat's file, under a statement's exchanges.
	heartbeatFile = "running"
	sweepDelay    = time.Minute
	sweepEvery    = time.Hour
	// removeTimeout bounds how long removing a statement's exchanges may
	// take; a sweep removes what is left. Close waits closeGrace for
	// removals still running before it stops them.
	removeTimeout = 10 * time.Minute
	closeGrace    = 10 * time.Second
	// removeConcurrency bounds the deletes in flight: object stores delete
	// an object a request.
	removeConcurrency = 32
)

// Storage is the object storage that exchanges are written to: the
// warehouse, with Igloo's own credentials, as workers write to it.
type Storage interface {
	// List calls fn with every file under dir, by path, and when it was
	// last written.
	List(ctx context.Context, dir string, fn func(path string, written time.Time) error) error
	// Remove deletes files. A file that is gone already is not an error.
	Remove(ctx context.Context, paths []string) error
	// Write writes a small file.
	Write(ctx context.Context, path string, data []byte) error
}

// NewStorage returns the Storage of fsys, a file system of the warehouse
// (catalog.FS).
func NewStorage(fsys icebergio.IO) Storage {
	if b, ok := fsys.(bucket); ok {
		return bucketStorage{b}
	}
	return fsStorage{fsys}
}

// bucket is a blob store's bucket, which iceberg-go's file systems for
// object storage embed. Listing a prefix of it flat takes a request per
// thousand files, where walking it as a tree of directories takes one per
// directory: the two exchanges of a report over 32 partitions were 173
// directories, listed one after another before a file was deleted.
type bucket interface {
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string) error
	WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error
}

type bucketStorage struct{ b bucket }

// objectKey returns path's key in its bucket, and path as a URL.
func objectKey(path string) (string, *url.URL, error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", nil, err
	}
	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", nil, fmt.Errorf("%s names no object", path)
	}
	return key, u, nil
}

func (s bucketStorage) List(ctx context.Context, dir string, fn func(string, time.Time) error) error {
	prefix, u, err := objectKey(strings.TrimSuffix(dir, "/"))
	if err != nil {
		return err
	}
	it := s.b.List(&blob.ListOptions{Prefix: prefix + "/"})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if obj.IsDir {
			continue
		}
		p := *u
		p.Path, p.RawPath = "/"+obj.Key, ""
		if err := fn(p.String(), obj.ModTime); err != nil {
			return err
		}
	}
}

func (s bucketStorage) Remove(ctx context.Context, paths []string) error {
	return removeEach(paths, func(path string) error {
		key, _, err := objectKey(path)
		if err != nil {
			return err
		}
		if err := s.b.Delete(ctx, key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return err
		}
		return nil
	})
}

func (s bucketStorage) Write(ctx context.Context, path string, data []byte) error {
	key, _, err := objectKey(path)
	if err != nil {
		return err
	}
	return s.b.WriteAll(ctx, key, data, nil)
}

// removeEach calls remove for every path, removeConcurrency at a time,
// and returns their errors.
func removeEach(paths []string, remove func(string) error) error {
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	sem := make(chan struct{}, removeConcurrency)
	for _, p := range paths {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := remove(p); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// fsStorage is the Storage of a file system that is not a blob store's:
// the local disk of tests.
type fsStorage struct{ fsys icebergio.IO }

func (s fsStorage) List(_ context.Context, dir string, fn func(string, time.Time) error) error {
	lister, ok := s.fsys.(icebergio.ListableIO)
	if !ok {
		return errors.New("the warehouse's storage cannot list files")
	}
	err := lister.WalkDir(strings.TrimSuffix(dir, "/"), func(path string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return fn(path, info.ModTime())
	})
	if errors.Is(err, iofs.ErrNotExist) {
		return nil
	}
	return err
}

// Remove deletes paths, then the directories of exchanges they leave
// empty, as object stores have none.
func (s fsStorage) Remove(_ context.Context, paths []string) error {
	err := removeEach(paths, func(path string) error {
		if err := s.fsys.Remove(path); err != nil && !errors.Is(err, iofs.ErrNotExist) {
			return err
		}
		return nil
	})
	dirs := map[string]bool{}
	for _, p := range paths {
		for d := filepath.Dir(p); strings.Contains(d, "/"+config.ShuffleDir+"/"); d = filepath.Dir(d) {
			dirs[d] = true
		}
	}
	// The deepest first; a directory something else is still in stays.
	for _, d := range slices.SortedFunc(maps.Keys(dirs), func(a, b string) int { return len(b) - len(a) }) {
		_ = os.Remove(strings.TrimPrefix(d, "file://"))
	}
	return err
}

func (s fsStorage) Write(_ context.Context, path string, data []byte) error {
	w, ok := s.fsys.(icebergio.WriteFileIO)
	if !ok {
		return errors.New("the warehouse's storage cannot write files")
	}
	f, err := w.Create(path)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Close())
}

// hold marks a statement's exchanges, under root, as in use until the
// returned function is called: sweeps leave them, and a heartbeat writes
// under them every heartbeatEvery, so that the sweeps of other replicas,
// which cannot know the statement runs, find something recent.
func (co *Coordinator) hold(qid, root string) (release func()) {
	co.runningMu.Lock()
	co.running[qid] = true
	co.runningMu.Unlock()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(co.heartbeat)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				if err := co.opts.Storage.Write(ctx, root+"/"+heartbeatFile, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
					co.log.Warn("could not write the heartbeat of a statement's exchanges", "query", qid, "err", err)
				}
				cancel()
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// removeLater removes a statement's exchanges, under root, in the
// background, and then lets sweeps have them.
func (co *Coordinator) removeLater(qid, root string) {
	co.removals.Go(func() {
		defer func() {
			co.runningMu.Lock()
			delete(co.running, qid)
			co.runningMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(co.removeCtx, removeTimeout)
		defer cancel()
		var paths []string
		err := co.opts.Storage.List(ctx, root, func(path string, _ time.Time) error {
			paths = append(paths, path)
			return nil
		})
		if err == nil {
			err = co.opts.Storage.Remove(ctx, paths)
		}
		if err != nil {
			co.log.Warn("could not remove exchanged rows", "dir", root, "err", err)
		}
	})
}

// sweepLoop sweeps sweepDelay after the coordinator starts, and then
// every sweepEvery, until ctx is done.
func (co *Coordinator) sweepLoop(ctx context.Context) {
	t := time.NewTimer(sweepDelay)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := co.sweep(ctx, time.Now()); err != nil && ctx.Err() == nil {
			co.log.Warn("could not sweep exchanges", "err", err)
		}
		t.Reset(sweepEvery)
	}
}

// sweep removes the exchanges of statements that no longer run: those no
// statement of this replica holds (see hold), under which nothing was
// written for staleExchanges.
func (co *Coordinator) sweep(ctx context.Context, now time.Time) error {
	root := co.opts.Warehouse + config.ShuffleDir
	marker := "/" + config.ShuffleDir + "/"
	files := map[string][]string{}
	newest := map[string]time.Time{}
	err := co.opts.Storage.List(ctx, root, func(path string, written time.Time) error {
		i := strings.LastIndex(path, marker)
		if i < 0 {
			return nil
		}
		qid, _, ok := strings.Cut(path[i+len(marker):], "/")
		if !ok || qid == "" {
			return nil
		}
		files[qid] = append(files[qid], path)
		if written.After(newest[qid]) {
			newest[qid] = written
		}
		return nil
	})
	if err != nil {
		return err
	}
	var errs []error
	for qid, paths := range files {
		co.runningMu.Lock()
		held := co.running[qid]
		co.runningMu.Unlock()
		if held || now.Sub(newest[qid]) < staleExchanges {
			continue
		}
		if err := co.opts.Storage.Remove(ctx, paths); err != nil {
			errs = append(errs, fmt.Errorf("exchanges of %s: %w", qid, err))
			continue
		}
		co.log.Info("removed the exchanges of a statement that no longer runs", "query", qid, "files", len(paths),
			"last_written", newest[qid])
	}
	return errors.Join(errs...)
}
