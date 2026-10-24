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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	// removeConcurrency bounds the deletes in flight where an object store
	// deletes an object a request; S3 deletes up to removeBatch objects a
	// request, removeBatches requests at a time.
	removeConcurrency = 32
	removeBatch       = 1000
	removeBatches     = 8
	// removingFile marks a statement's exchanges as being removed: the
	// statement is done, and a sweep removes them whenever it finds them
	// (see removeLater).
	removingFile = "removing"
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

// asBucket is a bucket that gives its driver's client (blob.Bucket.As).
type asBucket interface {
	As(i any) bool
}

// objectDeleter deletes objects in batches, as S3's DeleteObjects does.
type objectDeleter interface {
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
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

// Remove deletes paths in batches of removeBatch where the bucket is S3's,
// and one a request elsewhere. A statement's exchanges at a hundred times
// the data were 55,000 files: deleted one a request, 32 at a time, they
// were still being deleted minutes later when the coordinator restarted.
func (s bucketStorage) Remove(ctx context.Context, paths []string) error {
	var client *s3.Client
	if b, ok := s.b.(asBucket); ok && b.As(&client) && client != nil {
		return removeInBatches(ctx, client, paths, s.removeEach)
	}
	return s.removeEach(ctx, paths)
}

// removeEach deletes paths an object a request.
func (s bucketStorage) removeEach(ctx context.Context, paths []string) error {
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

// removeInBatches deletes paths with d's DeleteObjects, removeBatch keys a
// request in each bucket, removeBatches requests at a time. A batch the
// store will not delete so, as an S3-compatible store without
// DeleteObjects answers, is deleted by each, an object a request.
func removeInBatches(ctx context.Context, d objectDeleter, paths []string, each func(context.Context, []string) error) error {
	type batch struct {
		bucket string
		keys   []string
		paths  []string
	}
	var batches []*batch
	open := map[string]*batch{}
	var errs []error
	for _, p := range paths {
		key, u, err := objectKey(p)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		b := open[u.Host]
		if b == nil || len(b.keys) == removeBatch {
			b = &batch{bucket: u.Host}
			open[u.Host] = b
			batches = append(batches, b)
		}
		b.keys, b.paths = append(b.keys, key), append(b.paths, p)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, removeBatches)
	for _, b := range batches {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			ids := make([]s3types.ObjectIdentifier, len(b.keys))
			for i, k := range b.keys {
				ids[i] = s3types.ObjectIdentifier{Key: aws.String(k)}
			}
			out, err := d.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(b.bucket),
				Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)}})
			var failed []error
			switch {
			case err != nil && ctx.Err() == nil:
				if err := each(ctx, b.paths); err != nil {
					failed = append(failed, err)
				}
			case err != nil:
				failed = append(failed, err)
			default:
				for _, e := range out.Errors {
					if aws.ToString(e.Code) != "NoSuchKey" {
						failed = append(failed, fmt.Errorf("delete %s: %s: %s", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message)))
					}
				}
			}
			mu.Lock()
			errs = append(errs, failed...)
			mu.Unlock()
		})
	}
	wg.Wait()
	return errors.Join(errs...)
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
// background, and then lets sweeps have them. It marks them as being
// removed first (removingFile), so that if the coordinator stops before
// they are gone, the next sweep of any replica, a minute after it starts,
// removes the rest rather than waiting staleExchanges for them: a
// statement's 478 GB of exchanges outlived the coordinator that was
// removing them.
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
		if err == nil && len(paths) > 0 {
			err = co.removeMarked(ctx, root, paths)
		}
		if err != nil {
			co.log.Warn("could not remove exchanged rows", "dir", root, "err", err)
		}
	})
}

// removeMarked removes paths, the files of a statement's exchanges under
// root, marking them as being removed until the rest are gone.
func (co *Coordinator) removeMarked(ctx context.Context, root string, paths []string) error {
	marker := root + "/" + removingFile
	if !slices.Contains(paths, marker) {
		if err := co.opts.Storage.Write(ctx, marker, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
			co.log.Debug("could not mark exchanges as being removed", "dir", root, "err", err)
		}
	}
	rest := slices.DeleteFunc(slices.Clone(paths), func(p string) bool { return p == marker })
	if err := co.opts.Storage.Remove(ctx, rest); err != nil {
		return err
	}
	return co.opts.Storage.Remove(ctx, []string{marker})
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
// written for staleExchanges, or whose removal began (removingFile).
func (co *Coordinator) sweep(ctx context.Context, now time.Time) error {
	root := co.opts.Warehouse + config.ShuffleDir
	marker := "/" + config.ShuffleDir + "/"
	files := map[string][]string{}
	newest := map[string]time.Time{}
	roots := map[string]string{}
	removing := map[string]bool{}
	err := co.opts.Storage.List(ctx, root, func(path string, written time.Time) error {
		i := strings.LastIndex(path, marker)
		if i < 0 {
			return nil
		}
		qid, rest, ok := strings.Cut(path[i+len(marker):], "/")
		if !ok || qid == "" {
			return nil
		}
		files[qid] = append(files[qid], path)
		roots[qid] = path[:i+len(marker)] + qid
		if rest == removingFile {
			removing[qid] = true
		}
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
		if held || !removing[qid] && now.Sub(newest[qid]) < staleExchanges {
			continue
		}
		if err := co.removeMarked(ctx, roots[qid], paths); err != nil {
			errs = append(errs, fmt.Errorf("exchanges of %s: %w", qid, err))
			continue
		}
		co.log.Info("removed the exchanges of a statement that no longer runs", "query", qid, "files", len(paths),
			"last_written", newest[qid], "removal_resumed", removing[qid])
	}
	return errors.Join(errs...)
}
