package pgwire

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

// Erasure requests and bitmap indexes. A bitmap holds an erased person's
// id, which their dictionary entry gave them alone, after the request has
// deleted their rows: an index's bitmaps are of a snapshot from before the
// delete, and the index table's own old snapshots keep older ones. An
// erasure request (package erasure) has each index of a domain whose
// bitmaps, in any snapshot of its table, hold one of the ids it erases,
// emptied, at the end of its expire step, when no snapshot of any table
// holds the people's values any more:
//
//   - one commit removes every data and delete file from the index table;
//   - a second expires every snapshot of it but that empty one, and the
//     files only the others referenced are deleted from object storage;
//   - the index is built again, in the background, from its table's
//     current snapshot, which no longer holds the people.
//
// The build's lease keeps a build from writing meanwhile. Verification
// counts the rows, in every snapshot of the indexes' tables, whose bitmaps
// hold one of the ids.

// indexEraseWait is how long erasing waits for an index's build lease.
const indexEraseWait = time.Minute

// BitmapIDSets erases ids from bitmap indexes for erasure requests
// (erasure.IDSets). Its server is set once made, after the erasure
// service; until then requests try again later.
type BitmapIDSets struct {
	srv atomic.Pointer[Server]
}

// Bind sets the server whose bitmap indexes b erases from.
func (b *BitmapIDSets) Bind(s *Server) { b.srv.Store(s) }

func (b *BitmapIDSets) server() (*Server, error) {
	s := b.srv.Load()
	if s == nil {
		return nil, errors.New("the SQL server is not ready to erase from bitmap indexes")
	}
	return s, nil
}

// Erase empties, and has built again, each of domain's indexes whose
// bitmaps hold one of ids, with the snapshots and files that held them.
func (b *BitmapIDSets) Erase(ctx context.Context, domain string, ids []int64) error {
	s, err := b.server()
	if err != nil {
		return err
	}
	return s.forDomainIndexes(ctx, domain, func(x iddomain.BitmapIndex) error {
		_, err := s.eraseIndex(ctx, x, ids)
		return err
	})
}

// Holding counts the rows of domain's indexes' tables, in any snapshot,
// whose bitmaps hold one of ids.
func (b *BitmapIDSets) Holding(ctx context.Context, domain string, ids []int64) (int64, error) {
	s, err := b.server()
	if err != nil {
		return 0, err
	}
	var n int64
	err = s.forDomainIndexes(ctx, domain, func(x iddomain.BitmapIndex) error {
		tbl, err := s.ddl.LoadTable(ctx, iddomain.IndexSchema, x.Name)
		if err != nil {
			return ignoreGone(err)
		}
		k, err := s.indexHolding(ctx, x, tbl, ids)
		n += k
		return err
	})
	return n, err
}

// forDomainIndexes calls fn with each bitmap index of domain's members.
func (s *Server) forDomainIndexes(ctx context.Context, domain string, fn func(iddomain.BitmapIndex) error) error {
	if s.ddl == nil || s.catalogName() == "" || s.idDomains == nil {
		return nil
	}
	snap, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	for _, x := range snap.Indexes {
		m := snap.MemberOf(x.Schema, x.Table, x.Column)
		if m == nil || !strings.EqualFold(m.Domain, domain) {
			continue
		}
		if err := fn(x); err != nil {
			return fmt.Errorf("bitmap index %s: %w", x.Name, err)
		}
	}
	return nil
}

// ignoreGone is err, unless it says the index's table is gone, which holds
// nothing then.
func ignoreGone(err error) error {
	if errors.Is(err, ddl.ErrNotFound) {
		return nil
	}
	return err
}

// idsBitmap is SQL for a bitmap of ids.
func idsBitmap(ids []int64) string {
	vals := make([]string, len(ids))
	for i, id := range ids {
		vals[i] = strconv.FormatInt(id, 10)
	}
	return "(SELECT rb_agg(CAST(i AS UINTEGER)) FROM (SELECT unnest([" + strings.Join(vals, ", ") + "]) AS i))"
}

// indexHolding counts the rows of x's table tbl, in each of its snapshots,
// whose bitmaps hold one of ids.
func (s *Server) indexHolding(ctx context.Context, x iddomain.BitmapIndex, tbl *table.Table, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	conn.SetBindHold(s.engine.CatalogShare)
	var n int64
	for _, snap := range tbl.Metadata().Snapshots() {
		rows, err := queryRows(ctx, conn, fmt.Sprintf("SELECT count(*) FROM %s AT (VERSION => %d) WHERE rb_and_cardinality(bm, %s) > 0",
			s.indexTable(x.Name), snap.SnapshotID, idsBitmap(ids)))
		if err != nil {
			return n, err
		}
		if len(rows) == 1 && len(rows[0]) == 1 {
			k, _ := rows[0][0].(int64)
			n += k
		}
	}
	return n, nil
}

// eraseIndex empties x's table, with every snapshot and file before, if
// any snapshot of it holds one of ids, and has x built again; it reports
// whether it did.
func (s *Server) eraseIndex(ctx context.Context, x iddomain.BitmapIndex, ids []int64) (bool, error) {
	if len(ids) == 0 {
		return false, nil
	}
	holder := s.idHolder + "/erase/" + strconv.FormatUint(s.idSeq.Add(1), 10)
	unlock, err := s.idDomains.Lock(ctx, "bitmap index "+x.Name, holder, indexLeaseTTL, indexEraseWait)
	if err != nil {
		return false, err
	}
	defer unlock()
	tbl, err := s.ddl.LoadTable(ctx, iddomain.IndexSchema, x.Name)
	if err != nil {
		return false, ignoreGone(err)
	}
	n, err := s.indexHolding(ctx, x, tbl, ids)
	if err != nil || n == 0 {
		return false, err
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return false, err
	}
	before := tbl
	if tbl.CurrentSnapshot() != nil {
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			return false, err
		}
		if len(tasks) > 0 {
			tx := tbl.NewTransaction()
			rw := tx.NewRewrite(map[string]string{"igloo.operation": "erasure"})
			var old []iceberg.DataFile
			deletes := map[string]bool{}
			for _, t := range tasks {
				old = append(old, t.File)
				for _, d := range append(append(t.DeleteFiles, t.EqualityDeleteFiles...), t.DeletionVectorFiles...) {
					if !deletes[d.FilePath()] {
						deletes[d.FilePath()] = true
						rw.DeleteFile(d)
					}
				}
			}
			rw.Apply(old, nil, nil)
			if err := rw.Commit(ctx); err != nil {
				return false, fmt.Errorf("remove the files: %w", err)
			}
			if tbl, err = tx.CommitWithoutRetry(ctx); err != nil {
				return false, fmt.Errorf("remove the files: %w", err)
			}
		}
	}
	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(table.WithOlderThan(0), table.WithRetainLast(1), table.WithPostCommit(false)); err != nil {
		return false, err
	}
	after, err := tx.CommitWithoutRetry(ctx)
	if err != nil {
		return false, fmt.Errorf("expire the snapshots: %w", err)
	}
	files, err := clone.Unreferenced(fs, before.Metadata(), after.Metadata(), before.Location())
	if err != nil {
		return false, fmt.Errorf("find the files of the expired snapshots: %w", err)
	}
	if err := removeFiles(ctx, fs, files); err != nil {
		return false, fmt.Errorf("delete the files of the expired snapshots: %w", err)
	}
	s.ddl.TableChanged(iddomain.IndexSchema, x.Name)
	s.log.Info("emptied a bitmap index for an erasure request", "index", x.Name, "files", len(files))
	s.indexesChanged(x.Schema, x.Table)
	return true, nil
}

// removeFiles deletes files, in bulk where the storage can. Files already
// gone are not an error.
func removeFiles(ctx context.Context, fs iceio.IO, paths []string) error {
	if bulk, ok := fs.(iceio.BulkRemovableIO); ok {
		done, err := bulk.DeleteFiles(ctx, paths)
		if err == nil {
			return nil
		}
		gone := map[string]bool{}
		for _, d := range done {
			gone[d] = true
		}
		var rest []string
		for _, p := range paths {
			if !gone[p] {
				rest = append(rest, p)
			}
		}
		paths = rest
	}
	var errs []error
	for _, p := range paths {
		if err := fs.Remove(p); err != nil && !errors.Is(err, iofs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
