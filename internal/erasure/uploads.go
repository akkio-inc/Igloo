package erasure

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/uploads"
)

// A request's unfinished uploads. What a process that stopped had uploaded of a
// file is in no listing of files, and can be recovered: on S3 and GCS, whoever may
// write can list an upload's parts and complete it, and on Azure commit a blob's
// staged blocks. So a request aborts those that could hold its rows, and does not
// complete until there are none, or it cannot tell:
//
//   - under its tables' locations, where a write of their rows was going;
//   - under no table's or view's location, as those of tables dropped;
//   - among exchanges (exchangeDir), which hold rows of any table.
//
// Of those, what began by uploadGrace after the latest deletion: a statement that
// began later read nothing deleted. Workers cancel their tasks of statements from
// before it when they are purged, and requests wait for fills from before it, so
// that what goes on past that is another statement's, and is left to finish.
// Others' tables' uploads hold those tables' rows, which a request does not erase.

// uploadGrace is how long after a deletion an upload still counts as one of a
// statement from before it, as workers take a few seconds to hear of a deletion.
const uploadGrace = 5 * time.Minute

// exchangeDir is where statements' exchanges are, under the warehouse.
const exchangeDir = "_igloo_shuffle/"

// suspects returns the unfinished uploads that could hold r's rows, and the store
// of each, by the store it was listed through.
func (s *Service) suspects(ctx context.Context, r *Request, tables []loaded) (map[uploads.Store][]uploads.Upload, error) {
	cut, err := s.uploadCut(ctx, r)
	if err != nil {
		return nil, err
	}
	owned, err := s.ownedLocations(ctx)
	if err != nil {
		return nil, err
	}
	ours := make([]string, 0, len(tables))
	for _, l := range tables {
		ours = append(ours, canonical(l.tbl.Location()))
	}
	// The warehouse's, and those of its tables' that are elsewhere.
	prefixes := []string{s.opts.Warehouse}
	for _, loc := range ours {
		if !within(loc, s.opts.Warehouse) {
			prefixes = append(prefixes, strings.TrimSuffix(loc, "/")+"/")
		}
	}
	fio := s.newFileIO(tables)
	out := map[uploads.Store][]uploads.Upload{}
	for i, prefix := range slices.Compact(prefixes) {
		var fsys iceio.IO
		if i == 0 {
			fsys, err = fio.warehouseIO(ctx)
		} else {
			fsys, err = fio.forPath(ctx, prefix)
		}
		if err != nil {
			return nil, err
		}
		store, err := s.uploadStore(fsys, prefix)
		if err != nil {
			return nil, unavailable(fmt.Errorf("the unfinished uploads under %s: %w", prefix, err))
		}
		if store == nil {
			continue
		}
		list, err := store.List(ctx, prefix)
		if err != nil {
			return nil, unavailable(fmt.Errorf("list the unfinished uploads under %s: %w", prefix, err))
		}
		for _, u := range list {
			p := canonical(u.Path)
			if !u.Initiated.Before(cut) {
				continue
			}
			if under(p, ours) || within(p, s.opts.Warehouse+exchangeDir) || !under(p, owned) {
				out[store] = append(out[store], u)
			}
		}
	}
	return out, nil
}

// uploadCut is when an upload begins too late to count: uploadGrace after the
// latest deletion of any request, or after this one was made if that is later.
func (s *Service) uploadCut(ctx context.Context, r *Request) (time.Time, error) {
	latest, err := s.opts.Store.LatestDeletion(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if r.Created.After(latest) {
		latest = r.Created
	}
	return latest.Add(uploadGrace), nil
}

func (s *Service) uploadStore(fsys iceio.IO, location string) (uploads.Store, error) {
	if s.opts.Uploads != nil {
		return s.opts.Uploads(fsys, location)
	}
	return uploads.For(fsys, location, s.opts.UploadKeys)
}

// abortUploads aborts the unfinished uploads that could hold r's rows.
func (s *Service) abortUploads(ctx context.Context, r *Request, tables []loaded) error {
	found, err := s.suspects(ctx, r, tables)
	if err != nil {
		return err
	}
	var errs []error
	n := 0
	for store, list := range found {
		for _, u := range list {
			if err := store.Abort(ctx, u); err != nil {
				errs = append(errs, fmt.Errorf("abort the upload of %s: %w", base(u.Path), err))
				continue
			}
			n++
		}
	}
	if n > 0 {
		s.log.Info("erasure aborted unfinished uploads that could hold its rows", "request", r.ID, "uploads", n)
	}
	if err := errors.Join(errs...); err != nil {
		return unavailable(err)
	}
	return nil
}

// uploadsRemaining counts the unfinished uploads that could still hold r's rows.
func (s *Service) uploadsRemaining(ctx context.Context, r *Request, tables []loaded) (int64, error) {
	found, err := s.suspects(ctx, r, tables)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, list := range found {
		n += int64(len(list))
	}
	return n, nil
}
