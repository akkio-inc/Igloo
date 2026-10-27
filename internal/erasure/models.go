package erasure

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// Models are the models Igloo trained from tables' rows (package models,
// docs/ml-functions.md), whose blobs can hold what they learned of a
// person: a recommender's factors for their id and the items they rated, a
// tree's split on their email as a category, a series' values and last
// steps. A request has each model whose training read one of its tables
// trained again, from its CREATE statement and as its owner, once the rows
// are gone (its models step, after history), and the blobs the models had
// deleted from object storage. It completes only once none is left: a
// model it cannot train again, because its owner is gone or may no longer
// read its input, say, blocks it (see Service.Close).
type Models interface {
	// BumpEpoch moves the models' erasure epoch on and returns the new one:
	// a CREATE that read its training data before, which may have read the
	// person's rows, then stores no model.
	BumpEpoch(ctx context.Context) (int64, error)
	// Retrain trains again each model whose training read any of tables,
	// or what Igloo could not name, or did not record what it read, and
	// began before epoch, oldest first, as CREATE OR REPLACE would: grants
	// kept. A model replaced or dropped meanwhile is left as it is. It
	// returns how many it trained again, the blobs the models had before,
	// and those it could not train again, with why, which are an error.
	Retrain(ctx context.Context, tables []string, epoch int64) (Retrained, error)
	// Stale names the models whose training read any of tables (as for
	// Retrain) and began before epoch: those that may hold what they
	// learned of the person's rows.
	Stale(ctx context.Context, tables []string, epoch int64) ([]string, error)
	// DropBlobs deletes from object storage each of blobs that no model
	// has, and returns how many of those it could not delete.
	DropBlobs(ctx context.Context, blobs []string) (int64, error)
}

// Retrained is what Models.Retrain did.
type Retrained struct {
	Models int
	// Blobs are the SHA-256s of the blobs the models had before.
	Blobs []string
	// Failed are the models it could not train again: schema.name: why.
	Failed []string
}

// retrainModels is the models step: it moves the models' epoch on, once a
// round, so that no CREATE that read the person's rows before stores its
// model; trains again every model trained from the request's tables
// before; deletes the blobs they had; and has every replica drop the
// models and results it holds in memory (see the cache step).
func (s *Service) retrainModels(ctx context.Context, r *Request) error {
	m := s.opts.Models
	if m == nil {
		return nil
	}
	if r.ModelEpoch == 0 {
		e, err := m.BumpEpoch(ctx)
		if err != nil {
			return fmt.Errorf("move the models' erasure epoch on: %w", err)
		}
		r.ModelEpoch = e
		if err := s.save(ctx, r); err != nil {
			return err
		}
	}
	res, err := m.Retrain(ctx, derivedTables(r), r.ModelEpoch)
	r.ModelsRetrained += res.Models
	for _, b := range res.Blobs {
		if !slices.Contains(r.ModelBlobs, b) {
			r.ModelBlobs = append(r.ModelBlobs, b)
		}
	}
	// Why a training failed can quote a value, as an error reading a row.
	r.ModelsFailed = nil
	for _, f := range res.Failed {
		r.ModelsFailed = append(r.ModelsFailed, scrub(r, f))
	}
	res.Failed = r.ModelsFailed
	if res.Models > 0 || len(res.Blobs) > 0 {
		// Replicas drop the models they hold in memory, the old ones too,
		// once they were replaced (see the cache step).
		r.DeletedThrough = s.now()
	}
	// The blobs of the models trained again go, whatever became of the
	// others.
	var dropErr error
	if len(r.ModelBlobs) > 0 {
		left, err := m.DropBlobs(ctx, r.ModelBlobs)
		switch {
		case err != nil:
			dropErr = fmt.Errorf("delete the blobs of the models trained again: %w", err)
		case left > 0:
			dropErr = fmt.Errorf("could not delete %d blobs of the models trained again from object storage", left)
		}
	}
	switch {
	case len(res.Failed) > 0:
		s.log.Error("an erasure request could not train again models trained from its tables, which may hold what they learned of the "+
			"person", "request", r.ID, "models", strings.Join(res.Failed, "; "))
		return terminal(fmt.Errorf("it could not train again %d models trained from its tables, which may hold what they learned of the "+
			"person: %s; once each can be trained again (or is dropped), it goes on, or an admin can close it (CLOSE ERASURE REQUEST)",
			len(res.Failed), strings.Join(res.Failed, "; ")))
	case err != nil:
		return fmt.Errorf("train again the models trained from its tables: %w", err)
	}
	return dropErr
}

// modelsRemaining counts, for verification, the models trained from the
// request's tables before its epoch, and the blobs they had that object
// storage still has.
func (s *Service) modelsRemaining(ctx context.Context, r *Request) (int64, error) {
	m := s.opts.Models
	if m == nil {
		return 0, nil
	}
	stale, err := m.Stale(ctx, derivedTables(r), r.ModelEpoch)
	if err != nil {
		return 0, err
	}
	n := int64(len(stale))
	if len(r.ModelBlobs) > 0 {
		left, err := m.DropBlobs(ctx, r.ModelBlobs)
		if err != nil {
			return 0, err
		}
		n += left
	}
	return n, nil
}

// leaveModels settles, for a request that is to be closed, the models
// trained from its tables: the blobs of those it trained again go, and it
// returns the names of those it did not, which may hold what they learned
// of the person and stay.
func (s *Service) leaveModels(ctx context.Context, r *Request) ([]string, error) {
	m := s.opts.Models
	if m == nil {
		return nil, nil
	}
	if len(r.ModelBlobs) > 0 {
		left, err := m.DropBlobs(ctx, r.ModelBlobs)
		if err != nil {
			return nil, fmt.Errorf("the request could not delete the blobs of the models it trained again: %w", err)
		}
		if left > 0 {
			return nil, fmt.Errorf("the request could not delete %d blobs of the models it trained again from object storage", left)
		}
	}
	return m.Stale(ctx, derivedTables(r), r.ModelEpoch)
}
