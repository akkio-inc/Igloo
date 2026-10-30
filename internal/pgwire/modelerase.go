package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// Erasure requests and models. A model's blob can hold what it learned of
// a person: a recommender's factors for their id and the items they rated,
// a tree's split on their email as a category, a series' values and last
// steps (a foundation model's context among them). Every model records the
// catalog's tables its training read (models.Model.Tables, from the
// statements CREATE prepared, and the tables the models its input used
// had read). An erasure request (package erasure) has each model trained
// from one of its tables trained again, once the rows are gone:
//
//   - it moves the models' erasure epoch on first, so that a CREATE that
//     read its training data before, which may have read the person's
//     rows, stores no model (models.ErrStale);
//   - it trains each model again from its CREATE statement, as its owner,
//     with the search path and dialect it was created with, as CREATE OR
//     REPLACE would: grants kept, replacing only the model it read, not
//     one its owner replaced or dropped meanwhile;
//   - it deletes the blobs the models had from object storage, and every
//     replica drops the models, and methods' results, it holds in memory
//     (models.Runtime.Purge), which statements prepared before then may no
//     longer name (checkModelCalls).
//
// A model it cannot train again, as when its owner is gone or disabled or
// may no longer read its input, blocks the request until an admin fixes
// it, drops the model, or closes the request, which records the model as
// left.

// ModelRetrainer trains models again for erasure requests (erasure.Models).
// Its server is set once made, after the erasure service; until then
// requests try again later.
type ModelRetrainer struct {
	srv atomic.Pointer[Server]
}

// Bind sets the server whose models m trains again.
func (m *ModelRetrainer) Bind(s *Server) { m.srv.Store(s) }

func (m *ModelRetrainer) server() (*Server, error) {
	s := m.srv.Load()
	if s == nil || s.models == nil {
		return nil, errors.New("the SQL server is not ready to train models again")
	}
	return s, nil
}

func (m *ModelRetrainer) BumpEpoch(ctx context.Context) (int64, error) {
	s, err := m.server()
	if err != nil {
		return 0, err
	}
	return s.models.BumpEpoch(ctx)
}

// stale returns the models whose training read any of tables and began
// before epoch (any, for 0), oldest first: one whose input used another's
// predictions is trained again after it.
func (m *ModelRetrainer) stale(ctx context.Context, s *Server, tables []string, epoch int64) ([]models.Model, error) {
	all, err := s.models.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []models.Model
	for _, md := range all {
		if (epoch == 0 || md.Epoch < epoch) && md.TrainedOn(tables) {
			out = append(out, md)
		}
	}
	slices.SortStableFunc(out, func(a, b models.Model) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out, nil
}

func (m *ModelRetrainer) Stale(ctx context.Context, tables []string, epoch int64) ([]string, error) {
	s, err := m.server()
	if err != nil {
		return nil, err
	}
	stale, err := m.stale(ctx, s, tables, epoch)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(stale))
	for i, md := range stale {
		names[i] = md.Schema + "." + md.Name
	}
	return names, nil
}

func (m *ModelRetrainer) Retrain(ctx context.Context, tables []string, epoch int64) (erasure.Retrained, error) {
	var out erasure.Retrained
	s, err := m.server()
	if err != nil {
		return out, err
	}
	stale, err := m.stale(ctx, s, tables, epoch)
	if err != nil {
		return out, err
	}
	for _, md := range stale {
		out.Blobs = append(out.Blobs, md.Blob)
		err := s.retrainModel(ctx, md)
		switch {
		case err == nil:
			out.Models++
		case errors.Is(err, models.ErrChanged):
			// Its owner replaced it, after the epoch moved on, or dropped
			// it: nothing of it is left to train again.
		case ctx.Err() != nil:
			return out, ctx.Err()
		default:
			out.Failed = append(out.Failed, md.Schema+"."+md.Name+": "+err.Error())
		}
	}
	return out, nil
}

func (m *ModelRetrainer) DropBlobs(ctx context.Context, blobs []string) (int64, error) {
	s, err := m.server()
	if err != nil {
		return 0, err
	}
	b := s.modelRuntime.Blobs()
	if b == nil {
		return 0, nil
	}
	var left int64
	for _, sha := range blobs {
		// Under the blob's lock, as dropBlob deletes blobs: a CREATE may
		// be about to store a model that uses it.
		unlock, err := s.models.LockBlob(ctx, sha)
		if err != nil {
			return left, err
		}
		used, err := s.models.BlobUsed(ctx, sha)
		switch {
		case err != nil:
			unlock()
			return left, err
		case used:
		default:
			if err := b.Delete(ctx, sha); err != nil {
				s.log.Warn("an erasure request could not delete a model's old blob; it tries again", "blob", sha, "err", err)
				left++
			} else {
				s.modelRuntime.Forget(sha)
			}
		}
		unlock()
	}
	return left, nil
}

// retrainModel trains m again from its CREATE statement, as its owner,
// with the search path and dialect it was created with, replacing it only
// if it still has the blob it had (else models.ErrChanged).
func (s *Server) retrainModel(ctx context.Context, m models.Model) error {
	owner, err := s.users.Get(ctx, m.Owner)
	if errors.Is(err, auth.ErrNotFound) {
		return fmt.Errorf("its owner %s no longer exists: an admin can create it again, or drop it", m.Owner)
	}
	if err != nil {
		return err
	}
	if owner.Disabled {
		return fmt.Errorf("its owner %s is disabled (ALTER USER %s LOGIN enables them)", owner.Name, owner.Name)
	}
	st, ok, err := snowflake.ParseModelStatement(m.TrainingSQL)
	switch {
	case err != nil:
		return fmt.Errorf("its CREATE statement no longer reads: %w", err)
	case !ok || st.Verb != "CREATE":
		return errors.New("its CREATE statement no longer reads as one")
	}
	// The model it is, whatever the search path says now; replaced as by
	// CREATE OR REPLACE.
	st.Name = []string{m.Schema, m.Name}
	st.Replace, st.IfNotExists = true, false
	es, err := s.OpenEmbedded(ctx, owner, "erasure: train model "+m.Schema+"."+m.Name+" again")
	if err != nil {
		return err
	}
	defer es.Close() //nolint:errcheck
	sess := es.sess
	if m.Dialect != "" {
		sess.dialect = m.Dialect
	}
	ctx = context.WithValue(ctx, sessionKey{}, sess)
	if m.SearchPath != "" {
		if _, err := sess.conn.ExecContext(ctx, "SET SESSION search_path = "+quoteLiteral(m.SearchPath), nil); err != nil {
			return fmt.Errorf("its search path %s: %w", m.SearchPath, err)
		}
		if s.ddl != nil {
			if err := s.keepViewsOnPath(ctx, sess); err != nil {
				return err
			}
		}
	}
	w := &embeddedWriter{out: &taskResult{}, types: es.types}
	return sess.runCancelable(ctx, m.TrainingSQL, func(ctx context.Context) error {
		return s.createModelAs(ctx, sess, owner, st, m.TrainingSQL, w, m.Blob)
	})
}
