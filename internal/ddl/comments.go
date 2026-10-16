package ddl

import (
	"context"
	"errors"
	"fmt"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
)

// CommentTarget is what a COMMENT ON statement names.
type CommentTarget struct {
	// Kind is TABLE, VIEW or COLUMN.
	Kind         string
	Schema, Name string
	Column       string
	// Text is the new comment; nil removes it.
	Text *string
}

// Comment sets or removes a comment. COMMENT ON TABLE also works for views,
// as in PostgreSQL.
func (m *Manager) Comment(ctx context.Context, t CommentTarget) error {
	isView := m.IsView(t.Schema, t.Name)
	var err error
	switch {
	case t.Kind == "COLUMN" && isView:
		return errors.New("comments on view columns are not supported")
	case t.Kind == "COLUMN":
		err = m.cat.SetColumnComment(ctx, t.Schema, t.Name, t.Column, t.Text)
	case t.Kind == "VIEW" || isView:
		err = m.cat.SetViewComment(ctx, t.Schema, t.Name, t.Text)
	default:
		err = m.cat.SetTableComment(ctx, t.Schema, t.Name, t.Text)
	}
	if errors.Is(err, icecat.ErrNoSuchTable) || errors.Is(err, icecat.ErrNoSuchView) {
		return fmt.Errorf("relation %q %w", t.Name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	m.invalidateComments()
	return nil
}

// RefreshComments reloads the comments igloo_comments() lists if they are
// older than commentsTTL. Loading reads every table's metadata, so it runs
// only before queries that read comments.
func (m *Manager) RefreshComments(ctx context.Context) error {
	m.commentsMu.Lock()
	defer m.commentsMu.Unlock()
	if !m.commentsAt.IsZero() && time.Since(m.commentsAt) < commentsTTL {
		return nil
	}
	list, err := m.cat.Comments(ctx)
	if err != nil {
		return err
	}
	m.comments.Store(&list)
	m.commentsAt = time.Now()
	return nil
}

// invalidateComments makes the next RefreshComments reload, so a session
// sees its own comments at once.
func (m *Manager) invalidateComments() {
	m.commentsMu.Lock()
	m.commentsAt = time.Time{}
	m.commentsMu.Unlock()
}
