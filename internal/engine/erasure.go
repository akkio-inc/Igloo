package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/erasure"
)

// ErasureViewer says whether a statement may list erasure requests. ctx is
// the context the statement was prepared with; the returned function is
// called when it runs.
type ErasureViewer func(ctx context.Context) func() bool

type erasureConfig struct {
	list   func(context.Context) ([]*erasure.Request, error)
	viewer ErasureViewer
}

// SetErasure makes igloo.erasure_requests list what list returns, to the
// statements viewer allows: admins and holders of ERASE. Until it is set,
// the view is empty.
func (e *Engine) SetErasure(list func(context.Context) ([]*erasure.Request, error), viewer ErasureViewer) {
	e.erasure.Store(&erasureConfig{list: list, viewer: viewer})
}

// registerErasure defines igloo_erasure_requests() and the view
// igloo.erasure_requests over it: one row per request, with JSON columns
// for what it did where, and no identifiers.
func (e *Engine) registerErasure(ctx context.Context) error {
	// Not closed: closing a DB closes its connector.
	db := sql.OpenDB(e.connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	t, err := newHistoryTypes()
	if err != nil {
		return err
	}
	var columns []duckdb.ColumnInfo
	for _, name := range erasure.Columns {
		typ := t.varchar
		switch {
		case strings.HasSuffix(name, "_at"):
			typ = t.timestamp
		case name == "history_redacted":
			typ = t.bigint
		}
		columns = append(columns, duckdb.ColumnInfo{Name: name, T: typ})
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_erasure_requests", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			c := e.erasure.Load()
			allowed := func() bool { return false }
			if c != nil && c.viewer != nil {
				if v := c.viewer(ctx); v != nil {
					allowed = v
				}
			}
			return &erasureSource{columns: columns, config: c, allowed: allowed}, nil
		},
	})
	if err != nil {
		return err
	}
	schema := ident(e.local) + "." + ident(HistorySchema)
	for _, stmt := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"CREATE OR REPLACE VIEW " + schema + ".erasure_requests AS SELECT * FROM system.main.igloo_erasure_requests()",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// erasureSource lists requests when DuckDB first asks for a row.
type erasureSource struct {
	columns []duckdb.ColumnInfo
	config  *erasureConfig
	allowed func() bool
	rows    [][]any
	loaded  bool
}

func (s *erasureSource) ColumnInfos() []duckdb.ColumnInfo     { return s.columns }
func (s *erasureSource) Cardinality() *duckdb.CardinalityInfo { return nil }
func (s *erasureSource) Init()                                {}

func (s *erasureSource) FillRow(row duckdb.Row) (bool, error) {
	if !s.loaded {
		s.loaded = true
		if s.config == nil || !s.allowed() {
			return false, nil
		}
		requests, err := s.config.list(context.Background())
		if err != nil {
			return false, fmt.Errorf("read erasure requests: %w", err)
		}
		for _, r := range requests {
			s.rows = append(s.rows, erasure.Row(r))
		}
	}
	if len(s.rows) == 0 {
		return false, nil
	}
	r := s.rows[0]
	s.rows = s.rows[1:]
	for i, v := range r {
		if err := row.SetRowValue(i, v); err != nil {
			return false, err
		}
	}
	return true, nil
}

// cacheIDFile keeps the read cache's ID, beside its blocks.
const cacheIDFile = "cache-id"

// CacheID identifies this replica's read cache to erasure requests, which
// wait for every running replica to drop the files they deleted. A cache
// on disk keeps its ID in its directory, so the ID lasts as long as the
// cache does, across restarts; without one, the ID is the process's own.
func (e *Engine) CacheID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	random := hex.EncodeToString(b[:])
	if e.cache == nil {
		return erasure.MemoryCache + random, nil
	}
	file := filepath.Join(filepath.Dir(e.cache.dir), cacheIDFile)
	if id, err := os.ReadFile(file); err == nil && strings.TrimSpace(string(id)) != "" {
		return strings.TrimSpace(string(id)), nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	id := "disk:" + random
	return id, os.WriteFile(file, []byte(id+"\n"), 0o640)
}

// ForgetCachedFiles drops paths from the read cache on disk, and from
// DuckDB's cache of file contents in memory, which can only be emptied
// whole: only when it holds one of them.
func (e *Engine) ForgetCachedFiles(ctx context.Context, paths []string) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	if e.cache != nil {
		for _, p := range paths {
			if _, err := conn.ExecContext(ctx, "SELECT cache_httpfs_clear_cache_for_file("+literal(p)+")", nil); err != nil {
				return fmt.Errorf("drop %s from the read cache: %w", filepath.Base(p), err)
			}
		}
	}
	rows, err := conn.QueryContext(ctx, "SELECT DISTINCT path FROM duckdb_external_file_cache()", nil)
	if err != nil {
		return err
	}
	cached := map[string]bool{}
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				rows.Close() //nolint:errcheck
				return err
			}
			break
		}
		if p, ok := dest[0].(string); ok {
			cached[p] = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	held := false
	for _, p := range paths {
		held = held || cached[p]
	}
	if !held {
		return nil
	}
	for _, stmt := range []string{"SET GLOBAL enable_external_file_cache = false", "SET GLOBAL enable_external_file_cache = true"} {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return err
		}
	}
	return nil
}
