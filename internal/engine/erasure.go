package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
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

// cacheIDFile keeps the read cache's ID, beside its blocks, with the
// machine it belongs to.
const cacheIDFile = "cache-id"

// CacheID identifies this replica's read cache to erasure requests, which
// wait for every running replica to empty its cache after they delete
// files. A cache on disk keeps its ID in its directory, so the ID lasts as
// long as the cache does, across restarts, but only on the machine that
// made it: a disk copied with a VM image to another machine gets a new
// one, and is emptied, so the two replicas are told apart. Without a cache
// on disk, the ID is the process's own.
func (e *Engine) CacheID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	random := hex.EncodeToString(b[:])
	if e.cache == nil {
		return erasure.MemoryCache + random, nil
	}
	machine := machineID()
	file := filepath.Join(filepath.Dir(e.cache.dir), cacheIDFile)
	saved, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if id, on, ok := strings.Cut(strings.TrimSpace(string(saved)), " "); ok && id != "" && on == machine {
		return id, nil
	}
	if len(saved) > 0 {
		// Blocks from another machine, whose erasures this one's ID has
		// not seen: they go.
		if err := e.EmptyCache(context.Background()); err != nil {
			return "", err
		}
	}
	id := "disk:" + random
	return id, os.WriteFile(file, []byte(id+" "+machine+"\n"), 0o640)
}

// machineID tells this machine from others: its host name and, where
// there is one, its machine ID, which a cloned VM gets anew.
func machineID() string {
	host, _ := os.Hostname()
	id, _ := os.ReadFile("/etc/machine-id")
	return strings.ReplaceAll(host, " ", "_") + "/" + strings.TrimSpace(string(id))
}

// EmptyCache drops everything the read cache on disk holds, and DuckDB's
// cache of file contents in memory.
func (e *Engine) EmptyCache(ctx context.Context) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	if e.cache != nil {
		if _, err := conn.ExecContext(ctx, "SELECT cache_httpfs_clear_cache()", nil); err != nil {
			return fmt.Errorf("empty the read cache: %w", err)
		}
	}
	for _, stmt := range []string{"SET GLOBAL enable_external_file_cache = false", "SET GLOBAL enable_external_file_cache = true"} {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return err
		}
	}
	return nil
}
