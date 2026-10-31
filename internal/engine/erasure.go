package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

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

// erasureViewer returns whether the statement prepared with ctx may list
// erasure requests, asked when it runs: admins and holders of ERASE.
func (e *Engine) erasureViewer(ctx context.Context) func() bool {
	if c := e.erasure.Load(); c != nil && c.viewer != nil {
		if v := c.viewer(ctx); v != nil {
			return v
		}
	}
	return func() bool { return false }
}

// registerErasure defines igloo_erasure_requests() and the view
// igloo.erasure_requests over it: one row per request, with JSON columns
// for what it did where, and no identifiers.
func (e *Engine) registerErasure(ctx context.Context) error {
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()

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
			return &erasureSource{columns: columns, config: e.erasure.Load(), allowed: e.erasureViewer(ctx)}, nil
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

// cacheIDFile keeps the read cache's ID and purge token, beside its
// blocks, on the same disk.
const cacheIDFile = "cache-id"

// CacheIdentity identifies this replica's read cache to erasure requests,
// which wait for every running replica to empty its cache after they
// delete files (see erasure.Purger). A cache on disk keeps its ID, and the
// purge token it was given when last emptied, in a file in its directory,
// so they last as long as the disk does: across restarts, and across a
// container made again on the same volume, whose host name and machine ID
// are new each time. save writes a new ID and token there. The purger,
// not the machine, tells copies of a disk apart (see erasure.Purger).
// Without a cache on disk, the ID is the process's own and save is nil.
//
// A file of the format before tokens, "disk:<id> <machine>", keeps its ID
// on the machine it names, with a new token; on another machine the cache
// is emptied and gets a new ID, as before.
func (e *Engine) CacheIdentity() (id, token string, save func(id, token string) error, err error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", "", nil, err
	}
	random := hex.EncodeToString(b[:])
	if e.cache == nil {
		return erasure.MemoryCache + random, "", nil, nil
	}
	file := filepath.Join(filepath.Dir(e.cache.dir), cacheIDFile)
	save = func(id, token string) error {
		tmp := file + ".new"
		if err := os.WriteFile(tmp, []byte(id+" "+token+"\n"), 0o640); err != nil {
			return err
		}
		return os.Rename(tmp, file)
	}
	saved, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", nil, err
	}
	fields := strings.Fields(string(saved))
	switch {
	case len(fields) == 2 && strings.HasPrefix(fields[0], "disk:") && strings.HasPrefix(fields[1], "t:"):
		return fields[0], fields[1], save, nil
	case len(fields) == 2 && strings.HasPrefix(fields[0], "disk:") && fields[1] == machineID():
		// Before tokens, on the machine that made it.
		token = "t:" + random
		return fields[0], token, save, save(fields[0], token)
	case len(saved) > 0:
		// Blocks of a cache whose erasures this one's ID has not seen:
		// they go.
		if err := e.EmptyCache(context.Background()); err != nil {
			return "", "", nil, err
		}
	}
	var t [12]byte
	if _, err := rand.Read(t[:]); err != nil {
		return "", "", nil, err
	}
	id, token = "disk:"+random, "t:"+hex.EncodeToString(t[:])
	return id, token, save, save(id, token)
}

// erasureMarkFile keeps, beside a worker's read cache on disk, the latest
// deletion of erasure requests the cache was purged of (see ErasureMark).
const erasureMarkFile = "erased-through"

// ErasureMark returns, for a worker with a read cache on disk, the latest
// deletion of erasure requests its cache was purged of, as kept beside the
// cache, so that it outlives the worker as the cache does, and what keeps
// a new one (see package distributed's erasure.go). Without a cache on
// disk, or with a mark that cannot be read, it is zero, so the worker
// purges before its first task; save is nil without a cache on disk.
func (e *Engine) ErasureMark() (through time.Time, save func(time.Time) error, err error) {
	if e.cache == nil {
		return time.Time{}, nil, nil
	}
	file := filepath.Join(filepath.Dir(e.cache.dir), erasureMarkFile)
	save = func(t time.Time) error {
		tmp := file + ".new"
		if err := os.WriteFile(tmp, []byte(t.UTC().Format(time.RFC3339Nano)+"\n"), 0o640); err != nil {
			return err
		}
		return os.Rename(tmp, file)
	}
	b, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return time.Time{}, nil, err
	}
	if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b))); err == nil {
		through = t
	}
	return through, save, nil
}

// machineID tells this machine from others, as the cache's ID file named
// it before purge tokens: its host name and, where there is one, its
// machine ID.
func machineID() string {
	host, _ := os.Hostname()
	id, _ := os.ReadFile("/etc/machine-id")
	return strings.ReplaceAll(host, " ", "_") + "/" + strings.TrimSpace(string(id))
}

// OnEmptyCache has EmptyCache call purge too: it empties what else the
// process keeps of rows, as the SQL server's result cache.
func (e *Engine) OnEmptyCache(purge func()) {
	e.purgeMu.Lock()
	defer e.purgeMu.Unlock()
	e.purges = append(e.purges, purge)
}

// EmptyCache drops everything the read cache on disk holds, and DuckDB's
// cache of file contents in memory; the models, pinned ones too, and
// methods' results the process holds, which can hold what they learned
// from rows an erasure request erased (see models.Runtime.Purge); and
// whatever else OnEmptyCache was given, as kept query results.
func (e *Engine) EmptyCache(ctx context.Context) error {
	if e.models != nil {
		e.models.Purge()
	}
	e.purgeMu.Lock()
	purges := slices.Clone(e.purges)
	e.purgeMu.Unlock()
	for _, purge := range purges {
		purge()
	}
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
