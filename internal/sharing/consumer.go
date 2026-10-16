package sharing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// External tables. Each is a view in DuckDB's own database (memory.s.t for
// the external table s.t) over the shared table's Parquet files, read
// through the Proxy. Before a statement that names one runs, Igloo makes
// sure the view lists the table's current version: it asks the provider
// for the version at most every checkInterval, and lists the files again
// when the version changed or the provider's URLs are about to expire. A
// table read only through views, whose statements do not name it, is kept
// current in the background while DuckDB reads it.

// Engine is the part of the query engine external tables need;
// *engine.Engine implements it.
type Engine interface {
	Connect(ctx context.Context) (*duckdb.Conn, error)
	LocalDatabase() string
	Catalog() string
}

const (
	// checkInterval is how often statements on an external table make
	// Igloo ask the provider whether the table has a new version.
	checkInterval = 15 * time.Second
	// urlMargin is how long before the provider's file URLs expire Igloo
	// lists the files again.
	urlMargin = 10 * time.Minute
	// syncInterval is how often a replica picks up providers and
	// external tables that other replicas created or dropped, and keeps
	// tables in use current.
	syncInterval = 10 * time.Second
	// activeWindow is how recently DuckDB must have read a table's files
	// for the background refresh to keep it current.
	activeWindow = 10 * time.Minute
	// retryInterval spaces out attempts to create a view whose provider
	// failed.
	retryInterval = time.Minute
)

// Consumer keeps external tables.
type Consumer struct {
	store  *Store
	tokens *sealer
	proxy  *Proxy
	eng    Engine
	log    *slog.Logger

	// connMu guards conn, which creates and drops the views.
	connMu sync.Mutex
	conn   *duckdb.Conn

	mu        sync.Mutex
	providers map[string]*client
	tables    map[string]*external
	// known is the external tables by key, for lock-free lookups.
	known atomic.Pointer[map[string]*external]
}

// external is an external table and the state of its view.
type external struct {
	def ExternalTable
	key string

	// mu serializes refreshes.
	mu       sync.Mutex
	version  int64
	checked  time.Time
	attempt  time.Time
	expires  time.Time
	viewSQL  string
	scans    atomic.Int32
	ready    atomic.Bool
	tableKey string
	// columns are the view's, for the data catalog.
	columns atomic.Pointer[[]catalog.ColumnMeta]
}

func tableKey(schema, name string) string {
	return strings.ToLower(schema) + "." + strings.ToLower(name)
}

func newConsumer(ctx context.Context, store *Store, tokens *sealer, proxy *Proxy, eng Engine, log *slog.Logger) (*Consumer, error) {
	conn, err := eng.Connect(ctx)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		store: store, tokens: tokens, proxy: proxy, eng: eng, conn: conn,
		log:       log.With("component", "sharing-consumer"),
		providers: map[string]*client{}, tables: map[string]*external{},
	}
	c.known.Store(&map[string]*external{})
	proxy.refresh = func(ctx context.Context, table string) error {
		if t := (*c.known.Load())[table]; t != nil {
			return c.refresh(ctx, t, true)
		}
		return ErrNotFound
	}
	return c, nil
}

// close releases the consumer's connection.
func (c *Consumer) close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn.Close()
}

// IsExternal reports whether schema.name is an external table, and how many
// scans of Parquet files its view makes: one, or none while the shared
// table is empty.
func (c *Consumer) IsExternal(schema, name string) (scans int, ok bool) {
	t := (*c.known.Load())[tableKey(schema, name)]
	if t == nil {
		return 0, false
	}
	return int(t.scans.Load()), true
}

// CatalogTables describes the external tables for the data catalog: their
// providers, and their columns once their views are made. It says nothing
// of the shares they come from.
func (c *Consumer) CatalogTables() []catalog.TableMeta {
	known := *c.known.Load()
	out := make([]catalog.TableMeta, 0, len(known))
	for _, t := range known {
		m := catalog.TableMeta{Schema: t.def.Schema, Name: t.def.Name, Provider: t.def.Provider, Rows: -1, Bytes: -1, Files: -1}
		if cols := t.columns.Load(); cols != nil {
			m.Columns = *cols
		}
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b catalog.TableMeta) int {
		return strings.Compare(tableKey(a.Schema, a.Name), tableKey(b.Schema, b.Name))
	})
	return out
}

// TableNames lists the external tables in schema.
func (c *Consumer) TableNames(schema string) []string {
	var out []string
	for _, t := range *c.known.Load() {
		if strings.EqualFold(t.def.Schema, schema) {
			out = append(out, t.def.Name)
		}
	}
	return out
}

// Prepare readies the external tables a statement names and points
// references to them at their views.
func (c *Consumer) Prepare(ctx context.Context, query string) (string, error) {
	known := *c.known.Load()
	if len(known) == 0 {
		return query, nil
	}
	words := map[string]bool{}
	for _, w := range sqllex.CodeWords(query) {
		words[strings.ToLower(w)] = true
	}
	for _, w := range sqllex.QuotedIdentifiers(query) {
		words[w] = true
	}
	named := false
	for _, t := range known {
		if !words[strings.ToLower(t.def.Name)] {
			continue
		}
		named = true
		if err := c.refresh(ctx, t, false); err != nil {
			// Any user may name the table, so the error says no more
			// than the provider's name about where it comes from.
			return "", fmt.Errorf("external table %s.%s could not be read from provider %s: %w", t.def.Schema, t.def.Name, t.def.Provider, err)
		}
	}
	if !named {
		return query, nil
	}
	return rewriteRefs(query, c.eng.Catalog(), quoteIdent(c.eng.LocalDatabase()), func(schema, name string) bool {
		return known[tableKey(schema, name)] != nil
	}), nil
}

// refresh makes t's view list the shared table's current files. Unless
// force is set, a view checked within checkInterval whose URLs are good
// for a while yet is left as it is.
func (c *Consumer) refresh(ctx context.Context, t *external, force bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	fresh := t.ready.Load() && now.Before(t.expires.Add(-urlMargin))
	if fresh && !force && now.Sub(t.checked) < checkInterval {
		return nil
	}
	t.attempt = now
	cl, err := c.client(t.def.Provider)
	if err != nil {
		return err
	}
	if fresh && !force {
		// A failed version check falls through to listing the files,
		// which reports the error if it persists.
		if v, err := cl.version(ctx, t.def.Share, t.def.RemoteSchema, t.def.RemoteTable); err == nil && v == t.version {
			t.checked = now
			return nil
		}
	}
	snap, err := cl.query(ctx, t.def.Share, t.def.RemoteSchema, t.def.RemoteTable, now)
	if err != nil {
		return err
	}
	urls := c.proxy.register(t.key, snap.files, snap.partitionColumns)
	sql, scans, err := viewSQL(snap, urls)
	if err != nil {
		return err
	}
	if sql != t.viewSQL || !t.ready.Load() {
		if err := c.createView(ctx, t.def, sql); err != nil {
			return fmt.Errorf("read the shared table's files: %w", err)
		}
		t.viewSQL = sql
		t.scans.Store(int32(scans))
		t.ready.Store(true)
		if cols, err := remoteColumns(snap); err == nil {
			t.columns.Store(&cols)
		}
	}
	t.version, t.checked = snap.version, now
	t.expires = now.Add(24 * time.Hour)
	for _, f := range snap.files {
		if f.Expires.Before(t.expires) {
			t.expires = f.Expires
		}
	}
	return nil
}

// viewSQL is the query of an external table's view: read_parquet over its
// files, or for a table without files an empty query with its columns.
func viewSQL(snap *remoteSnapshot, urls []string) (string, int, error) {
	rs, err := parseRemoteSchema(snap.schema)
	if err != nil {
		return "", 0, err
	}
	if len(rs.Fields) == 0 {
		return "", 0, errors.New("the shared table has no columns")
	}
	if len(urls) == 0 {
		cols := make([]string, len(rs.Fields))
		for i, f := range rs.Fields {
			t, err := duckType(f.Type)
			if err != nil {
				return "", 0, fmt.Errorf("column %s: %w", f.Name, err)
			}
			cols[i] = "CAST(NULL AS " + t + ") AS " + quoteIdent(f.Name)
		}
		return "SELECT " + strings.Join(cols, ", ") + " LIMIT 0", 0, nil
	}
	quoted := make([]string, len(urls))
	for i, u := range urls {
		quoted[i] = quoteLiteral(u)
	}
	opts := ", union_by_name = true, hive_partitioning = false"
	if len(snap.partitionColumns) > 0 {
		types := make([]string, 0, len(snap.partitionColumns))
		for _, col := range snap.partitionColumns {
			t := "VARCHAR"
			for _, f := range rs.Fields {
				if f.Name == col {
					t, err = hiveType(f.Type)
					if err != nil {
						return "", 0, fmt.Errorf("partition column %s: %w", col, err)
					}
				}
			}
			types = append(types, quoteLiteral(col)+": "+t)
		}
		opts = ", union_by_name = true, hive_partitioning = true, hive_types = {" + strings.Join(types, ", ") + "}"
	}
	return "SELECT * FROM read_parquet([" + strings.Join(quoted, ", ") + "]" + opts + ")", 1, nil
}

// remoteColumns are the columns of a shared table's view (see viewSQL),
// with their DuckDB types: read_parquet puts partition columns, which the
// files lack, last.
func remoteColumns(snap *remoteSnapshot) ([]catalog.ColumnMeta, error) {
	rs, err := parseRemoteSchema(snap.schema)
	if err != nil {
		return nil, err
	}
	var cols, partitions []catalog.ColumnMeta
	for _, f := range rs.Fields {
		partition := len(snap.files) > 0 && slices.Contains(snap.partitionColumns, f.Name)
		typeOf := duckType
		if partition {
			typeOf = hiveType
		}
		t, err := typeOf(f.Type)
		if err != nil {
			return nil, err
		}
		if partition {
			partitions = append(partitions, catalog.ColumnMeta{Name: f.Name, Type: t})
		} else {
			cols = append(cols, catalog.ColumnMeta{Name: f.Name, Type: t})
		}
	}
	slices.SortStableFunc(partitions, func(a, b catalog.ColumnMeta) int {
		return slices.Index(snap.partitionColumns, a.Name) - slices.Index(snap.partitionColumns, b.Name)
	})
	return append(cols, partitions...), nil
}

// hiveType is the type of a partition column's values, which the protocol
// writes as text: timestamps without a zone, and binary values as text.
func hiveType(t any) (string, error) {
	switch t {
	case "timestamp", "timestamp_ntz":
		return "TIMESTAMP", nil
	case "binary":
		return "VARCHAR", nil
	}
	return duckType(t)
}

func (c *Consumer) localName(schema, name string) string {
	return quoteIdent(c.eng.LocalDatabase()) + "." + quoteIdent(schema) + "." + quoteIdent(name)
}

func (c *Consumer) createView(ctx context.Context, def ExternalTable, sql string) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if _, err := c.conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdent(c.eng.LocalDatabase())+"."+quoteIdent(def.Schema), nil); err != nil {
		return err
	}
	_, err := c.conn.ExecContext(ctx, "CREATE OR REPLACE VIEW "+c.localName(def.Schema, def.Name)+" AS "+sql, nil)
	return err
}

func (c *Consumer) dropView(ctx context.Context, def ExternalTable) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	_, err := c.conn.ExecContext(ctx, "DROP VIEW IF EXISTS "+c.localName(def.Schema, def.Name), nil)
	return err
}

// client returns the client of a provider.
func (c *Consumer) client(provider string) (*client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl := c.providers[provider]
	if cl == nil {
		return nil, fmt.Errorf("provider %q %w", provider, ErrNotFound)
	}
	return cl, nil
}

func (c *Consumer) newClient(p Provider) *client {
	c.proxy.dialer.allow(endpointHostPort(p.Endpoint))
	return &client{endpoint: p.Endpoint, token: p.Token, http: c.proxy.http}
}

// Sync loads the providers and external tables from the metadata
// database, creating and dropping views to match. changed says whether
// external tables came or went.
func (c *Consumer) Sync(ctx context.Context) (changed bool, err error) {
	providers, sealed, err := c.store.Providers(ctx)
	if err != nil {
		return false, err
	}
	defs, err := c.store.ExternalTables(ctx)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	clients := map[string]*client{}
	for i, p := range providers {
		token, err := c.tokens.open(sealed[i])
		if err != nil {
			c.log.Error("could not decrypt a provider's token", "provider", p.Name, "err", err)
			continue
		}
		p.Token = string(token)
		clients[p.Name] = c.newClient(p)
	}
	c.providers = clients

	var gone []*external
	want := map[string]bool{}
	for _, d := range defs {
		k := tableKey(d.Schema, d.Name)
		want[k] = true
		if t, ok := c.tables[k]; ok && t.def.Remote() == d.Remote() {
			continue
		} else if ok {
			gone = append(gone, t)
		}
		c.tables[k] = &external{def: d, key: k}
		changed = true
	}
	for k, t := range c.tables {
		if !want[k] {
			gone = append(gone, t)
			delete(c.tables, k)
			changed = true
		}
	}
	known := make(map[string]*external, len(c.tables))
	for k, t := range c.tables {
		known[k] = t
	}
	c.known.Store(&known)
	c.mu.Unlock()

	var errs []error
	for _, t := range gone {
		c.proxy.forget(t.key)
		if _, still := known[t.key]; still {
			continue // replaced: its new view replaces the old one
		}
		if err := c.dropView(ctx, t.def); err != nil {
			errs = append(errs, err)
		}
	}
	return changed, errors.Join(errs...)
}

// Run keeps external tables in sync with other replicas, creates views
// whose provider was unreachable, and keeps tables that DuckDB is reading
// current, until ctx is done.
func (c *Consumer) Run(ctx context.Context) {
	tick := time.NewTicker(syncInterval)
	defer tick.Stop()
	for {
		if _, err := c.Sync(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("could not load external tables", "err", err)
		}
		for _, t := range *c.known.Load() {
			if ctx.Err() != nil {
				return
			}
			t.mu.Lock()
			due := !t.ready.Load() && time.Since(t.attempt) > retryInterval ||
				t.ready.Load() && time.Since(c.proxy.lastUsed(t.key)) < activeWindow && time.Since(t.checked) > time.Minute
			t.mu.Unlock()
			if !due {
				continue
			}
			if err := c.refresh(ctx, t, false); err != nil && ctx.Err() == nil {
				c.log.Info("could not refresh an external table", "table", t.def.Schema+"."+t.def.Name, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// --- statements -----------------------------------------------------------

// CreateProvider registers a partner's profile file as provider name, after
// checking that its token works. With replace an existing provider's
// profile is replaced, as when the partner rotates the token.
func (c *Consumer) CreateProvider(ctx context.Context, name, profileText, by string, replace bool) error {
	p, err := parseProfile(profileText)
	if err != nil {
		return err
	}
	p.Name, p.CreatedBy = name, by
	if _, err := c.newClient(p).shares(ctx); err != nil {
		return fmt.Errorf("check the profile with the provider: %w", err)
	}
	sealed, err := c.tokens.seal([]byte(p.Token))
	if err != nil {
		return err
	}
	if err := c.store.PutProvider(ctx, p, sealed, replace); err != nil {
		return err
	}
	_, err = c.Sync(ctx)
	if replace {
		// Make every external table list its files with the new token.
		for _, t := range *c.known.Load() {
			if t.def.Provider == name {
				t.mu.Lock()
				t.checked = time.Time{}
				t.expires = time.Time{}
				t.mu.Unlock()
			}
		}
	}
	return err
}

// DropProvider forgets a provider. External tables must be dropped first.
func (c *Consumer) DropProvider(ctx context.Context, name string) error {
	if err := c.store.DropProvider(ctx, name); err != nil {
		return err
	}
	_, err := c.Sync(ctx)
	return err
}

// Providers lists the providers, without their tokens.
func (c *Consumer) Providers(ctx context.Context) ([]Provider, error) {
	providers, _, err := c.store.Providers(ctx)
	return providers, err
}

// RemoteTables lists the tables provider shares with Igloo.
func (c *Consumer) RemoteTables(ctx context.Context, provider string) ([]remoteTable, error) {
	if _, err := c.Sync(ctx); err != nil {
		return nil, err
	}
	cl, err := c.client(provider)
	if err != nil {
		return nil, err
	}
	shares, err := cl.shares(ctx)
	if err != nil {
		return nil, err
	}
	var out []remoteTable
	for _, sh := range shares {
		tables, err := cl.tables(ctx, sh)
		if err != nil {
			return nil, err
		}
		out = append(out, tables...)
	}
	return out, nil
}

// CreateExternalTable records def and creates its view, after checking that
// the provider shares the table.
func (c *Consumer) CreateExternalTable(ctx context.Context, def ExternalTable) error {
	if _, err := c.Sync(ctx); err != nil {
		return err
	}
	cl, err := c.client(def.Provider)
	if err != nil {
		return err
	}
	if _, err := cl.version(ctx, def.Share, def.RemoteSchema, def.RemoteTable); err != nil {
		return fmt.Errorf("read %s from the provider: %w", def.Remote(), err)
	}
	if err := c.store.CreateExternalTable(ctx, def); err != nil {
		return err
	}
	if _, err := c.Sync(ctx); err != nil {
		return err
	}
	t := (*c.known.Load())[tableKey(def.Schema, def.Name)]
	if t == nil {
		return fmt.Errorf("external table %s.%s %w", def.Schema, def.Name, ErrNotFound)
	}
	if err := c.refresh(ctx, t, true); err != nil {
		// Recorded already; the first query retries.
		return fmt.Errorf("created, but could not read the shared table yet: %w", err)
	}
	return nil
}

// DropExternalTable drops the external table schema.name.
func (c *Consumer) DropExternalTable(ctx context.Context, schema, name string) error {
	if err := c.store.DropExternalTable(ctx, schema, name); err != nil {
		return err
	}
	_, err := c.Sync(ctx)
	return err
}

// ExternalTableInfo describes an external table for SHOW EXTERNAL TABLES.
type ExternalTableInfo struct {
	ExternalTable
	// Version is the shared table's version the view last listed, or -1.
	Version int64
	Checked time.Time
}

// ExternalTables lists the external tables.
func (c *Consumer) ExternalTables(ctx context.Context) ([]ExternalTableInfo, error) {
	if _, err := c.Sync(ctx); err != nil {
		return nil, err
	}
	defs, err := c.store.ExternalTables(ctx)
	if err != nil {
		return nil, err
	}
	known := *c.known.Load()
	out := make([]ExternalTableInfo, len(defs))
	for i, d := range defs {
		out[i] = ExternalTableInfo{ExternalTable: d, Version: -1}
		if t := known[tableKey(d.Schema, d.Name)]; t != nil && t.ready.Load() {
			t.mu.Lock()
			out[i].Version, out[i].Checked = t.version, t.checked
			t.mu.Unlock()
		}
	}
	return out, nil
}

// --- name rewriting ---------------------------------------------------------

// rewriteRefs points references to external tables at their views in the
// local database: catalog.schema.name and schema.name become
// local.schema.name. (A bare name needs nothing: sessions' search path
// includes the local database's default schema.) Literals, comments and
// everything else are left as written.
func rewriteRefs(query, catalog, local string, isExternal func(schema, name string) bool) string {
	toks := nameTokens(query)
	isName := func(i int) bool { return i >= 0 && i < len(toks) && !toks[i].dot && !toks[i].brk }
	isDot := func(i int) bool { return i >= 0 && i < len(toks) && toks[i].dot }

	var out strings.Builder
	last := 0
	for i := 0; i < len(toks); i++ {
		if !isName(i) || isDot(i-1) || !isDot(i+1) || !isName(i+2) {
			continue
		}
		switch {
		case isDot(i+3) && isName(i+4) && !isDot(i+5):
			// catalog.schema.name
			if strings.EqualFold(toks[i].name, catalog) && isExternal(toks[i+2].name, toks[i+4].name) {
				out.WriteString(query[last:toks[i].start])
				out.WriteString(local)
				last = toks[i].end
			}
			i += 4
		case !isDot(i + 3):
			// schema.name
			if isExternal(toks[i].name, toks[i+2].name) {
				out.WriteString(query[last:toks[i].start])
				out.WriteString(local + ".")
				last = toks[i].start
			}
			i += 2
		}
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// nameToken is an identifier or a dot in a query; anything else is a break.
type nameToken struct {
	start, end int
	name       string
	dot, brk   bool
}

// nameTokens lists the identifiers and dots in query, in order, with breaks
// for anything else that could separate them. Whitespace and comments do
// not break a name.
func nameTokens(query string) []nameToken {
	var toks []nameToken
	pos := 0
	for _, s := range sqllex.Segments(query) {
		start := pos
		pos += len(s.Text)
		switch s.Kind {
		case sqllex.Ident:
			name := s.Text
			if len(name) >= 2 {
				name = strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
			}
			toks = append(toks, nameToken{start: start, end: pos, name: name})
		case sqllex.String:
			toks = append(toks, nameToken{brk: true})
		case sqllex.Code:
			for i := 0; i < len(s.Text); {
				ch := s.Text[i]
				switch {
				case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f':
					i++
				case ch == '.':
					toks = append(toks, nameToken{start: start + i, end: start + i + 1, dot: true})
					i++
				case sqllex.IdentByte(ch) && !(ch >= '0' && ch <= '9'):
					j := i
					for j < len(s.Text) && sqllex.IdentByte(s.Text[j]) {
						j++
					}
					toks = append(toks, nameToken{start: start + i, end: start + j, name: s.Text[i:j]})
					i = j
				default:
					toks = append(toks, nameToken{brk: true})
					i++
				}
			}
		}
	}
	return toks
}
