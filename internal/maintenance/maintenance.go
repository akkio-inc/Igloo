// Package maintenance keeps Iceberg tables small and fast without an
// operator. On a schedule it compacts small data files, expires old snapshots
// (deleting the files only they referenced) and deletes files that no
// snapshot references, such as the leftovers of failed writes.
//
// Every write through DuckDB adds a snapshot and at least one data file, so
// without this a table that takes frequent small writes grows without bound
// and gets slower to read.
//
// Clones share their source's files (see package clone). A table's
// maintenance deletes only files inside its own location, and of those only
// ones no clone references: each pass first compacts and expires every
// table, noting what each clone shares, then deletes.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/deletefiles"
	"github.com/akkio-inc/igloo/internal/filerewrite"
)

// Standard Iceberg table properties that override the retention defaults for
// one table. iceberg-go reads differently named keys, so Igloo reads these
// itself and passes the values explicitly.
const (
	maxSnapshotAgeProperty     = "history.expire.max-snapshot-age-ms"
	minSnapshotsToKeepProperty = "history.expire.min-snapshots-to-keep"
	maxRefAgeProperty          = "history.expire.max-ref-age-ms"
)

const (
	// orphanAge is how old an unreferenced file must be before it is
	// deleted, so the files of a write that has not committed yet are
	// left alone.
	orphanAge = 72 * time.Hour
	// orphanInterval spaces out orphan scans, which list every file under
	// a table's location.
	orphanInterval = 24 * time.Hour
	// maxCompactBytes caps the data rewritten in one commit. A writer that
	// commits first makes the compaction commit fail, and the cap bounds
	// the work that is then thrown away. Larger backlogs finish over
	// several passes.
	maxCompactBytes = 8 << 30
	// resumeDelay is how soon after starting a replica begins a pass that
	// is overdue. It is short because Igloo may only run for a burst of
	// queries before the scale-to-zero gateway stops it again.
	resumeDelay = 10 * time.Second
	// interruptedBackoff is how long a table whose rewrite (compaction, or
	// the rewrite of rows deleted longer ago than its data retention) was
	// interrupted, most likely because Igloo was killed while running it,
	// is not rewritten again. Its snapshots are still expired.
	interruptedBackoff = 24 * time.Hour
	// recordTimeout bounds recording that a rewrite ended, which also
	// happens while Igloo stops.
	recordTimeout = 5 * time.Second
)

// Jobs recorded in a Schedule.
const (
	passJob         = "pass"
	orphanJobPrefix = "orphans/"
	// When the rewrite of a table (namespace.table) last started and
	// ended. One that started after it last ended was interrupted.
	rewriteJobPrefix  = "rewrite/"
	rewriteStartedJob = rewriteJobPrefix + "started/"
	rewriteEndedJob   = rewriteJobPrefix + "ended/"
)

// Options controls the maintenance loop.
type Options struct {
	// Interval is the time between passes over every table.
	Interval time.Duration
	// Retention is how long snapshots are kept, and so how far back time
	// travel reaches, for tables that do not set
	// history.expire.max-snapshot-age-ms.
	Retention time.Duration
	// Lock makes sure only one replica maintains tables at a time. Nil
	// means this replica always runs.
	Lock Lock
	// Metrics may be nil.
	Metrics *Metrics
	// Activity, if set, records each pass while it runs.
	Activity *activity.Tracker
	// Schedule, if set, remembers when passes and orphan scans last ran,
	// across restarts and replicas. Without it, a replica that keeps
	// restarting, as with scale to zero, may rarely maintain tables.
	Schedule Schedule
	// Failed, if set, is told about each table whose maintenance failed,
	// as namespace.table, or "" when the tables could not be listed. It
	// must not block.
	Failed func(table string, err error)
	// Clones keeps the files clones share. Nil means no table is a clone.
	Clones *clone.Guard
	// DataRetention, if set, returns the tables' data retentions, by
	// lower-case schema.table (ALTER TABLE ... SET (data_retention_days
	// = N)): how long tables that hold personal data keep old versions of
	// their rows, whatever their own retention properties say.
	DataRetention func(ctx context.Context) (map[string]time.Duration, error)
	// Memory is how many bytes of memory compaction may hold while it
	// rewrites a table's files. Zero means DefaultMemory. A table that
	// cannot be compacted within it is left as it is.
	Memory int64
	// SpillDir is where clustering spills the rows it sorts (see
	// cluster), in a directory of its own that it removes; empty means the
	// system's temporary directory.
	SpillDir string
	// Threads are DuckDB's threads, each of which holds a row group while
	// an insert writes (see sizeRowGroups); zero means one.
	Threads int
	// BeforePass, if set, runs at the start of each pass, while this
	// replica holds the lock, before the tables are maintained: Igloo maps
	// ID domains' ids there (pgwire.Server.MapIDDomains), so that a pass
	// compacts what mapping rewrote.
	BeforePass func(ctx context.Context)
	// Busy, if set, reports whether a transaction block that has not
	// ended has read or written table (namespace.table). Its COMMIT would
	// fail after a rewrite of the table's files, as DuckDB commits only on
	// top of the snapshot it read, so maintenance leaves the files for a
	// later pass. Igloo passes its own sessions' blocks
	// (pgwire.Server.InTransaction).
	Busy func(table string) bool
}

// Catalog is the Iceberg catalog; *catalog.Client implements it.
type Catalog interface {
	Tables(ctx context.Context) ([]table.Identifier, error)
	LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error)
}

// Schedule records when maintenance jobs last ran.
type Schedule interface {
	// LastRuns returns when each job whose name starts with prefix last ran.
	LastRuns(ctx context.Context, prefix string) (map[string]time.Time, error)
	// Ran records that job ran at at.
	Ran(ctx context.Context, job string, at time.Time) error
}

// Lock is held by at most one replica at a time.
type Lock interface {
	// TryLock takes the lock if nobody holds it. When ok is true, release
	// must be called once the work is done.
	TryLock(ctx context.Context) (release func(), ok bool, err error)
}

// Maintainer runs table maintenance against an Iceberg REST catalog.
type Maintainer struct {
	catalog Catalog
	opts    Options
	log     *slog.Logger
	// warehouse is the warehouse root with a trailing slash.
	warehouse string

	// lastOrphanScan is when each table's orphan files were last removed by
	// this replica.
	lastOrphanScan map[string]time.Time
	// targets are the data files position delete files delete rows of.
	targets *deletefiles.Targets
	// rewrites are the rewriteJobPrefix jobs of the Schedule, as the pass
	// began.
	rewrites map[string]time.Time
	// tooBig holds the tables whose compaction needed more memory than
	// Options.Memory, with the snapshot it was tried at: they are not
	// compacted again until they change.
	tooBig map[string]int64
	// clusterFailed holds the tables whose clustering needed more memory
	// than Options.Memory (see compactOrCluster).
	clusterFailed map[string]clusterFailure
	// maxRewrite, if set, is how many bytes clustering rewrites a pass in
	// place of maxCompactBytes, for tests.
	maxRewrite int64
	// sorted, if set, writes clustering's groups in place of sortedWriter,
	// for tests.
	sorted func([]filerewrite.SortField) groupWriter
}

// New maintains the tables in cat. warehouse is the warehouse root, such as
// s3://bucket/warehouse/.
func New(cat Catalog, warehouse string, opts Options, log *slog.Logger) *Maintainer {
	return &Maintainer{
		catalog:        cat,
		opts:           opts,
		log:            log.With("component", "maintenance"),
		warehouse:      warehouse,
		lastOrphanScan: map[string]time.Time{},
		targets:        deletefiles.NewTargets(),
		rewrites:       map[string]time.Time{},
		tooBig:         map[string]int64{},
		clusterFailed:  map[string]clusterFailure{},
	}
}

// memory is the memory compaction may hold.
func (m *Maintainer) memory() int64 {
	if m.opts.Memory > 0 {
		return m.opts.Memory
	}
	return DefaultMemory
}

// Run maintains tables every Interval until ctx is canceled.
func (m *Maintainer) Run(ctx context.Context) {
	delay := m.firstDelay(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		m.RunOnce(ctx)
		delay = m.opts.Interval
	}
}

// firstDelay is how long to wait before the first pass. With a Schedule, a
// pass that is overdue, because no replica finished one within Interval
// (perhaps Igloo was stopped), starts within seconds. Otherwise the first
// pass waits for a random part of the interval, so replicas that start
// together spread out.
func (m *Maintainer) firstDelay(ctx context.Context) time.Duration {
	jitter := time.Duration(rand.Int64N(int64(m.opts.Interval)/4 + 1))
	if m.opts.Schedule == nil {
		return jitter
	}
	runs, err := m.opts.Schedule.LastRuns(ctx, passJob)
	if err != nil {
		m.log.Warn("could not read the maintenance schedule", "err", err)
		return jitter
	}
	soon := resumeDelay + time.Duration(rand.Int64N(int64(resumeDelay)))
	return max(time.Until(runs[passJob].Add(m.opts.Interval)), soon)
}

// RunOnce makes one pass over every table, unless another replica holds the
// lock.
func (m *Maintainer) RunOnce(ctx context.Context) {
	if m.opts.Lock != nil {
		release, ok, err := m.opts.Lock.TryLock(ctx)
		if err != nil {
			m.log.Warn("could not take the maintenance lock", "err", err)
			return
		}
		if !ok {
			m.log.Debug("another replica is maintaining tables")
			return
		}
		defer release()
	}
	defer m.opts.Activity.Begin()()
	if m.opts.BeforePass != nil {
		m.opts.BeforePass(ctx)
	}
	if m.opts.Schedule != nil {
		runs, err := m.opts.Schedule.LastRuns(ctx, orphanJobPrefix)
		if err != nil {
			m.log.Warn("could not read the maintenance schedule", "err", err)
		}
		for job, at := range runs {
			key := strings.TrimPrefix(job, orphanJobPrefix)
			if at.After(m.lastOrphanScan[key]) {
				m.lastOrphanScan[key] = at
			}
		}
		if m.rewrites, err = m.opts.Schedule.LastRuns(ctx, rewriteJobPrefix); err != nil {
			m.log.Warn("could not read the maintenance schedule", "err", err)
			m.rewrites = map[string]time.Time{}
		}
	}

	start := time.Now()
	tables, err := m.catalog.Tables(ctx)
	if err != nil {
		m.log.Warn("could not list tables", "err", err)
		m.opts.Metrics.pass(false)
		m.failed("", err)
		return
	}
	var limits map[string]time.Duration
	if m.opts.DataRetention != nil {
		if limits, err = m.opts.DataRetention(ctx); err != nil {
			// Without them, tables would keep data longer than they may.
			m.log.Warn("could not read data retentions; skipping the pass", "err", err)
			m.opts.Metrics.pass(false)
			m.failed("", err)
			return
		}
	}
	failed := 0
	pass := m.opts.Clones.Begin()
	var cleanups []*cleanup
	for _, ident := range tables {
		if ctx.Err() != nil {
			return
		}
		limit, limited := limits[strings.ToLower(name(ident))]
		c, err := m.maintain(ctx, ident, pass, limit, limited)
		if err != nil {
			failed++
			m.log.Warn("table maintenance failed", "table", name(ident), "err", err)
			m.failed(name(ident), err)
		}
		if c != nil {
			cleanups = append(cleanups, c)
		}
	}
	// Files are deleted once every table was seen, so what every clone
	// shares is known.
	pass.Settle(ctx)
	for _, c := range cleanups {
		if ctx.Err() != nil {
			return
		}
		if err := m.clean(ctx, c, pass); err != nil {
			failed++
			m.log.Warn("table maintenance failed", "table", name(c.ident), "err", err)
		}
	}
	pass.Sweep(ctx)
	m.opts.Metrics.pass(failed == 0)
	m.record(ctx, passJob)
	m.log.Debug("maintenance pass finished", "tables", len(tables), "failed", failed, "took", time.Since(start))
}

// failed tells Options.Failed about a failure, unless Igloo is stopping.
func (m *Maintainer) failed(table string, err error) {
	if m.opts.Failed != nil && !errors.Is(err, context.Canceled) {
		m.opts.Failed(table, err)
	}
}

// record notes in the Schedule that job ran just now.
func (m *Maintainer) record(ctx context.Context, job string) {
	if m.opts.Schedule == nil {
		return
	}
	if err := m.opts.Schedule.Ran(ctx, job, time.Now()); err != nil {
		m.log.Warn("could not record maintenance", "job", job, "err", err)
	}
}

// cleanup is what is left to delete of one table once it is compacted and
// expired.
type cleanup struct {
	ident table.Identifier
	// expired are the files only expired snapshots referenced.
	expired []string
	// orphans says whether to look for orphan files too.
	orphans bool
}

// maintain compacts and expires one table, and returns what is left to
// delete, if anything. A commit that loses a race with a writer is not an
// error; the next pass tries again. A failure in one step does not stop
// the others. A table with a data retention (limited) keeps no version of
// its rows older than limit.
func (m *Maintainer) maintain(ctx context.Context, ident table.Identifier, pass *clone.Pass, limit time.Duration, limited bool) (*cleanup, error) {
	tbl, err := m.catalog.LoadTable(ctx, ident)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		pass.Missing() // dropped or renamed since the listing
		return nil, nil
	}
	if err != nil {
		pass.Failed()
		return nil, fmt.Errorf("load: %w", err)
	}
	pass.Saw(ctx, tbl)
	log := m.log.With("table", name(ident))
	var errs []error

	if at, interrupted := m.interrupted(name(ident)); interrupted {
		// Most likely the rewrite needed more memory than Igloo had, and
		// starting it again would only kill Igloo again.
		errs = append(errs, fmt.Errorf("the last rewrite of the table's files, started %s, never ended, most likely "+
			"because Igloo was killed while running it; its files are not rewritten again until %s",
			at.UTC().Format(time.RFC3339), at.Add(interruptedBackoff).UTC().Format(time.RFC3339)))
	} else {
		errs = append(errs, m.rewrite(ctx, ident, tbl, limit, limited, log)...)
	}
	if tbl, err = m.catalog.LoadTable(ctx, ident); err != nil {
		return nil, errors.Join(append(errs, fmt.Errorf("load: %w", err))...)
	}
	switch err := m.sizeRowGroups(ctx, tbl, log); {
	case err == nil:
	case errors.Is(err, table.ErrCommitFailed):
		log.Debug("sizing the table's row groups lost a race with a writer; retrying next pass")
	default:
		errs = append(errs, fmt.Errorf("size row groups: %w", err))
	}
	if tbl, err = m.catalog.LoadTable(ctx, ident); err != nil {
		return nil, errors.Join(append(errs, fmt.Errorf("load: %w", err))...)
	}

	c := &cleanup{ident: ident, orphans: m.orphansDue(tbl)}
	switch expired, files, err := m.expire(ctx, tbl, log, limit, limited); {
	case err == nil:
		c.expired = files
		if expired != tbl {
			if err := pass.Release(ctx, expired); err != nil {
				log.Warn("could not update which files the clone shares", "err", err)
			}
		}
	case errors.Is(err, table.ErrCommitFailed):
		log.Debug("snapshot expiry lost a race with a writer; retrying next pass")
	default:
		errs = append(errs, fmt.Errorf("expire snapshots: %w", err))
	}
	if len(c.expired) == 0 && !c.orphans {
		c = nil
	}
	return c, errors.Join(errs...)
}

// rewrite compacts tbl and, for a table with a data retention (limited),
// rewrites the files with rows deleted longer ago than limit. It returns
// what failed.
func (m *Maintainer) rewrite(ctx context.Context, ident table.Identifier, tbl *table.Table, limit time.Duration, limited bool, log *slog.Logger) []error {
	if m.busy(ident) {
		log.Debug("a transaction block that read the table is open; its files are rewritten on a later pass")
		return nil
	}
	var errs []error
	switch _, err := m.compactOrCluster(ctx, tbl, log); {
	case err == nil:
	case errors.Is(err, table.ErrCommitFailed):
		log.Debug("compaction lost a race with a writer; retrying next pass")
	case errors.Is(err, errOverBudget):
		// Trying again before the table changes would fail the same way.
		m.tooBig[name(ident)] = tbl.CurrentSnapshot().SnapshotID
		errs = append(errs, fmt.Errorf("compact: %w; the table is not compacted again until it changes", err))
	default:
		errs = append(errs, fmt.Errorf("compact: %w", err))
	}
	if !limited {
		return errs
	}
	tbl, err := m.catalog.LoadTable(ctx, ident)
	if err != nil {
		return append(errs, fmt.Errorf("load: %w", err))
	}
	switch _, err := m.purgeDeleted(ctx, tbl, limit, log); {
	case err == nil:
	case errors.Is(err, table.ErrCommitFailed):
		log.Debug("rewriting files with old deletes lost a race with a writer; retrying next pass")
	default:
		errs = append(errs, fmt.Errorf("rewrite files with old deletes: %w", err))
	}
	return errs
}

// interrupted reports whether the last rewrite of table, which started at,
// never ended, less than interruptedBackoff ago.
func (m *Maintainer) interrupted(table string) (at time.Time, ok bool) {
	at = m.rewrites[rewriteStartedJob+table]
	return at, at.After(m.rewrites[rewriteEndedJob+table]) && time.Since(at) < interruptedBackoff
}

// rewriting records that a rewrite of table starts, and returns a function
// that records that it ended. It ends too when ctx is canceled, as when
// Igloo stops: only a rewrite that kills Igloo is left without an end.
func (m *Maintainer) rewriting(ctx context.Context, table string) (ended func()) {
	if m.opts.Schedule == nil {
		return func() {}
	}
	// Recorded times only move forward, and replicas' clocks differ: the
	// end is recorded no earlier than the latest start.
	started := later(time.Now(), m.rewrites[rewriteStartedJob+table])
	m.rewrites[rewriteStartedJob+table] = started
	if err := m.opts.Schedule.Ran(ctx, rewriteStartedJob+table, started); err != nil {
		m.log.Warn("could not record maintenance", "job", rewriteStartedJob+table, "err", err)
	}
	var once sync.Once
	end := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
			defer cancel()
			job, at := rewriteEndedJob+table, later(time.Now(), started)
			if err := m.opts.Schedule.Ran(ctx, job, at); err != nil {
				m.log.Warn("could not record maintenance", "job", job, "err", err)
			}
		})
	}
	stop := context.AfterFunc(ctx, end)
	return func() {
		stop()
		end()
		m.rewrites[rewriteEndedJob+table] = later(time.Now(), started)
	}
}

// later returns the later of a and b.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// orphansDue reports whether to look for the table's orphan files, which
// happens at most once per orphanInterval. Iceberg treats everything under a
// table's location as the table's, so this is only for tables inside
// Igloo's warehouse: a table registered from elsewhere may share its
// location with other data.
func (m *Maintainer) orphansDue(tbl *table.Table) bool {
	return time.Since(m.lastOrphanScan[name(tbl.Identifier())]) >= orphanInterval &&
		strings.HasPrefix(strings.TrimSuffix(tbl.Location(), "/")+"/", m.warehouse)
}

// clean deletes the files only expired snapshots referenced and, when due,
// the table's orphan files: files under its location that no snapshot
// references and that are older than orphanAge, such as the leftovers of
// failed writes. Files that clones share stay (see clone.Guard).
func (m *Maintainer) clean(ctx context.Context, c *cleanup, pass *clone.Pass) error {
	log := m.log.With("table", name(c.ident))
	tbl, err := m.catalog.LoadTable(ctx, c.ident)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return nil // dropped meanwhile: its files stay, as dropping leaves them
	}
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	files := c.expired
	var orphans []string
	if c.orphans {
		res, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(orphanAge), table.WithDryRun(true))
		if err != nil {
			return fmt.Errorf("find orphan files: %w", err)
		}
		orphans = res.OrphanFileLocations
		files = append(files, orphans...)
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	deleted, err := pass.Delete(ctx, tbl, files, func(ctx context.Context, paths []string) error {
		return remove(ctx, fs, paths)
	})
	if errors.Is(err, clone.ErrBusy) || errors.Is(err, clone.ErrUnsure) {
		log.Debug("kept the table's files for a later pass", "reason", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete files: %w", err)
	}
	if !c.orphans {
		return nil
	}
	key := name(c.ident)
	m.lastOrphanScan[key] = time.Now()
	m.record(ctx, orphanJobPrefix+key)
	isOrphan := make(map[string]bool, len(orphans))
	for _, f := range orphans {
		isOrphan[f] = true
	}
	n := 0
	for _, f := range deleted {
		if isOrphan[f] {
			n++
		}
	}
	if n > 0 {
		m.opts.Metrics.orphansDeleted(n)
		log.Info("deleted orphan files", "files", n)
	}
	return nil
}

// remove deletes files, in bulk where the storage can. Files already gone
// are not an error.
func remove(ctx context.Context, fs iceio.IO, paths []string) error {
	if bulk, ok := fs.(iceio.BulkRemovableIO); ok {
		deleted, err := bulk.DeleteFiles(ctx, paths)
		if err == nil {
			return nil
		}
		done := make(map[string]bool, len(deleted))
		for _, d := range deleted {
			done[d] = true
		}
		var rest []string
		for _, p := range paths {
			if !done[p] {
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

// removeOrphans deletes files under the table's location that no snapshot
// references, at most once per orphanInterval. Iceberg treats everything
// under a table's location as the table's, so this runs only for tables
// inside Igloo's warehouse: a table registered from elsewhere may share its
// location with other data.
func (m *Maintainer) removeOrphans(ctx context.Context, tbl *table.Table, log *slog.Logger) error {
	key := name(tbl.Identifier())
	if time.Since(m.lastOrphanScan[key]) < orphanInterval {
		return nil
	}
	if !strings.HasPrefix(strings.TrimSuffix(tbl.Location(), "/")+"/", m.warehouse) {
		return nil
	}
	res, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(orphanAge))
	if err != nil {
		return err
	}
	m.lastOrphanScan[key] = time.Now()
	m.record(ctx, orphanJobPrefix+key)
	if n := len(res.DeletedFiles); n > 0 {
		m.opts.Metrics.orphansDeleted(n)
		log.Info("deleted orphan files", "files", n)
	}
	return nil
}

// compact rewrites small files, and files with many deletes, into files of
// the table's target size, merging deletes as it goes. It returns the table
// as committed.
func (m *Maintainer) compact(ctx context.Context, tbl *table.Table, log *slog.Logger) (*table.Table, error) {
	if tbl.CurrentSnapshot() == nil {
		return tbl, nil
	}
	if id, ok := m.tooBig[name(tbl.Identifier())]; ok {
		if id == tbl.CurrentSnapshot().SnapshotID {
			log.Debug("not compacting the table: it needed more memory than compaction may use, and has not changed since")
			return tbl, nil
		}
		delete(m.tooBig, name(tbl.Identifier()))
	}
	cfg := compaction.DefaultConfig()
	if target, err := strconv.ParseInt(tbl.Properties()[table.WriteTargetFileSizeBytesKey], 10, 64); err == nil && target > 0 {
		cfg.TargetFileSizeBytes = target
		cfg.MinFileSizeBytes = target * 3 / 4
		cfg.MaxFileSizeBytes = target * 9 / 5
	}
	plan, err := compaction.Analyze(ctx, tbl, cfg)
	if err != nil || len(plan.Groups) == 0 {
		return tbl, err
	}

	var (
		groups  []table.CompactionTaskGroup
		total   int64
		rewrite = map[string]struct{}{}
	)
	for _, g := range plan.Groups {
		if len(groups) > 0 && total+g.TotalSizeBytes > maxCompactBytes {
			break
		}
		groups = append(groups, table.CompactionTaskGroup{
			PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes,
		})
		total += g.TotalSizeBytes
		for _, task := range g.Tasks {
			rewrite[task.File.FilePath()] = struct{}{}
		}
	}

	// Before it starts, so the log says what a pass was doing if it never
	// finishes.
	log.Info("compacting", "files", len(rewrite), "bytes", total, "groups", len(groups), "memory_limit", m.memory())
	start := time.Now()
	committed, res, err := m.rewriteGroups(ctx, tbl, groups, rewrite, "compact", rewriteGroup)
	if err != nil || res.removed == 0 {
		return committed, err
	}
	m.opts.Metrics.compacted(res.removed, res.added)
	log.Info("compacted",
		"files_before", res.removed, "files_after", res.added, "delete_files_removed", res.deletes,
		"bytes_before", res.bytesBefore, "bytes_after", res.bytesAfter,
		"took", time.Since(start).Round(time.Millisecond), "peak_memory", res.peak)
	return committed, nil
}

// rewritten counts what rewriteGroups did.
type rewritten struct {
	removed, added, deletes int
	bytesBefore, bytesAfter int64
	// peak is the most memory the rewrite held at once.
	peak int64
}

// rewriteGroups writes the files of groups, whose paths rewrite holds,
// again with their deletes applied, and commits the new files in their
// place. It removes the equality deletes that apply to no file left, and
// the position deletes that delete rows only of files it replaces (see
// package deletefiles): one that also deletes rows of a file it keeps
// stays. iceberg-go's RewriteDataFiles removed every position delete file
// attached to the files it rewrote, and a partition-scoped one (as Spark
// before Iceberg 1.6, iceberg-go and DuckDB write them) also deletes rows
// of the partition's other files, which came back. The commit is not
// retried on a conflict, which would bring the replaced files back (see
// iceberg-go's IGLOO_PATCHES.md): maintenance tries again on its next run.
// The groups are rewritten one after another, each by write, within the
// memory of Options.Memory (see rewriteGroup); one that needs more fails with
// errOverBudget.
func (m *Maintainer) rewriteGroups(ctx context.Context, tbl *table.Table, groups []table.CompactionTaskGroup,
	rewrite map[string]struct{}, operation string, write groupWriter) (*table.Table, rewritten, error) {
	var res rewritten
	fs, err := tbl.FS(ctx)
	if err != nil {
		return tbl, res, err
	}
	snap := tbl.CurrentSnapshot()
	deadDeletes, err := compaction.CollectDeadEqualityDeletes(ctx, fs, snap, rewrite)
	if err != nil {
		return tbl, res, fmt.Errorf("find dead equality deletes: %w", err)
	}
	live, err := deletefiles.Live(fs, snap)
	if err != nil {
		return tbl, res, err
	}
	var tasks []table.FileScanTask
	for _, g := range groups {
		tasks = append(tasks, g.Tasks...)
	}
	// iceberg-go cannot read deletion vectors yet: rewriting would fail
	// on every pass, so the table's files are left as they are.
	if slices.ContainsFunc(tasks, func(t table.FileScanTask) bool { return len(t.DeletionVectorFiles) > 0 }) {
		m.log.Warn("files with deletion vectors are not rewritten: Igloo cannot read deletion vectors yet",
			"table", name(tbl.Identifier()), "operation", operation)
		return tbl, res, nil
	}
	safe, err := m.targets.Removable(ctx, fs, tasks, live)
	if err != nil {
		return tbl, res, fmt.Errorf("find the position deletes the rewrite may remove: %w", err)
	}
	tx := tbl.NewTransaction()
	rw := tx.NewRewrite(map[string]string{"igloo.operation": operation})
	defer m.rewriting(ctx, name(tbl.Identifier()))()
	budget := m.memory()
	rctx, meter, stop := metered(ctx, budget)
	defer stop(nil)
	for _, g := range groups {
		if len(g.Tasks) == 0 {
			continue
		}
		added, lay, err := write(rctx, tbl, fs, g, budget)
		res.peak = meter.Peak()
		if err != nil {
			return tbl, res, err
		}
		m.log.Debug("rewrote files", "table", name(tbl.Identifier()), "operation", operation, "files", len(g.Tasks),
			"files_written", len(added), "batch_rows", lay.BatchRows, "row_group_rows", lay.RowGroupRows, "peak_memory", res.peak)
		old := make([]iceberg.DataFile, len(g.Tasks))
		for i, task := range g.Tasks {
			old[i] = task.File
		}
		rw.Apply(old, added, nil)
		res.removed += len(old)
		res.added += len(added)
		for _, df := range old {
			res.bytesBefore += df.FileSizeBytes()
		}
		for _, df := range added {
			res.bytesAfter += df.FileSizeBytes()
		}
	}
	if res.removed == 0 && res.added == 0 {
		return tbl, res, nil
	}
	removed := map[string]bool{}
	for _, df := range slices.Concat(safe, deadDeletes) {
		if !removed[df.FilePath()] {
			removed[df.FilePath()] = true
			rw.DeleteFile(df)
		}
	}
	res.deletes = len(removed)
	if m.busy(tbl.Identifier()) {
		// A block began reading the table while its files were written:
		// give way to it, as to a writer that committed first.
		return tbl, res, fmt.Errorf("%w: a transaction block read the table while its files were rewritten", table.ErrCommitFailed)
	}
	if err := rw.Commit(ctx); err != nil {
		return tbl, res, fmt.Errorf("commit the rewrite: %w", err)
	}
	committed, err := tx.CommitWithoutRetry(ctx)
	if err != nil {
		return tbl, res, err
	}
	return committed, res, nil
}

// expire removes snapshots older than the table's retention, always keeping
// the current one, and returns the table as committed and the files inside
// its location that only those snapshots referenced, for clean to delete.
// Files that compaction replaced stay until the snapshots that read them
// expire, so time travel within the retention window keeps working.
func (m *Maintainer) expire(ctx context.Context, tbl *table.Table, log *slog.Logger, limit time.Duration, limited bool) (*table.Table, []string, error) {
	maxAge, minKeep := m.retention(tbl)
	if limited {
		maxAge, minKeep = min(maxAge, limit), 1
	}
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	expirable := 0
	for _, s := range tbl.Metadata().Snapshots() {
		if s.TimestampMs < cutoff {
			expirable++
		}
	}
	if expirable == 0 || len(tbl.Metadata().Snapshots()) <= minKeep {
		return tbl, nil, nil
	}

	before := len(tbl.Metadata().Snapshots())
	tx := tbl.NewTransaction()
	// Branches and tags are kept, with the snapshots they point at, as
	// long as their own max-ref-age-ms or the table's
	// history.expire.max-ref-age-ms says, forever by default, as Java's
	// Iceberg keeps them: a tag is someone's reference point, such as an
	// audit's, and a branch can hold unpublished writes. iceberg-go would
	// otherwise drop every ref older than the snapshot retention. It would
	// also delete the files itself, a clone's included and those of its
	// source that a clone references.
	refAge := time.Duration(math.MaxInt64)
	if ms, err := strconv.ParseInt(tbl.Properties()[maxRefAgeProperty], 10, 64); err == nil && ms > 0 {
		refAge = millis(ms)
	}
	if err := tx.ExpireSnapshots(table.WithOlderThan(maxAge), table.WithRetainLast(minKeep), table.WithMaxRefAge(refAge),
		table.WithPostCommit(false)); err != nil {
		return tbl, nil, err
	}
	// Without retry: on a conflict the rebuild could reintroduce replaced
	// files, so maintenance fails and retries next pass (see IGLOO_PATCHES.md).
	committed, err := tx.CommitWithoutRetry(ctx)
	if err != nil {
		return tbl, nil, err
	}
	n := before - len(committed.Metadata().Snapshots())
	if n == 0 {
		return committed, nil, nil
	}
	m.opts.Metrics.expired(n)
	log.Info("expired snapshots", "snapshots", n, "retention", maxAge)
	fs, err := tbl.FS(ctx)
	if err != nil {
		return committed, nil, err
	}
	// Files missed here, if a manifest cannot be read, are orphans that
	// the orphan cleanup deletes later.
	files, err := clone.Unreferenced(fs, tbl.Metadata(), committed.Metadata(), tbl.Location())
	if err != nil {
		return committed, nil, fmt.Errorf("find the files of expired snapshots: %w", err)
	}
	return committed, files, nil
}

// purgeDeleted rewrites the data files that delete files older than limit
// delete rows of, with the deletes applied, so rows deleted longer ago
// leave the table's files, as expiring snapshots makes old versions of rows
// leave. A delete file is as old as the snapshot that added it; one whose
// snapshot has expired counts as old. It returns the table as committed.
func (m *Maintainer) purgeDeleted(ctx context.Context, tbl *table.Table, limit time.Duration, log *slog.Logger) (*table.Table, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return tbl, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return tbl, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return tbl, err
	}
	cutoff := time.Now().Add(-limit).UnixMilli()
	old := map[string]bool{}
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range mf.Entries(fs, true) {
			if err != nil {
				return tbl, err
			}
			if added := tbl.SnapshotByID(e.SnapshotID()); added == nil || added.TimestampMs < cutoff {
				old[e.DataFile().FilePath()] = true
			}
		}
	}
	if len(old) == 0 {
		return tbl, nil
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return tbl, err
	}
	var (
		groups  []table.CompactionTaskGroup
		index   = map[string]int{}
		total   int64
		rewrite = map[string]struct{}{}
	)
	for _, task := range tasks {
		if hit, err := m.deletesFrom(ctx, fs, task, old); err != nil {
			return tbl, err
		} else if !hit {
			continue
		}
		if len(rewrite) > 0 && total+task.File.FileSizeBytes() > maxCompactBytes {
			break
		}
		key := fmt.Sprint(task.File.SpecID(), task.File.Partition())
		i, ok := index[key]
		if !ok {
			i, index[key] = len(groups), len(groups)
			groups = append(groups, table.CompactionTaskGroup{PartitionKey: key})
		}
		groups[i].Tasks = append(groups[i].Tasks, task)
		groups[i].TotalSizeBytes += task.File.FileSizeBytes()
		total += task.File.FileSizeBytes()
		rewrite[task.File.FilePath()] = struct{}{}
	}
	if len(rewrite) == 0 {
		return tbl, nil
	}
	log.Info("rewriting files whose deleted rows are older than the data retention", "files", len(rewrite), "bytes", total,
		"memory_limit", m.memory())
	committed, res, err := m.rewriteGroups(ctx, tbl, groups, rewrite, "purge-deleted", rewriteGroup)
	if err != nil || res.removed == 0 {
		return committed, err
	}
	m.opts.Metrics.compacted(res.removed, res.added)
	log.Info("rewrote files whose deleted rows are older than the data retention",
		"files", res.removed, "retention", limit)
	return committed, nil
}

// deletesFrom reports whether one of the old delete files deletes rows of
// task's data file. Planning attaches a position delete file to every data
// file it may apply to, which for DuckDB's is every file of the partition:
// those it really applies to are the ones its file_path column names.
func (m *Maintainer) deletesFrom(ctx context.Context, fs iceio.IO, task table.FileScanTask, old map[string]bool) (bool, error) {
	if slices.ContainsFunc(slices.Concat(task.EqualityDeleteFiles, task.DeletionVectorFiles), func(d iceberg.DataFile) bool {
		return old[d.FilePath()]
	}) {
		return true, nil
	}
	for _, d := range task.DeleteFiles {
		if !old[d.FilePath()] {
			continue
		}
		targets, err := m.targets.Of(ctx, fs, d)
		if err != nil {
			return false, err
		}
		if targets[task.File.FilePath()] {
			return true, nil
		}
	}
	return false, nil
}

// retention is how long tbl keeps snapshots and how many it always keeps.
func (m *Maintainer) retention(tbl *table.Table) (time.Duration, int) {
	maxAge, minKeep := m.opts.Retention, 1
	props := tbl.Properties()
	if ms, err := strconv.ParseInt(props[maxSnapshotAgeProperty], 10, 64); err == nil && ms >= 0 {
		maxAge = millis(ms)
	}
	if n, err := strconv.Atoi(props[minSnapshotsToKeepProperty]); err == nil && n >= 1 {
		minKeep = n
	}
	return maxAge, minKeep
}

// millis is ms milliseconds as a duration, which saturates: Java's Iceberg
// writes Long.MAX_VALUE milliseconds for "forever", which overflows a
// duration's nanoseconds into a negative age.
func millis(ms int64) time.Duration {
	if ms > math.MaxInt64/int64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ms) * time.Millisecond
}

func name(ident table.Identifier) string {
	return strings.Join(ident, ".")
}

// busy reports whether Options.Busy says a transaction block has read the
// table.
func (m *Maintainer) busy(ident table.Identifier) bool {
	return m.opts.Busy != nil && m.opts.Busy(name(ident))
}
