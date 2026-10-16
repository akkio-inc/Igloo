// Package maintenance keeps Iceberg tables small and fast without an
// operator. On a schedule it compacts small data files, expires old snapshots
// (deleting the files only they referenced) and deletes files that no
// snapshot references, such as the leftovers of failed writes.
//
// Every write through DuckDB adds a snapshot and at least one data file, so
// without this a table that takes frequent small writes grows without bound
// and gets slower to read.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/catalog"
)

// Standard Iceberg table properties that override the retention defaults for
// one table. iceberg-go reads differently named keys, so Igloo reads these
// itself and passes the values explicitly.
const (
	maxSnapshotAgeProperty     = "history.expire.max-snapshot-age-ms"
	minSnapshotsToKeepProperty = "history.expire.min-snapshots-to-keep"
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
)

// Jobs recorded in a Schedule.
const (
	passJob         = "pass"
	orphanJobPrefix = "orphans/"
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
	catalog *catalog.Client
	opts    Options
	log     *slog.Logger
	// warehouse is the warehouse root with a trailing slash.
	warehouse string

	// lastOrphanScan is when each table's orphan files were last removed by
	// this replica.
	lastOrphanScan map[string]time.Time
}

// New maintains the tables in cat. warehouse is the warehouse root, such as
// s3://bucket/warehouse/.
func New(cat *catalog.Client, warehouse string, opts Options, log *slog.Logger) *Maintainer {
	return &Maintainer{
		catalog:        cat,
		opts:           opts,
		log:            log.With("component", "maintenance"),
		warehouse:      warehouse,
		lastOrphanScan: map[string]time.Time{},
	}
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
	}

	start := time.Now()
	tables, err := m.catalog.Tables(ctx)
	if err != nil {
		m.log.Warn("could not list tables", "err", err)
		m.opts.Metrics.pass(false)
		return
	}
	failed := 0
	for _, ident := range tables {
		if ctx.Err() != nil {
			return
		}
		if err := m.maintain(ctx, ident); err != nil {
			failed++
			m.log.Warn("table maintenance failed", "table", name(ident), "err", err)
		}
	}
	m.opts.Metrics.pass(failed == 0)
	m.record(ctx, passJob)
	m.log.Debug("maintenance pass finished", "tables", len(tables), "failed", failed, "took", time.Since(start))
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

// maintain compacts, expires and cleans one table. A commit that loses a race
// with a writer is not an error; the next pass tries again. A failure in one
// step does not stop the others.
func (m *Maintainer) maintain(ctx context.Context, ident table.Identifier) error {
	tbl, err := m.catalog.LoadTable(ctx, ident)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return nil // dropped since the listing
	}
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	log := m.log.With("table", name(ident))
	var errs []error

	switch compacted, err := m.compact(ctx, tbl, log); {
	case err == nil:
		tbl = compacted
	case errors.Is(err, table.ErrCommitFailed):
		log.Debug("compaction lost a race with a writer; retrying next pass")
	default:
		errs = append(errs, fmt.Errorf("compact: %w", err))
	}
	if tbl, err = m.catalog.LoadTable(ctx, ident); err != nil {
		return errors.Join(append(errs, fmt.Errorf("load: %w", err))...)
	}

	switch expired, err := m.expire(ctx, tbl, log); {
	case err == nil:
		tbl = expired
	case errors.Is(err, table.ErrCommitFailed):
		log.Debug("snapshot expiry lost a race with a writer; retrying next pass")
	default:
		errs = append(errs, fmt.Errorf("expire snapshots: %w", err))
	}

	if err := m.removeOrphans(ctx, tbl, log); err != nil {
		errs = append(errs, fmt.Errorf("delete orphan files: %w", err))
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

	fs, err := tbl.FS(ctx)
	if err != nil {
		return tbl, err
	}
	deadDeletes, err := compaction.CollectDeadEqualityDeletes(ctx, fs, tbl.CurrentSnapshot(), rewrite)
	if err != nil {
		return tbl, fmt.Errorf("find dead equality deletes: %w", err)
	}

	tx := tbl.NewTransaction()
	res, err := tx.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{
		ExtraDeleteFilesToRemove: deadDeletes,
		SnapshotProps:            map[string]string{"igloo.operation": "compact"},
	})
	if err != nil {
		return tbl, err
	}
	committed, err := tx.Commit(ctx)
	if err != nil {
		return tbl, err
	}
	m.opts.Metrics.compacted(res.RemovedDataFiles, res.AddedDataFiles)
	log.Info("compacted",
		"files_before", res.RemovedDataFiles, "files_after", res.AddedDataFiles,
		"delete_files_removed", res.RemovedPositionDeleteFiles+res.RemovedEqualityDeleteFiles,
		"bytes_before", res.BytesBefore, "bytes_after", res.BytesAfter)
	return committed, nil
}

// expire removes snapshots older than the table's retention, always keeping
// the current one, and deletes the files that only they referenced. Files
// that compaction replaced stay until the snapshots that read them expire,
// so time travel within the retention window keeps working.
func (m *Maintainer) expire(ctx context.Context, tbl *table.Table, log *slog.Logger) (*table.Table, error) {
	maxAge, minKeep := m.retention(tbl)
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	expirable := 0
	for _, s := range tbl.Metadata().Snapshots() {
		if s.TimestampMs < cutoff {
			expirable++
		}
	}
	if expirable == 0 || len(tbl.Metadata().Snapshots()) <= minKeep {
		return tbl, nil
	}

	before := len(tbl.Metadata().Snapshots())
	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(table.WithOlderThan(maxAge), table.WithRetainLast(minKeep)); err != nil {
		return tbl, err
	}
	committed, err := tx.Commit(ctx)
	if err != nil {
		return tbl, err
	}
	if n := before - len(committed.Metadata().Snapshots()); n > 0 {
		m.opts.Metrics.expired(n)
		log.Info("expired snapshots", "snapshots", n, "retention", maxAge)
	}
	return committed, nil
}

// retention is how long tbl keeps snapshots and how many it always keeps.
func (m *Maintainer) retention(tbl *table.Table) (time.Duration, int) {
	maxAge, minKeep := m.opts.Retention, 1
	props := tbl.Properties()
	if ms, err := strconv.ParseInt(props[maxSnapshotAgeProperty], 10, 64); err == nil && ms >= 0 {
		maxAge = time.Duration(ms) * time.Millisecond
	}
	if n, err := strconv.Atoi(props[minSnapshotsToKeepProperty]); err == nil && n >= 1 {
		minKeep = n
	}
	return maxAge, minKeep
}

func name(ident table.Identifier) string {
	return strings.Join(ident, ".")
}
