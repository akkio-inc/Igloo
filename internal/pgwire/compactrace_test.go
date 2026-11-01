package pgwire

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/icebergtest"
	"github.com/akkio-inc/igloo/internal/maintenance"
)

// compactRace is a server on an Iceberg catalog whose tables maintenance
// compacts when a test says, for TestCompactionGivesWayToUserWrites.
type compactRace struct {
	*refreshEnv
	hooked maintenance.Options
}

func (r *compactRace) maintainer(opts maintenance.Options) *maintenance.Maintainer {
	opts.Retention = 7 * 24 * time.Hour
	return maintenance.New(r.catalog, r.dir, opts, slog.New(slog.DiscardHandler))
}

// fill makes a table of six rows, in six small files to compact.
func (r *compactRace) fill(t *testing.T, name string) {
	t.Helper()
	mustExec(t, r.admin, "CREATE TABLE "+name+" (k INTEGER, v INTEGER)")
	for i := range 6 {
		mustExec(t, r.admin, fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", name, i, i))
	}
}

// merge updates one row of a filled table and inserts another, as dbt's
// merge and microbatch models do.
func compactMerge(name string) string {
	return "MERGE INTO " + name + " AS t USING (VALUES (1, 100), (99, 9)) AS s(k, v) ON t.k = s.k " +
		"WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v)"
}

var (
	mergedRows   = []string{"0|0", "1|100", "2|2", "3|3", "4|4", "5|5", "99|9"}
	unmergedRows = []string{"0|0", "1|1", "2|2", "3|3", "4|4", "5|5"}
)

func (r *compactRace) rows(t *testing.T, name string, want []string) {
	t.Helper()
	if got := sortedTexts(t, r.admin, "SELECT k, v FROM "+name); !slices.Equal(got, want) {
		t.Errorf("%s: rows %v, want %v", name, got, want)
	}
}

// compacted reports whether maintenance has committed a rewrite of the
// table's files.
func (r *compactRace) compacted(t *testing.T, name string) bool {
	t.Helper()
	meta := r.fake.Metadata("public", name)
	if meta == nil {
		t.Fatalf("no table %s", name)
	}
	return slices.ContainsFunc(meta.Snapshots(), func(s table.Snapshot) bool {
		return s.Summary != nil && s.Summary.Operation == table.OpReplace
	})
}

// block has a transaction block MERGE into the table once compaction has
// written the table's new files, just before it commits them (in
// Options.Hold, which then asks hold), and returns what the block's COMMIT
// returned and how many commits the catalog refused meanwhile.
func (r *compactRace) block(t *testing.T, name string, hold func(context.Context, string) (func(), bool)) (error, int) {
	t.Helper()
	ctx := context.Background()
	r.fill(t, name)
	block := r.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	opened := false
	opts := r.hooked
	opts.Hold = func(ctx context.Context, table string) (func(), bool) {
		if table == "public."+name && !opened {
			opened = true
			mustExec(t, block, "BEGIN")
			mustExec(t, block, compactMerge(name))
		}
		return hold(ctx, table)
	}
	before := r.fake.Conflicts()
	r.maintainer(opts).RunOnce(ctx)
	if !opened {
		t.Fatalf("%s: compaction never reached its commit", name)
	}
	_, err := block.Exec(ctx, "COMMIT")
	return err, r.fake.Conflicts() - before
}

// autocommit runs a MERGE into the table on its own, with maintenance
// making a pass, with opts, while the MERGE's commit waits at the catalog,
// and returns how many commits the catalog refused meanwhile.
func (r *compactRace) autocommit(t *testing.T, name string, opts maintenance.Options) int {
	t.Helper()
	ctx := context.Background()
	r.fill(t, name)
	var fired atomic.Bool
	r.fake.BeforeCommit(func(c icebergtest.Commit) {
		if c.Transaction && slices.Contains(c.Tables, "public."+name) && fired.CompareAndSwap(false, true) {
			r.maintainer(opts).RunOnce(ctx)
		}
	})
	defer r.fake.BeforeCommit(nil)
	before := r.fake.Conflicts()
	mustExec(t, r.admin, compactMerge(name))
	if !fired.Load() {
		t.Fatalf("%s: the MERGE never committed through a transaction", name)
	}
	r.rows(t, name, mergedRows)
	return r.fake.Conflicts() - before
}

// TestCompactionGivesWayToUserWrites makes maintenance's compaction commit
// between a user's MERGE reading a table and committing, the race that
// failed dbt's microbatch models: dbt runs each batch's MERGE in BEGIN ...
// COMMIT, which Igloo cannot run again, and the COMMIT failed with
// CatalogCommitConflicts. The order is forced, not timed: a block is open
// when compaction starts; or a block opens and reads the table while
// compaction writes the table's files, just before it commits them; or a
// block lists the table's columns, as dbt does before each MERGE, and
// compaction runs before its MERGE; or an autocommit MERGE is committing.
// Each time compaction gives way, the user's commit succeeds the first
// time, the MERGE applied once, and the next pass compacts. Without Igloo's
// hooks, the same orders make the block's COMMIT fail, and the autocommit
// MERGE run twice to commit once.
func TestCompactionGivesWayToUserWrites(t *testing.T) {
	env := startRefreshServer(t)
	r := &compactRace{refreshEnv: env, hooked: maintenance.Options{Busy: env.srv.InTransaction, Hold: env.srv.HoldTable}}
	ctx := context.Background()

	t.Run("a block open as compaction starts", func(t *testing.T) {
		r.fill(t, "open_block")
		block := r.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, block, "BEGIN")
		mustExec(t, block, compactMerge("open_block"))
		before := r.fake.Conflicts()
		r.maintainer(r.hooked).RunOnce(ctx)
		if r.compacted(t, "open_block") {
			t.Fatal("compacted a table an open block had written")
		}
		mustExec(t, block, "COMMIT")
		r.rows(t, "open_block", mergedRows)
		r.maintainer(r.hooked).RunOnce(ctx)
		if !r.compacted(t, "open_block") || r.fake.Conflicts() != before {
			t.Errorf("once the block ended: compacted %v, conflicts %d", r.compacted(t, "open_block"), r.fake.Conflicts()-before)
		}
		r.rows(t, "open_block", mergedRows)
	})

	t.Run("a block that reads the table while compaction writes its files", func(t *testing.T) {
		err, conflicts := r.block(t, "racing_block", env.srv.HoldTable)
		if err != nil || conflicts != 0 || r.compacted(t, "racing_block") {
			t.Fatalf("COMMIT: %v, conflicts %d, compacted %v", err, conflicts, r.compacted(t, "racing_block"))
		}
		r.rows(t, "racing_block", mergedRows)
		r.maintainer(r.hooked).RunOnce(ctx)
		if !r.compacted(t, "racing_block") {
			t.Error("not compacted once the block ended")
		}
		r.rows(t, "racing_block", mergedRows)
	})

	t.Run("the same race without the hold", func(t *testing.T) {
		err, conflicts := r.block(t, "unheld_block", func(context.Context, string) (func(), bool) { return func() {}, true })
		if err == nil || !strings.Contains(err.Error(), "CatalogCommitConflicts") || conflicts != 1 || !r.compacted(t, "unheld_block") {
			t.Fatalf("COMMIT: %v, conflicts %d, compacted %v: the test no longer forces the race", err, conflicts, r.compacted(t, "unheld_block"))
		}
		r.rows(t, "unheld_block", unmergedRows)
	})

	// dbt lists the table's columns before each MERGE, in the block: the
	// listing loads the table into the block's transaction as it runs, and
	// the MERGE, prepared after compaction, loads nothing.
	listed := func(t *testing.T, name string, opts maintenance.Options) (error, int) {
		r.fill(t, name)
		block := r.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, block, "BEGIN")
		texts(t, block, "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '"+name+"'")
		before := r.fake.Conflicts()
		r.maintainer(opts).RunOnce(ctx)
		mustExec(t, block, compactMerge(name))
		_, err := block.Exec(ctx, "COMMIT")
		return err, r.fake.Conflicts() - before
	}

	t.Run("a block that listed the table's columns", func(t *testing.T) {
		err, conflicts := listed(t, "listed_block", r.hooked)
		if err != nil || conflicts != 0 || r.compacted(t, "listed_block") {
			t.Fatalf("COMMIT: %v, conflicts %d, compacted %v", err, conflicts, r.compacted(t, "listed_block"))
		}
		r.rows(t, "listed_block", mergedRows)
	})

	t.Run("the listing without the hooks", func(t *testing.T) {
		err, conflicts := listed(t, "unhooked_listing", maintenance.Options{})
		if err == nil || conflicts != 1 || !r.compacted(t, "unhooked_listing") {
			t.Fatalf("COMMIT: %v, conflicts %d, compacted %v: the listing no longer loads the table into the block",
				err, conflicts, r.compacted(t, "unhooked_listing"))
		}
		r.rows(t, "unhooked_listing", unmergedRows)
	})

	t.Run("an autocommit MERGE committing", func(t *testing.T) {
		if conflicts := r.autocommit(t, "committing_merge", r.hooked); conflicts != 0 || r.compacted(t, "committing_merge") {
			t.Fatalf("conflicts %d, compacted %v", conflicts, r.compacted(t, "committing_merge"))
		}
		r.maintainer(r.hooked).RunOnce(ctx)
		if !r.compacted(t, "committing_merge") {
			t.Error("not compacted once the MERGE ended")
		}
		r.rows(t, "committing_merge", mergedRows)
	})

	t.Run("the same without the hooks", func(t *testing.T) {
		if conflicts := r.autocommit(t, "unhooked_merge", maintenance.Options{}); conflicts != 1 || !r.compacted(t, "unhooked_merge") {
			t.Fatalf("conflicts %d, compacted %v: the test no longer forces the race", conflicts, r.compacted(t, "unhooked_merge"))
		}
	})
}
