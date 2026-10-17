package distributed

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/akkio-inc/igloo/internal/config"
)

// Partitioned joins. When the split table is joined to a second table too
// large for every worker to read, both are partitioned by a hash of their
// join key instead, through object storage:
//
//  1. Workers read shares of each table's files, keep the rows the
//     statement's filters on that table alone let through, and write the
//     columns the statement uses to <warehouse>/_igloo_shuffle/..., one
//     directory per partition (COPY ... PARTITION_BY).
//  2. One task per partition then runs the job's fragment, unchanged, with
//     the two tables' views defined over that partition's files.
//
// Every row of either table that can join a row of the other has the same
// key, so lands in the same partition: each task sees every pair of rows the
// join can produce, once, and the partial results combine as for any job.
// Workers stay stateless: a task that fails runs again elsewhere over the
// same files, and only the files of tasks that succeeded are read.

// partitionPlan is how a candidate's two largest tables are partitioned.
type partitionPlan struct {
	other *tableUse
	// keys pair the split table's key columns with the other's.
	keys [][2]string
	// casts are the types both sides of each key are hashed as, or "".
	casts []string
}

// Partition says how a job partitions its two largest tables.
type Partition struct {
	// Sides are the split table and the other.
	Sides [2]Side
}

// Side is one of a partitioned job's tables.
type Side struct {
	// View is the name the fragment reads the table as.
	View  string
	Table *Table
	// SQL selects the rows to partition: the columns the statement uses,
	// and __h, the hash of the join key, after the statement's filters on
	// this table alone.
	SQL   string
	stage node
}

// partition plans a partitioned join of the candidate's split table and
// other, which must be joined to it directly on equal columns.
func (pl *planner) partition(c *candidate, other *tableUse) (*partitionPlan, error) {
	f := c.split
	if !other.splittable || other.scope == nil || other.scope != f.scope {
		return nil, unsupported("%s and %s, both too large to read in full, are not joined directly", f.table, other.table)
	}
	for _, u := range c.uses {
		if u != other && u.table.key() == other.table.key() {
			return nil, unsupported("%s is read more than once", other.table)
		}
	}
	for _, u := range []*tableUse{f, other} {
		if len(list(u.ref, "column_name_alias")) > 0 {
			return nil, unsupported("%s with renamed columns", u.table)
		}
	}
	p := &partitionPlan{other: other}
	for _, cond := range f.scope.conds {
		if str(cond["class"]) != "COMPARISON" || str(cond["type"]) != "COMPARE_EQUAL" {
			continue
		}
		l, r := child(cond, "left"), child(cond, "right")
		if str(l["class"]) != "COLUMN_REF" || str(r["class"]) != "COLUMN_REF" {
			continue
		}
		li, ri := resolveColumn(l, f.scope), resolveColumn(r, f.scope)
		ln, rn := lastName(l), lastName(r)
		switch {
		case li == f.item && ri == other.item:
			p.keys = append(p.keys, [2]string{ln, rn})
		case li == other.item && ri == f.item:
			p.keys = append(p.keys, [2]string{rn, ln})
		}
	}
	if len(p.keys) == 0 {
		return nil, unsupported("%s and %s, both too large to read in full, are not joined on equal columns", f.table, other.table)
	}
	for _, k := range p.keys {
		tf, tg := columnType(f.table, k[0]), columnType(other.table, k[1])
		cast, ok := hashType(tf, tg)
		if !ok {
			return nil, unsupported("join key %s (%s) = %s (%s) of different types", k[0], tf, k[1], tg)
		}
		if strings.HasPrefix(tf, "VARCHAR") {
			// Values equal under a collation do not hash alike.
			collation, err := pl.env.Setting(pl.ctx, "default_collation")
			if err != nil {
				return nil, err
			}
			if collation != "" {
				return nil, unsupported("a text join key under collation %s", collation)
			}
		}
		p.casts = append(p.casts, cast)
	}
	return p, nil
}

// resolveColumn returns the FROM item a column reference names, or nil
// when that is not certain.
func resolveColumn(ref node, sc *scope) *fromItem {
	parts := columnNames(ref)
	for i := range parts {
		parts[i] = strings.ToLower(parts[i])
	}
	switch len(parts) {
	case 1:
		return sc.owner(parts[0])
	case 2:
		var found *fromItem
		for _, it := range sc.items {
			if it.name == parts[0] && it.columns != nil && slices.Contains(it.columns, parts[1]) {
				if found != nil {
					return nil
				}
				found = it
			}
		}
		return found
	}
	return nil
}

func lastName(ref node) string {
	parts := columnNames(ref)
	return parts[len(parts)-1]
}

func columnType(t *Table, name string) string {
	for _, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c.Type
		}
	}
	return ""
}

// hashType returns the type both sides of a join key are hashed as, ""
// when they already share one. DuckDB hashes the integer types by value,
// so they mix; other types must match.
func hashType(a, b string) (string, bool) {
	ints := []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER"}
	switch {
	case a == "" || b == "":
		return "", false
	case a == b:
		return "", true
	case slices.Contains(ints, a) && slices.Contains(ints, b):
		return "BIGINT", true
	}
	return "", false
}

// stages builds the queries that select each partitioned table's rows.
func (pl *planner) stages(c *candidate, j *Job) (*Partition, error) {
	p := c.partition
	refs := referencedNames(c.sel)
	part := &Partition{}
	for i, u := range []*tableUse{c.split, p.other} {
		view := str(u.ref[viewMark])
		var keys []string
		for k, pair := range p.keys {
			e := quoteIdent(pair[i])
			if p.casts[k] != "" {
				e = "CAST(" + e + " AS " + p.casts[k] + ")"
			}
			keys = append(keys, e)
		}
		var cols []string
		for _, col := range u.table.Columns {
			if refs == nil || refs[strings.ToLower(col.Name)] {
				cols = append(cols, quoteIdent(col.Name))
			}
		}
		alias := str(u.ref["alias"])
		if alias == "" {
			alias = str(u.ref["table_name"])
		}
		stage, err := pl.p.parse("SELECT " + strings.Join(append(cols, "hash("+strings.Join(keys, ", ")+") AS __h"), ", ") +
			" FROM " + quoteIdent(view) + " AS " + quoteIdent(alias))
		if err != nil {
			return nil, err
		}
		// The statement's filters on this table alone, so less crosses.
		var filters []any
		for _, cond := range c.split.scope.conds {
			if onlyColumnsOf(cond, u.item, c.split.scope) {
				filters = append(filters, clone(cond))
			}
		}
		switch len(filters) {
		case 0:
		case 1:
			stage["where_clause"] = filters[0]
		default:
			and, err := pl.p.template("SELECT 1 WHERE true AND true")
			if err != nil {
				return nil, err
			}
			w := child(and, "where_clause")
			w["children"] = filters
			stage["where_clause"] = w
		}
		part.Sides[i] = Side{View: view, Table: u.table, stage: stage}
	}
	return part, nil
}

// referencedNames returns every name a column reference in n uses, lower
// case, or nil when a * or a NATURAL join may use any column.
func referencedNames(n node) map[string]bool {
	names := map[string]bool{}
	all := false
	walk(n, func(m node) bool {
		switch {
		case str(m["class"]) == "STAR", str(m["ref_type"]) == "NATURAL":
			all = true
		case str(m["class"]) == "COLUMN_REF":
			for _, p := range columnNames(m) {
				names[strings.ToLower(p)] = true
			}
		}
		for _, u := range list(m, "using_columns") {
			names[strings.ToLower(str(u))] = true
		}
		return !all
	})
	if all {
		return nil
	}
	return names
}

// onlyColumnsOf reports whether cond reads columns of item alone, and
// nothing that depends on more than the row: a filter that can run on
// item's rows before they are partitioned.
func onlyColumnsOf(cond node, item *fromItem, sc *scope) bool {
	ok, any := true, false
	walk(cond, func(m node) bool {
		switch str(m["class"]) {
		case "SUBQUERY", "WINDOW", "PARAMETER", "LAMBDA", "STAR":
			ok = false
		case "COLUMN_REF":
			any = true
			if resolveColumn(m, sc) != item {
				ok = false
			}
		}
		return ok
	})
	return ok && any
}

// --- running a partitioned job ----------------------------------------------

// maxPartitions caps a partitioned job's partitions, and so its tasks.
const maxPartitions = 64

// runPartitioned runs a partitioned job: the tables' rows are written to
// object storage by partition, then one task per partition runs the
// fragment.
func (co *Coordinator) runPartitioned(ctx context.Context, qid, dir string, j *Job, ws []*worker, files map[string][]File, settings []setting, sp *spool) (*jobResult, error) {
	parts := min(maxPartitions, max(2, 2*len(ws)))
	root := co.opts.Warehouse + config.ShuffleDir + "/" + qid + "/" + strconv.Itoa(j.ID)
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := co.opts.Source.RemoveAll(ctx, j.Split, root); err != nil {
			co.log.Warn("could not remove partitioned rows", "dir", root, "err", err)
		}
	}()

	// Stage one: both tables, partitioned.
	type sideResult struct {
		columns []Column
		files   [][]string
		rows    int64
	}
	var sides [2]sideResult
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for s, side := range j.Partition.Sides {
		sides[s].files = make([][]string, parts)
		shares := assign(files[side.Table.key()], ws)
		for i, share := range shares {
			task := &taskRequest{
				Query: qid, Task: j.ID*10000 + 1000*(s+1) + i, Version: co.opts.Version, Settings: settings,
				SQL:     fmt.Sprintf("SELECT * EXCLUDE (__h), CAST(__h %% %d AS INTEGER) AS __b FROM (%s)", parts, side.SQL),
				Views:   []viewDef{{Name: side.View, Columns: side.Table.Columns, Files: paths(share), Rows: rowCount(share)}},
				Shuffle: root + "/" + strconv.Itoa(s) + "/" + strconv.Itoa(i),
			}
			var bytes int64
			for _, f := range share {
				bytes += f.Size
			}
			g.Go(func() error {
				_, head, err := co.dispatch(gctx, task, ws, i, "", nil)
				if err != nil {
					return err
				}
				co.opts.Metrics.scan(bytes)
				mu.Lock()
				defer mu.Unlock()
				sides[s].rows += head.Rows
				if sides[s].columns == nil {
					sides[s].columns = head.Columns
				} else if !slices.Equal(sides[s].columns, head.Columns) {
					return fmt.Errorf("workers partitioned %s with different columns", side.Table)
				}
				for _, f := range head.Files {
					if f.Partition < 0 || f.Partition >= parts || !strings.HasPrefix(f.Path, task.Shuffle+"/") {
						return fmt.Errorf("a worker wrote an unexpected file %s", f.Path)
					}
					sides[s].files[f.Partition] = append(sides[s].files[f.Partition], f.Path)
				}
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Stage two: a task per partition, over that partition of both tables
	// and all of the others.
	var others []viewDef
	for _, v := range j.Views {
		if v.Name == j.Partition.Sides[0].View || v.Name == j.Partition.Sides[1].View {
			continue
		}
		def := viewDef{Name: v.Name, Columns: v.Table.Columns, Rows: rowCount(files[v.Table.key()])}
		for _, f := range files[v.Table.key()] {
			def.Files = append(def.Files, f.Path)
		}
		others = append(others, def)
	}
	res := &jobResult{files: make([]string, parts), workers: map[string]bool{}}
	g, gctx = errgroup.WithContext(ctx)
	for p := range parts {
		views := slices.Clone(others)
		for s, side := range j.Partition.Sides {
			// The hash spreads rows evenly over the partitions.
			views = append(views, viewDef{Name: side.View, Columns: sides[s].columns, Files: sides[s].files[p], ByName: true,
				Rows: max(1, sides[s].rows/int64(parts))})
		}
		task := &taskRequest{Query: qid, Task: j.ID*10000 + p, Version: co.opts.Version, Settings: settings,
			SQL: j.Fragment, Columns: j.Columns, Views: views}
		out := filepath.Join(dir, fmt.Sprintf("%d-%d.parquet", j.ID, p))
		g.Go(func() error {
			w, head, err := co.dispatch(gctx, task, ws, p%len(ws), out, sp)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			res.files[p] = out
			res.workers[w.addr] = true
			if res.columns == nil {
				res.columns = head.Columns
			} else if !slices.Equal(res.columns, head.Columns) {
				return fmt.Errorf("workers returned different columns for job %d", j.ID)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return res, nil
}
