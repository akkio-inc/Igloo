package distributed

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Bucketed tables. An Iceberg table partitioned with a bucket transform,
// bucket[N](key), keeps the rows of each bucket of the key's hash in files
// of their own: every row with a given key is in that bucket's files. A
// stage that needs its rows partitioned by the key, to join two tables on
// it, to complete groups that include it or to count its distinct values,
// or for windows partitioned by it, reads them by bucket instead of
// through an exchange: each task reads the files of some buckets, and of
// the same buckets of the other tables bucketed by the column it is joined
// on. Tables of different bucket counts meet where the smallest divides
// the others: bucket b of N rows is bucket b mod M of M rows for M dividing
// N, as the transform takes the hash modulo the count. The tables'
// key columns must hash alike: integers of any width (Iceberg hashes int
// and long alike), or the same type otherwise.

// Bucketing is a bucket transform of a table's partition spec.
type Bucketing struct {
	// Column is the column bucketed, as the table names it; N the buckets.
	Column string
	N      int
	// sourceID is the column's field ID.
	sourceID int
}

// String writes the transform as Iceberg does: bucket[16](customer_id).
func (b Bucketing) String() string { return fmt.Sprintf("bucket[%d](%s)", b.N, b.Column) }

// icebergBuckets returns the bucket transforms of tbl's partition spec that
// every data file of snap was written under: those of every partition spec
// its manifests use, of top-level columns.
func icebergBuckets(ctx context.Context, tbl *table.Table, snap *table.Snapshot) []Bucketing {
	meta := tbl.Metadata()
	spec := meta.PartitionSpec()
	var out []Bucketing
	for _, f := range spec.Fields() {
		b, ok := f.Transform.(iceberg.BucketTransform)
		if !ok || len(f.SourceIDs) != 1 || b.NumBuckets <= 0 {
			continue
		}
		name, ok := topLevelColumn(tbl.Schema(), f.SourceIDs[0])
		if !ok {
			continue
		}
		out = append(out, Bucketing{Column: name, N: b.NumBuckets, sourceID: f.SourceIDs[0]})
	}
	if len(out) == 0 || snap == nil {
		return out
	}
	// The specs the snapshot's files were written under: all of them must
	// bucket so.
	specs := map[int]bool{spec.ID(): true}
	if len(meta.PartitionSpecs()) > 1 {
		fs, err := tbl.FS(ctx)
		if err != nil {
			return nil
		}
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return nil
		}
		specs = map[int]bool{}
		for _, m := range manifests {
			specs[int(m.PartitionSpecID())] = true
		}
	}
	return slices.DeleteFunc(out, func(b Bucketing) bool {
		for id := range specs {
			s := meta.PartitionSpecByID(id)
			if s == nil || bucketField(s, b) < 0 {
				return true
			}
		}
		return false
	})
}

// topLevelColumn returns the name of schema's top-level column of field ID
// id.
func topLevelColumn(schema *iceberg.Schema, id int) (string, bool) {
	for _, f := range schema.Fields() {
		if f.ID == id {
			return f.Name, true
		}
	}
	return "", false
}

// bucketField returns the field ID of spec's partition field that buckets
// as b does, or -1.
func bucketField(spec *iceberg.PartitionSpec, b Bucketing) int {
	for _, f := range spec.Fields() {
		if t, ok := f.Transform.(iceberg.BucketTransform); ok && t.NumBuckets == b.N && len(f.SourceIDs) == 1 && f.SourceIDs[0] == b.sourceID {
			return f.FieldID
		}
	}
	return -1
}

// fileBuckets returns a data file's bucket of each of buckets, from its
// partition values under its spec, -1 for NULL; ok is false when the
// file's spec does not bucket so.
func fileBuckets(meta table.Metadata, df iceberg.DataFile, buckets []Bucketing) ([]int, bool) {
	if len(buckets) == 0 {
		return nil, true
	}
	spec := meta.PartitionSpecByID(int(df.SpecID()))
	if spec == nil {
		return nil, false
	}
	values := df.Partition()
	out := make([]int, len(buckets))
	for i, b := range buckets {
		id := bucketField(spec, b)
		if id < 0 {
			return nil, false
		}
		switch v := values[id].(type) {
		case nil:
			out[i] = -1
		case int32:
			out[i] = int(v)
		case int64:
			out[i] = int(v)
		case int:
			out[i] = v
		case *int32:
			out[i] = -1
			if v != nil {
				out[i] = int(*v)
			}
		default:
			return nil, false
		}
		if out[i] >= b.N {
			return nil, false
		}
	}
	return out, true
}

// bucketOf returns the index of t's bucketing of column name (lower case),
// or -1.
func bucketOf(t *Table, name string) int {
	return slices.IndexFunc(t.Buckets, func(b Bucketing) bool { return strings.EqualFold(b.Column, name) })
}

// --- planning -----------------------------------------------------------

// bucketKeyed reads a candidate's inputs by bucket rather than through
// exchanges where they are all tables bucketed by their key column, which
// hash alike, in counts the smallest of which divides the others (see the
// top of this file). It says why, for EXPLAIN DISTRIBUTED, either way, for
// inputs that are bucketed.
func (pl *planner) bucketKeyed(c *candidate) {
	if len(c.keyed) == 0 {
		return
	}
	var names, why []string
	var types []string
	smallest := 0
	ok := true
	for _, k := range c.keyed {
		if k.use == nil {
			ok = false
			why = append(why, k.String()+" is a relation workers compute")
			continue
		}
		t := k.use.table
		if len(k.keys) != 1 {
			ok = false
			why = append(why, fmt.Sprintf("%s is keyed by %d columns", t, len(k.keys)))
			continue
		}
		i := bucketOf(t, k.keys[0])
		if i < 0 {
			ok = false
			if len(t.Buckets) > 0 {
				why = append(why, fmt.Sprintf("%s is bucketed by %s, not by %s", t, t.Buckets[0], k.keys[0]))
			} else {
				why = append(why, fmt.Sprintf("%s is not bucketed by %s", t, k.keys[0]))
			}
			continue
		}
		b := t.Buckets[i]
		names = append(names, fmt.Sprintf("%s by %s", t, b))
		types = append(types, columnType(t, b.Column))
		if smallest == 0 || b.N < smallest {
			smallest = b.N
		}
	}
	for _, k := range c.keyed {
		if k.use == nil || len(k.keys) != 1 {
			continue
		}
		if i := bucketOf(k.use.table, k.keys[0]); i >= 0 && k.use.table.Buckets[i].N%smallest != 0 {
			ok = false
			why = append(why, fmt.Sprintf("%s's %d buckets are not a multiple of %d", k.use.table, k.use.table.Buckets[i].N, smallest))
		}
	}
	if workers := max(pl.opts.workers, 1); smallest > 0 && smallest < workers {
		// As many tasks as buckets: fewer than the workers.
		ok = false
		why = append(why, fmt.Sprintf("%d buckets are fewer than the %d workers", smallest, workers))
	}
	for _, t := range types[min(1, len(types)):] {
		if t != types[0] && !(slices.Contains(intTypes, t) && slices.Contains(intTypes, types[0])) {
			ok = false
			why = append(why, "the bucketed columns are of types that hash apart ("+strings.Join(types, ", ")+")")
			break
		}
	}
	if len(names) == 0 {
		return
	}
	if !ok {
		pl.bucketNotes = append(pl.bucketNotes, fmt.Sprintf("exchanged although %s: %s", strings.Join(names, ", "), strings.Join(why, "; ")))
		return
	}
	for _, k := range c.keyed {
		k.bucketed, k.bucket = true, bucketOf(k.use.table, k.keys[0])
	}
	pl.bucketNotes = append(pl.bucketNotes, fmt.Sprintf("no exchange: %s, read by bucket, %d groups of buckets a stage", strings.Join(names, " and "), smallest))
}

// bucketSplit reads a candidate's divided table by bucket instead of by
// shares of its files, where it is bucketed by a column its stage wants
// whole: the column it is joined on to tables bucketed alike by theirs,
// which every task then reads a share of rather than all of; one its
// groups fix, which partitions then complete (aggComplete); or the
// argument of its aggregates over DISTINCT values, which they then count
// in place. Without a table bucketed so, it changes nothing.
func (pl *planner) bucketSplit(c *candidate) {
	if c.split == nil || len(c.split.table.Buckets) == 0 || c.split.scope != c.scope || len(list(c.split.ref, "column_name_alias")) > 0 {
		return
	}
	if collation, err := pl.collation(); err != nil || collation != "" {
		// Text equal under a collation buckets apart, as it hashes apart
		// (see hashCast).
		return
	}
	if pl.bucketJoin(c) || !c.aggregating(pl) {
		return
	}
	u := c.split
	items := list(c.sel, "select_list")
	var groups []itemColumn
	for _, g := range list(c.sel, "group_expressions") {
		e := g.(node)
		// A position or an alias names a select item.
		if k, ok := intConstant(e); ok && k >= 1 && int(k) <= len(items) {
			e = items[k-1].(node)
		} else if str(e["class"]) == "COLUMN_REF" && len(columnNames(e)) == 1 {
			name := strings.ToLower(columnNames(e)[0])
			if has, certain := c.scope.hasColumn(name); !has && certain {
				for _, it := range items {
					if strings.EqualFold(str(it.(node)["alias"]), name) {
						e = it.(node)
					}
				}
			}
		}
		groups = append(groups, determined(e, c.scope)...)
	}
	single := len(list(c.sel, "group_sets")) <= 1
	for _, b := range u.table.Buckets {
		col := itemColumn{u.item, strings.ToLower(b.Column)}
		whole := single && slices.Contains(groups, col)
		if !whole && !distinctsFix(pl, c, col) || b.N < max(pl.opts.workers, 1) {
			continue
		}
		if len(list(u.ref, "column_name_alias")) > 0 {
			return
		}
		cast := ""
		if slices.Contains(intTypes, columnType(u.table, b.Column)) {
			cast = "BIGINT"
		}
		c.keyed = []*keyed{{use: u, item: u.item, keys: []string{col.name}, casts: []string{cast}, bucketed: true, bucket: bucketOf(u.table, b.Column)}}
		c.split, c.keyScope = nil, c.scope
		pl.bucketNotes = append(pl.bucketNotes, fmt.Sprintf("no exchange: %s by %s, read by bucket, whose groups or distinct values each bucket holds whole", u.table, b))
		return
	}
}

// bucketJoin reads a candidate's divided table, and the tables it is
// joined to on its bucket column that are bucketed alike by theirs, by
// bucket: each task reads a share of each, the same buckets of all, rather
// than every task all of the others. It needs a bucket at least for each
// worker the statement may run on, for as many tasks, and reports whether
// it read them so.
func (pl *planner) bucketJoin(c *candidate) bool {
	u := c.split
	classes := joinKeys(c.scope)
	for _, b := range u.table.Buckets {
		col := itemColumn{u.item, strings.ToLower(b.Column)}
		i := slices.IndexFunc(classes, func(cl []itemColumn) bool { return slices.Contains(cl, col) })
		if i < 0 {
			continue
		}
		cast := ""
		if slices.Contains(intTypes, columnType(u.table, b.Column)) {
			cast = "BIGINT"
		}
		ks := []*keyed{{use: u, item: u.item, keys: []string{col.name}, casts: []string{cast}, bucketed: true, bucket: bucketOf(u.table, b.Column)}}
		names := []string{fmt.Sprintf("%s by %s", u.table, b)}
		smallest, typ, ok := b.N, columnType(u.table, b.Column), true
		for _, v := range c.uses {
			if v == u || v.scope != c.scope || !(v.splittable || v.semi) || len(list(v.ref, "column_name_alias")) > 0 {
				continue
			}
			for _, ic := range classes[i] {
				j := bucketOf(v.table, ic.name)
				if ic.item != v.item || j < 0 {
					continue
				}
				vb, vt := v.table.Buckets[j], columnType(v.table, ic.name)
				if vt != typ && !(slices.Contains(intTypes, vt) && slices.Contains(intTypes, typ)) {
					ok = false
				}
				ks = append(ks, &keyed{use: v, item: v.item, keys: []string{ic.name}, casts: []string{cast}, bucketed: true, bucket: j})
				names = append(names, fmt.Sprintf("%s by %s", v.table, vb))
				smallest = min(smallest, vb.N)
				break
			}
		}
		for _, k := range ks {
			if k.use.table.Buckets[k.bucket].N%smallest != 0 {
				ok = false
			}
		}
		if len(ks) < 2 || !ok || smallest < max(pl.opts.workers, 1) {
			continue
		}
		c.keyed, c.split, c.keyScope = ks, nil, c.scope
		pl.bucketNotes = append(pl.bucketNotes, fmt.Sprintf("no exchange: %s, joined on their bucket columns, read by bucket, %d groups of buckets a stage",
			strings.Join(names, " and "), smallest))
		return true
	}
	return false
}

// distinctsFix reports whether every argument of the candidate's
// aggregates over DISTINCT values fixes col, and there is one.
func distinctsFix(pl *planner, c *candidate, col itemColumn) bool {
	found, all := false, true
	walk([]any{c.sel["select_list"], c.sel["having"]}, func(n node) bool {
		if !pl.isAggregate(n) {
			return true
		}
		if n["distinct"] == true {
			args := list(n, "children")
			if len(args) != 1 || !slices.Contains(determined(args[0].(node), c.scope), col) {
				all = false
			}
			found = true
		}
		return false
	})
	return found && all
}

// --- running ------------------------------------------------------------

// bucketCount is how many buckets a stage that reads tables by bucket
// reads its tables' files in, its tasks' groups: the smallest count of
// theirs; 0 for a stage that reads none so.
func (s *Stage) bucketCount() int {
	n := 0
	for _, in := range s.Inputs {
		if in.Table != nil && in.Bucketed {
			if b := in.Table.Buckets[in.Bucket].N; n == 0 || b < n {
				n = b
			}
		}
	}
	return n
}

// bucketGroups returns the buckets, of n, each task of a stage reads, and
// the files of each input read by bucket, by bucket of n: as many tasks as
// the workers run at once, twice over, or one a bucket if there are fewer,
// balanced by the bytes of their files. Rows whose key is NULL are in
// bucket 0's.
func (e *execution) bucketGroups(s *Stage, n int) ([][]int, map[string][][]File, error) {
	files := map[string][][]File{}
	bytes := make([]int64, n)
	for _, in := range s.Inputs {
		if in.Table == nil || !in.Bucketed {
			continue
		}
		byBucket := make([][]File, n)
		for _, f := range e.files[in.Table.key()] {
			if in.Bucket >= len(f.Buckets) {
				return nil, nil, fmt.Errorf("stage %d reads %s by bucket, whose file %s has no bucket", s.ID, in.Table, f.Path)
			}
			b := max(f.Buckets[in.Bucket], 0) % n
			byBucket[b] = append(byBucket[b], f)
			bytes[b] += f.Size
		}
		files[in.View] = byBucket
	}
	tasks := min(n, max(2*totalSlots(e.ws)<<e.smaller, 1))
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(bytes[b], bytes[a]) })
	groups := make([][]int, tasks)
	load := make([]int64, tasks)
	for _, b := range order {
		g := slices.Index(load, slices.Min(load))
		groups[g] = append(groups[g], b)
		load[g] += max(bytes[b], 1)
	}
	for _, g := range groups {
		slices.Sort(g)
	}
	groups = slices.DeleteFunc(groups, func(g []int) bool { return len(g) == 0 })
	return groups, files, nil
}
