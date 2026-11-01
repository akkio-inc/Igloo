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
//
// An input joined to such a table on its bucket column that is not itself
// bucketed so, a relation workers compute or another table, is shuffled into
// the table's buckets: its stage writes each row to the partition of the
// key's Iceberg bucket, with iceberg_bucket, the function DuckDB writes
// bucketed tables with, and the table's rows stay where they are, each task
// reading the files of some buckets and the partitions those buckets are
// written to. Only one key, the column itself on the table's side, and
// types Iceberg hashes by value alone qualify (bucketHashes): a key
// computed from the column, or counts that do not divide, are exchanged as
// before.
//
// A subquery that keeps its rows' keys as they are makes rows that a bucket
// of its table holds whole, by filtering and projecting one such table, by
// joining such tables on the key, or by grouping one with the key among the
// groups: it is read where it stands, its tables by bucket, rather than
// written to an exchange (through). Only one read once, in place of its
// name: a common table expression that several parts read is computed once
// and written to their buckets, which scans its table once.

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
	var names, others, why []string
	var types []string
	smallest := 0
	ok := true
	for _, k := range c.keyed {
		if len(k.keys) != 1 {
			ok = false
			why = append(why, fmt.Sprintf("%s is keyed by %d columns", k, len(k.keys)))
			continue
		}
		if keyExpression(k.casts[0]) {
			// Keyed by an expression of the column, which its buckets
			// do not hold apart.
			ok = false
			why = append(why, fmt.Sprintf("%s is keyed by %s", k, fmt.Sprintf(k.casts[0], k.keys[0])))
			continue
		}
		i := -1
		if k.use != nil {
			i = bucketOf(k.use.table, k.tableColumn(k.keys[0]))
		}
		if i < 0 && k.through != nil {
			ok = false
			continue
		}
		if i < 0 {
			others = append(others, k.String())
			types = append(types, k.columnType(k.keys[0]))
			continue
		}
		t := k.use.table
		b := t.Buckets[i]
		names = append(names, fmt.Sprintf("%s by %s", t, b))
		types = append(types, columnType(t, b.Column))
		if smallest == 0 || b.N < smallest {
			smallest = b.N
		}
	}
	for _, k := range c.keyed {
		if k.use == nil || len(k.keys) != 1 || smallest == 0 {
			continue
		}
		if i := bucketOf(k.use.table, k.tableColumn(k.keys[0])); i >= 0 && k.use.table.Buckets[i].N%smallest != 0 {
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
			why = append(why, "the key columns are of types that hash apart ("+strings.Join(types, ", ")+")")
			break
		}
	}
	if len(others) > 0 && len(types) > 0 && !bucketHashes(types[0]) {
		ok = false
		why = append(why, fmt.Sprintf("%s cannot be written to buckets of %s", strings.Join(others, ", "), types[0]))
	}
	if len(names) == 0 {
		return
	}
	if !ok {
		pl.bucketNotes = append(pl.bucketNotes, fmt.Sprintf("exchanged although %s: %s", strings.Join(names, ", "), strings.Join(why, "; ")))
		return
	}
	for _, k := range c.keyed {
		if k.use != nil && bucketOf(k.use.table, k.tableColumn(k.keys[0])) >= 0 {
			k.bucketed, k.bucket = true, bucketOf(k.use.table, k.tableColumn(k.keys[0]))
		} else {
			k.into = smallest
		}
	}
	note := fmt.Sprintf("no exchange: %s, read by bucket, %d groups of buckets a stage", strings.Join(names, " and "), smallest)
	if len(others) > 0 {
		note += fmt.Sprintf("; %s written to those buckets", strings.Join(others, ", "))
	}
	pl.bucketNotes = append(pl.bucketNotes, note)
}

// bucketHashes reports whether iceberg_bucket hashes values of a key's type
// as Iceberg's spec has every engine that writes a table hash a column of
// the type: integers of any width, cast to BIGINT, and others its spec's
// values show it does (see TestIcebergBucketHash). A UUID it hashes
// otherwise: a table another engine wrote has its keys in other buckets.
func bucketHashes(t string) bool {
	return slices.Contains(intTypes, t) || t == "VARCHAR" || t == "DATE" || t == "TIMESTAMP" || t == "TIMESTAMP WITH TIME ZONE" || strings.HasPrefix(t, "DECIMAL(")
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

// through is how a subquery passes on the columns of a table it reads.
type through struct {
	// item is the subquery's, in the candidate's FROM clause.
	item *fromItem
	// columns maps its columns that are the table's unchanged, by its names,
	// to the table's.
	columns map[string]string
}

// tableColumn returns the table's column that a key column of a table read
// by a keyed input is, or "" when a subquery computes it.
func (k *keyed) tableColumn(name string) string {
	if k.through != nil {
		return k.through.columns[name]
	}
	return name
}

// throughOf returns how the subqueries around a bucketed table pass on its
// columns to an item of the candidate's FROM clause, if they each read it
// alone, or the innermost joins it, and may be read by bucket (see the top of
// this file); nil otherwise.
func (pl *planner) throughOf(c *candidate, u *tableUse) *through {
	if c.noThrough || len(u.table.Buckets) == 0 || u.scope == nil || u.scope.ref == nil || !(u.splittable || u.outerSub || u.semi || u.grouped) {
		return nil
	}
	if collation, err := pl.collation(); err != nil || collation != "" {
		// Text equal under a collation buckets apart (see hashCast).
		return nil
	}
	names := map[string]string{}
	for _, col := range u.item.columns {
		names[col] = col
	}
	item := u.item
	for sc := u.scope; ; sc = sc.outer {
		if sc == nil || sc.ref == nil || sc.item == nil || sc.item.columns == nil {
			return nil
		}
		var passed []string
		switch {
		case len(sc.items) == 1 && sc.items[0] == item:
			passed = passedColumns(subqueryNode(sc.ref), item)
		case sc == u.scope && u.splittable:
			passed = joinedColumns(subqueryNode(sc.ref), item, sc)
		}
		if passed == nil || len(passed) != len(sc.item.columns) {
			return nil
		}
		next := map[string]string{}
		for i, col := range sc.item.columns {
			if t, ok := names[passed[i]]; ok && passed[i] != "" {
				next[col] = t
			}
		}
		names, item = next, sc.item
		if sc.outer == c.scope {
			return &through{item: item, columns: names}
		}
	}
}

// passedColumns returns what each column of sel, in order, is of its one
// FROM item: the item's column, or "" for a value computed; nil when that
// is not known.
func passedColumns(sel node, item *fromItem) []string {
	var out []string
	for _, it := range list(sel, "select_list") {
		e := it.(node)
		switch str(e["class"]) {
		case "COLUMN_REF":
			parts := columnNames(e)
			if len(parts) > 2 || len(parts) == 2 && !strings.EqualFold(parts[0], item.name) {
				return nil
			}
			out = append(out, strings.ToLower(parts[len(parts)-1]))
		case "STAR":
			rel := str(e["relation_name"])
			if e["columns"] == true || len(list(e, "exclude_list")) > 0 || len(list(e, "replace_list")) > 0 ||
				len(list(e, "rename_list")) > 0 || child(e, "expr") != nil || rel != "" && !strings.EqualFold(rel, item.name) {
				return nil
			}
			out = append(out, item.columns...)
		default:
			out = append(out, "")
		}
	}
	return out
}

// joinedColumns returns what each column of sel, in order, is of item, one of
// the items its FROM clause sc joins: the item's column, or that of another
// item it is joined on, the same value, or "" for anything else; nil when
// that is not known.
func joinedColumns(sel node, item *fromItem, sc *scope) []string {
	classes := joinKeys(sc)
	var out []string
	for _, it := range list(sel, "select_list") {
		e := it.(node)
		switch str(e["class"]) {
		case "STAR":
			return nil
		case "COLUMN_REF":
			from := resolveColumn(e, sc)
			name := strings.ToLower(lastName(e))
			col := ""
			if from == item {
				col = name
			} else if from != nil {
				for _, class := range classes {
					if !slices.Contains(class, itemColumn{from, name}) {
						continue
					}
					if i := slices.IndexFunc(class, func(ic itemColumn) bool { return ic.item == item }); i >= 0 {
						col = class[i].name
					}
				}
			}
			out = append(out, col)
		default:
			out = append(out, "")
		}
	}
	return out
}

// groupsByBucket reports whether q groups the rows of one bucketed table,
// in as many buckets as workers, so that a bucket of the table holds its
// groups whole if its bucket column is among them: planned to read the
// table by bucket where q stands, if a key of the statement is that column
// (groupedFrom), else q is a relation.
func (pl *planner) groupsByBucket(q node, ctes cteScope) bool {
	if pl.noGrouped || str(q["type"]) != "SELECT_NODE" || len(list(q, "group_expressions")) == 0 || len(list(q, "group_sets")) > 1 ||
		len(list(child(q, "cte_map"), "map")) > 0 || child(q, "sample") != nil || child(q, "qualify") != nil || len(list(q, "modifiers")) > 0 ||
		str(q["aggregate_handling"]) != "STANDARD_HANDLING" || hasClass(q["select_list"], "WINDOW") {
		return false
	}
	from := child(q, "from_table")
	if str(from["type"]) != "BASE_TABLE" || child(from, "sample") != nil || child(from, "at_clause") != nil || len(list(from, "column_name_alias")) > 0 {
		return false
	}
	parts := tableParts(from)
	if len(parts) == 1 && ctes.find(parts[0]) != nil {
		return false
	}
	t, err := pl.table(parts)
	if err != nil {
		return false
	}
	return slices.ContainsFunc(t.Buckets, func(b Bucketing) bool { return b.N >= max(pl.opts.workers, 1) })
}

// groupedFrom adds a subquery groupsByBucket accepted, ref of query sub, to
// sc and rd: its table may be read by bucket, and never by shares; its
// subquery expressions as any.
func (pl *planner) groupedFrom(ref, sub node, item *fromItem, splittable bool, ctes cteScope, sc *scope, rd *reads) error {
	cols, err := pl.relationColumns(sub, ctes, nil)
	if err != nil {
		return err
	}
	inner := &scope{ref: ref, outer: sc, splittable: splittable}
	uses := len(rd.uses)
	if err := pl.from(child(sub, "from_table"), false, ctes, inner, rd); err != nil {
		return err
	}
	for _, u := range rd.uses[uses:] {
		u.grouped = true
	}
	for _, part := range []string{"select_list", "where_clause", "having"} {
		if err := pl.filterSubqueries(sub[part], ctes, rd); err != nil {
			return err
		}
	}
	for _, col := range cols {
		item.columns = append(item.columns, strings.ToLower(col.Name))
	}
	renameColumns(item, list(ref, "column_name_alias"))
	sc.items = append(sc.items, item)
	inner.item = item
	pl.grouped++
	return nil
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

// bucketGroups returns the buckets, of count, each task of a stage reads,
// and the files of each input read by bucket, by bucket of count: as many
// tasks as the workers run at once, twice over, or one a bucket if there are
// fewer, balanced by the bytes of their files. Rows whose key is NULL are in
// bucket 0's. A stage that also reads exchanges written to those buckets
// reads partition p of them with the buckets that come to p modulo the
// partitions: where the partitions are fewer than the buckets, what it
// groups, and what the groups list, are those partitions.
func (e *execution) bucketGroups(s *Stage, count int) ([][]int, map[string][][]File, error) {
	n := count
	if s.partitioned() {
		n = min(count, e.parts)
	}
	files := map[string][][]File{}
	bytes := make([]int64, n)
	for _, in := range s.Inputs {
		if in.Exchange != nil && in.Partitioned {
			if in.Exchange.Buckets != count {
				return nil, nil, fmt.Errorf("stage %d reads tables by %d buckets beside exchange %d, written to %d", s.ID, count, in.Exchange.ID, in.Exchange.Buckets)
			}
			for p, b := range e.exchanges[in.Exchange.ID].partBytes {
				if p < n {
					bytes[p] += b
				}
			}
		}
		if in.Table == nil || !in.Bucketed {
			continue
		}
		byBucket := make([][]File, n)
		for _, f := range e.files[in.Table.key()] {
			if in.Bucket >= len(f.Buckets) {
				return nil, nil, fmt.Errorf("stage %d reads %s by bucket, whose file %s has no bucket", s.ID, in.Table, f.Path)
			}
			b := max(f.Buckets[in.Bucket], 0) % count % n
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
