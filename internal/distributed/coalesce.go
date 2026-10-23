package distributed

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"strings"
)

// Partitions per task. Every exchange of a statement has as many
// partitions as its largest table needs (see partitions), so a stage that
// reads small exchanges by partition would run a task for a few thousand
// rows, and read every table it reads in full once per task: a report's
// fourteen stages over an audience of 189,000 rows each ran 32 tasks, and
// read 80 GB of tables to join 6,000 rows a task. When a stage starts, the
// exchanges it reads are complete and their partitions' rows known, so its
// tasks each read several partitions, as many as fit in a task's share of
// a worker's memory at the memory a row takes in the hash tables that join
// or aggregate it. Exchanges hash keys alike, so a task that reads
// partitions p and q of every input still finds every match of its rows.
//
// Compressed bytes would not do: an exchange of 4.4 million person IDs took
// 25 MB, 6 bytes a row, and the hash tables counting them distinct several
// hundred bytes a row; tasks grouped by bytes spilled four times as much,
// and ran out of memory with two at a time. A partition is never split,
// and every worker gets a task.

// rowMemory is the memory a row of columns takes in the operators that
// read it, conservatively: twice its width, for its columns in a hash
// table's row layout and the copies that build and probe it, and 64 bytes
// for the hash, the pointers to it and the table's slack. Aggregates over
// DISTINCT values keep a second table of their groups and values.
func rowMemory(columns []Column) int64 {
	var width int64
	for _, c := range columns {
		width += typeWidth(c.Type)
	}
	return 2*width + 64
}

// typeWidth guesses the bytes a value of a DuckDB type takes in memory, in
// a row: more than typeBytes's guess at its compressed size, for strings
// and nested values above all.
func typeWidth(t string) int64 {
	t = strings.ToUpper(strings.TrimSpace(t))
	switch {
	case strings.HasSuffix(t, "]"), strings.HasPrefix(t, "MAP"), strings.HasPrefix(t, "STRUCT"), strings.HasPrefix(t, "UNION"):
		return 64
	case strings.HasPrefix(t, "DECIMAL"):
		if open, close := strings.IndexByte(t, '('), strings.IndexByte(t, ','); open >= 0 && close > open {
			if p, err := strconv.Atoi(strings.TrimSpace(t[open+1 : close])); err == nil && p <= 18 {
				return 8
			}
		}
		return 16
	case strings.HasPrefix(t, "TIMESTAMP"), strings.HasPrefix(t, "TIME"):
		return 8
	}
	switch t {
	case "BOOLEAN", "TINYINT", "UTINYINT":
		return 1
	case "SMALLINT", "USMALLINT":
		return 2
	case "INTEGER", "UINTEGER", "FLOAT", "DATE":
		return 4
	case "BIGINT", "UBIGINT", "DOUBLE":
		return 8
	case "HUGEINT", "UHUGEINT", "UUID", "INTERVAL":
		return 16
	}
	// Strings, blobs and the rest: a short string inline, a long one out of
	// line.
	return 32
}

// taskMemory is the memory a task may plan on: half of the smallest share
// of a worker's memory limit that a task gets, its limit over the tasks it
// runs at once, or 0 if a worker does not say.
func taskMemory(ws []*worker) int64 {
	var share int64
	for _, w := range ws {
		if w.status.MemoryBytes <= 0 {
			return 0
		}
		slots := w.status.Slots
		if slots <= 0 {
			slots = taskSlots(w.status.Cores, w.status.MemoryBytes)
		}
		if m := w.status.MemoryBytes / int64(slots); share == 0 || m < share {
			share = m
		}
	}
	return share / 2
}

// exchangePartitions is how many partitions the exchanges of stages get on
// n workers, ws those known: enough for the workers and the largest table
// divided (see partitions), more if the exchanges' rows need them to fit a
// task's memory (partitionsFor), at a hundred times the data, but no more
// than their writers have memory for with wide rows (writerPartitions),
// and still one a worker.
func exchangePartitions(stages []*Stage, ws []*worker, n int) int {
	var split int64
	for _, s := range stages {
		if in := s.split(); in != nil {
			split = max(split, in.Table.Bytes)
		}
	}
	parts := max(partitions(n, split, partitionBytes), min(partitionsFor(stages, taskMemory(ws)), maxPartitions))
	if most := writerPartitions(stages, ws); most > 0 {
		parts = min(parts, max(most, n))
	}
	return parts
}

// partitionsFor returns how many partitions the exchanges of stages need
// for a task that reads one partition of each to hold its rows within
// budget, as far as estimates tell: an exchange's rows are those of the
// tables its stages read (a hundredth where a filter compares a column to
// a constant) and of the exchanges they read, each taking the memory of a
// row of the columns they read (rowMemory). Partitions that turn out
// small are read several to a task (see groups); a partition that turns
// out large is not divided, so its task spills. Without a budget it is 0.
func partitionsFor(stages []*Stage, budget int64) int {
	if budget <= 0 {
		return 0
	}
	rows, width := exchangeEstimates(stages)
	need := 0
	for x, r := range rows {
		mem := r * float64(2*width[x]+64)
		need = max(need, int(math.Ceil(mem/float64(budget))))
	}
	return need
}

// exchangeEstimates estimates each exchange's rows, as partitionsFor says,
// and the width of a row of the columns its stages read, by their types.
func exchangeEstimates(stages []*Stage) (rows map[*Exchange]float64, width map[*Exchange]int64) {
	rows, width = map[*Exchange]float64{}, map[*Exchange]int64{}
	for _, s := range stages {
		var in float64
		var w int64
		for _, i := range s.Inputs {
			switch {
			case i.Table != nil:
				r := float64(max(i.Table.Rows, 0))
				if filtersByConstant(s.fragment, i.Table) {
					r *= filterSelectivity
				}
				in += r
				cols := columnsRead(s.fragment, i.Table)
				for _, c := range i.Table.Columns {
					if cols == nil || cols[strings.ToLower(c.Name)] {
						w += typeWidth(c.Type)
					}
				}
			case i.Exchange != nil:
				in += rows[i.Exchange]
				w += width[i.Exchange]
			}
		}
		if s.Out != nil {
			rows[s.Out] += in
			width[s.Out] = max(width[s.Out], w)
		}
	}
	return rows, width
}

// writerPartitions is the most partitions the stages' tasks can write an
// exchange of: each holds a row group of each partition (see
// shuffleRowGroup), of at least minShuffleRowGroup rows, which those of
// all partitions must fit a quarter of a task's share of the worker's
// memory in, at the width of the widest exchange's rows. Rows of 400
// strings took a writer of 3 GB out of memory over 128 partitions and 256,
// even in row groups of 2,048 rows. It is 0 when the workers' memory is not
// known or no stage writes an exchange.
func writerPartitions(stages []*Stage, ws []*worker) int {
	budget := taskMemory(ws) / 2
	_, width := exchangeEstimates(stages)
	var widest int64
	for _, w := range width {
		widest = max(widest, w)
	}
	if budget <= 0 || widest <= 0 {
		return 0
	}
	return int(budget / (minShuffleRowGroup * widest))
}

// groups returns the partitions each task of stage s, which reads
// exchanges by partition, reads.
func (e *execution) groups(s *Stage) [][]int {
	mem := make([]int64, e.parts)
	for _, in := range s.Inputs {
		if in.Exchange == nil || !in.Partitioned {
			continue
		}
		x := e.exchanges[in.Exchange.ID]
		per := rowMemory(x.columns)
		for p, rows := range x.partRows {
			mem[p] += rows * per
		}
	}
	return groupPartitions(mem, taskMemory(e.ws)>>e.smaller, len(e.ws))
}

// divide splits each of shares, a worker's share of a divided table's
// files each, into up to k shares of about equal size, for smaller tasks,
// and returns them with the index of the worker each came from.
func divide(shares [][]File, k int) ([][]File, []int) {
	var out [][]File
	var owners []int
	for w, share := range shares {
		if k <= 1 || len(share) <= 1 {
			out, owners = append(out, share), append(owners, w)
			continue
		}
		pieces := make([][]File, min(k, len(share)))
		sizes := make([]int64, len(pieces))
		sorted := slices.Clone(share)
		slices.SortStableFunc(sorted, func(a, b File) int { return cmp.Compare(b.Size, a.Size) })
		for _, f := range sorted {
			j := slices.Index(sizes, slices.Min(sizes))
			pieces[j] = append(pieces[j], f)
			sizes[j] += max(f.Size, 1)
		}
		for _, p := range pieces {
			out, owners = append(out, p), append(owners, w)
		}
	}
	return out, owners
}

// groupPartitions groups partitions, of the memory mem says each takes,
// into at least tasks groups (or one each, if there are fewer), as few as
// keep each group within budget but for a partition larger than that
// alone, balanced by memory. Without a budget each partition is a group.
func groupPartitions(mem []int64, budget int64, tasks int) [][]int {
	if budget <= 0 || len(mem) <= max(tasks, 1) {
		out := make([][]int, len(mem))
		for p := range mem {
			out[p] = []int{p}
		}
		return out
	}
	n := min(len(mem), max(tasks, 1))
	groups := make([][]int, n)
	load := make([]int64, n)
	order := make([]int, len(mem))
	for p := range order {
		order[p] = p
	}
	// The largest first, each to the group that holds least, or to a group
	// of its own when that would take the group over budget.
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(mem[b], mem[a]) })
	for _, p := range order {
		g := 0
		for i := range groups {
			if load[i] < load[g] || load[i] == load[g] && len(groups[i]) < len(groups[g]) {
				g = i
			}
		}
		if len(groups[g]) > 0 && load[g]+mem[p] > budget {
			groups = append(groups, nil)
			load = append(load, 0)
			g = len(groups) - 1
		}
		groups[g] = append(groups[g], p)
		load[g] += mem[p]
	}
	for _, g := range groups {
		slices.Sort(g)
	}
	slices.SortFunc(groups, func(a, b []int) int { return cmp.Compare(a[0], b[0]) })
	return groups
}
