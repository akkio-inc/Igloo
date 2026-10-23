package distributed

import (
	"cmp"
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

// typeWidth guesses the bytes a value of a DuckDB type takes in a row.
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
	return groupPartitions(mem, taskMemory(e.ws), len(e.ws))
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
