// Package memlimit divides the memory Igloo may use between DuckDB, which
// keeps to its memory_limit, and Go, whose heap the runtime otherwise grows
// without bound. Igloo runs both in one process, so unless their limits add
// up to less than the memory the process may use, the kernel kills it when
// both are busy, as when table maintenance compacts while queries run.
package memlimit

import (
	"bufio"
	"bytes"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// MinGo is the smallest memory limit Igloo gives Go.
	MinGo = 256 << 20
	// minMaintenance and maxMaintenance bound the memory table maintenance
	// gets by default.
	minMaintenance = 256 << 20
	maxMaintenance = 4 << 30
)

// Plan is how the process's memory is divided.
type Plan struct {
	// Total is the memory the process may use, and Source where that came
	// from: "cgroup" (a container's memory limit) or "machine". Total is
	// zero when neither could be read.
	Total  int64
	Source string
	// DuckDB is DuckDB's memory_limit, zero if unknown.
	DuckDB int64
	// Go is Go's soft memory limit, zero for none, and FromEnv says that
	// GOMEMLIMIT set it.
	Go      int64
	FromEnv bool
}

// Apply sets Go's memory limit to what the process's memory leaves after
// duckdb, DuckDB's memory limit, and Reserve, but at least MinGo, unless
// GOMEMLIMIT set one. It does nothing when the process's memory is unknown.
func Apply(duckdb int64) Plan {
	total, source := Total()
	p := Plan{Total: total, Source: source, DuckDB: duckdb}
	if os.Getenv("GOMEMLIMIT") != "" {
		p.Go, p.FromEnv = debug.SetMemoryLimit(-1), true
		return p
	}
	if total == 0 || duckdb == 0 {
		return p
	}
	p.Go = GoLimit(total, duckdb)
	debug.SetMemoryLimit(p.Go)
	return p
}

// Overcommitted reports whether DuckDB's and Go's limits and the reserve add
// up to more than the process may use.
func (p Plan) Overcommitted() bool {
	return p.Total > 0 && p.DuckDB > 0 && p.Go > 0 && p.DuckDB+p.Go+Reserve(p.Total) > p.Total
}

// Maintenance is the memory table maintenance gets by default: a quarter of
// Go's limit, between 256 MiB (or half of Go's limit, if less) and 4 GiB.
// It is zero without a Go limit.
func (p Plan) Maintenance() int64 {
	if p.Go <= 0 || p.Go == math.MaxInt64 {
		return 0
	}
	return min(max(p.Go/4, min(minMaintenance, p.Go/2)), maxMaintenance)
}

// Reserve is the memory of total that neither DuckDB's nor Go's limit
// counts: threads' stacks, the C library's caches, and what DuckDB
// allocates outside its buffer manager. It is a twentieth of total, at
// least 256 MiB.
func Reserve(total int64) int64 {
	return max(total/20, 256<<20)
}

// GoLimit is the memory limit for Go: what total leaves after DuckDB's
// limit and the reserve, but at least MinGo.
func GoLimit(total, duckdb int64) int64 {
	return max(total-duckdb-Reserve(total), MinGo)
}

// Total returns the memory the process may use: the least of its cgroups'
// limits (a container's memory limit) and the machine's memory. It returns
// 0 when neither can be read, as outside Linux.
func Total() (int64, string) {
	return total("/")
}

func total(root string) (int64, string) {
	machine := memTotal(filepath.Join(root, "proc/meminfo"))
	limit := cgroupLimit(root)
	switch {
	case limit > 0 && (machine == 0 || limit < machine):
		return limit, "cgroup"
	case machine > 0:
		return machine, "machine"
	default:
		return 0, ""
	}
}

// cgroupLimit returns the least memory limit of the process's cgroup and its
// parents, or 0 if none is set.
func cgroupLimit(root string) int64 {
	data, err := os.ReadFile(filepath.Join(root, "proc/self/cgroup"))
	if err != nil {
		return 0
	}
	var limit int64
	lower := func(n int64) {
		if n > 0 && (limit == 0 || n < limit) {
			limit = n
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			// cgroup v2. In a container the mount is the container's own
			// cgroup; on a host, the path leads to the process's.
			for _, dir := range candidates(filepath.Join(root, "sys/fs/cgroup"), parts[2]) {
				lower(readLimit(filepath.Join(dir, "memory.max")))
			}
		case hasItem(parts[1], "memory"):
			// cgroup v1.
			for _, dir := range candidates(filepath.Join(root, "sys/fs/cgroup/memory"), parts[2]) {
				lower(readLimit(filepath.Join(dir, "memory.limit_in_bytes")))
			}
		}
	}
	return limit
}

// hasItem reports whether the comma-separated list holds name.
func hasItem(list, name string) bool {
	for _, s := range strings.Split(list, ",") {
		if s == name {
			return true
		}
	}
	return false
}

// candidates are the directories that may hold the limits of the cgroup at
// path under mount: the cgroup's own and its parents', up to the mount.
func candidates(mount, path string) []string {
	dirs := []string{mount}
	path = filepath.Clean("/" + path)
	for path != "/" {
		dirs = append(dirs, filepath.Join(mount, path))
		path = filepath.Dir(path)
	}
	return dirs
}

// readLimit reads a cgroup memory limit file, which says "max" or a huge
// number when there is no limit. It returns 0 then, or if it cannot read it.
func readLimit(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64)
	if err != nil || n <= 0 || n >= 1<<60 {
		return 0
	}
	return n
}

// memTotal reads MemTotal from /proc/meminfo, or returns 0.
func memTotal(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close() //nolint:errcheck
	s := bufio.NewScanner(f)
	for s.Scan() {
		rest, ok := strings.CutPrefix(s.Text(), "MemTotal:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0
		}
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0
		}
		return n << 10
	}
	return 0
}

// Format writes n bytes for people, as 1.5 GiB.
func Format(n int64) string {
	switch {
	case n >= 1<<30:
		return strconv.FormatFloat(float64(n)/(1<<30), 'f', 1, 64) + " GiB"
	case n >= 1<<20:
		return strconv.FormatFloat(float64(n)/(1<<20), 'f', 1, 64) + " MiB"
	case n >= 1<<10:
		return strconv.FormatFloat(float64(n)/(1<<10), 'f', 1, 64) + " KiB"
	default:
		return strconv.FormatInt(n, 10) + " B"
	}
}
