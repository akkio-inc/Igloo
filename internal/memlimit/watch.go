package memlimit

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DuckDB keeps to its memory_limit only for the memory its buffer manager
// tracks. Some operators allocate outside it: a reservoir sample (USING
// SAMPLE n ROWS) of 600,000 rows of 60 text columns peaked at 7.8 GB under a
// 500 MB limit, and one of a million rows of a 384-column table took a
// replica on a 32 GB machine down with it. So Igloo also watches what the
// process can still allocate, and when that runs low cancels the statement
// that has run longest (see Watch) rather than wait for the kernel to kill
// the process, and every session with it.

// Low is the memory left, of total, below which Watch sheds work: a
// sixteenth of it, at least 256 MiB. Memory runs out at hundreds of
// megabytes a second when it does: on a 61 GiB machine, a statement that
// took DuckDB 8.5 GiB past its limit was canceled with 1.6 GiB left, under
// a second before it would have run out, when Low was a thirty-second.
func Low(total int64) int64 {
	return max(total/16, 256<<20)
}

// settle is how long Watch gives a canceled statement to let go of its
// memory before it sheds again, unless memory keeps running out faster.
const settle = 15 * time.Second

// Watch checks five times a second what the process can still allocate
// (Available) and, while that is below Low(total), calls shed, which
// cancels work and reports whether it did. After shedding it waits for the
// memory to come back: it sheds again only once settle has passed with
// memory still low, or sooner if what is left falls by another half of
// Low(total). It returns when ctx ends, and does nothing when total is 0.
func Watch(ctx context.Context, total int64, shed func(available int64) bool) {
	watch(ctx, total, 200*time.Millisecond, Available, shed)
}

// WatchFunc is Watch with what the process can still allocate read from
// available every interval, for tests of what shed cancels.
func WatchFunc(ctx context.Context, total int64, every time.Duration, available func() (int64, bool), shed func(available int64) bool) {
	watch(ctx, total, every, available, shed)
}

func watch(ctx context.Context, total int64, every time.Duration, available func() (int64, bool), shed func(int64) bool) {
	if total <= 0 {
		return
	}
	low := Low(total)
	t := time.NewTicker(every)
	defer t.Stop()
	var shedAt time.Time
	var shedLeft int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		left, ok := available()
		if !ok || left >= low {
			continue
		}
		if !shedAt.IsZero() && time.Since(shedAt) < settle && left > shedLeft-low/2 {
			continue
		}
		if shed(left) {
			shedAt, shedLeft = time.Now(), left
		}
	}
}

// Available returns the memory the process can still allocate before the
// kernel's out-of-memory killer acts: the least of what its cgroups'
// limits leave of their working sets (usage less the page cache the kernel
// reclaims first) and the machine's MemAvailable. ok is false when none of
// these can be read, as outside Linux.
func Available() (int64, bool) {
	return available("/")
}

func available(root string) (int64, bool) {
	var left int64
	ok := false
	lower := func(n int64) {
		if !ok || n < left {
			left, ok = n, true
		}
	}
	if n, found := meminfoField(filepath.Join(root, "proc/meminfo"), "MemAvailable:"); found {
		lower(n)
	}
	data, err := os.ReadFile(filepath.Join(root, "proc/self/cgroup"))
	if err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) != 3 {
				continue
			}
			switch {
			case parts[0] == "0" && parts[1] == "":
				for _, dir := range candidates(filepath.Join(root, "sys/fs/cgroup"), parts[2]) {
					if limit := readLimit(filepath.Join(dir, "memory.max")); limit > 0 {
						used := readCounter(filepath.Join(dir, "memory.current")) - stat(filepath.Join(dir, "memory.stat"), "inactive_file")
						lower(limit - used)
					}
				}
			case hasItem(parts[1], "memory"):
				for _, dir := range candidates(filepath.Join(root, "sys/fs/cgroup/memory"), parts[2]) {
					if limit := readLimit(filepath.Join(dir, "memory.limit_in_bytes")); limit > 0 {
						used := readCounter(filepath.Join(dir, "memory.usage_in_bytes")) - stat(filepath.Join(dir, "memory.stat"), "total_inactive_file")
						lower(limit - used)
					}
				}
			}
		}
	}
	return max(left, 0), ok
}

// readCounter reads a cgroup file that holds one number, or returns 0.
func readCounter(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return n
}

// stat reads one counter, in bytes, of a cgroup's memory.stat, or 0.
func stat(path, name string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			n, _ := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
			return n
		}
	}
	return 0
}

// meminfoField reads a field of /proc/meminfo in bytes.
func meminfoField(path, field string) (int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close() //nolint:errcheck
	s := bufio.NewScanner(f)
	for s.Scan() {
		rest, ok := strings.CutPrefix(s.Text(), field)
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, false
		}
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, false
		}
		return n << 10, true
	}
	return 0, false
}
