package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

// files writes files under a fake root directory.
func files(t *testing.T, contents map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, data := range contents {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const meminfo = "MemTotal:       31457280 kB\nMemFree:         1000000 kB\n"

func TestTotal(t *testing.T) {
	for _, c := range []struct {
		name   string
		files  map[string]string
		want   int64
		source string
	}{
		{"no limit", map[string]string{
			"proc/meminfo":             meminfo,
			"proc/self/cgroup":         "0::/\n",
			"sys/fs/cgroup/memory.max": "max\n",
		}, 30 << 30, "machine"},
		{"container (cgroup v2)", map[string]string{
			"proc/meminfo":             meminfo,
			"proc/self/cgroup":         "0::/\n",
			"sys/fs/cgroup/memory.max": "8589934592\n",
		}, 8 << 30, "cgroup"},
		{"systemd service (cgroup v2): the least limit up the tree", map[string]string{
			"proc/meminfo":                                        meminfo,
			"proc/self/cgroup":                                    "0::/system.slice/igloo.service\n",
			"sys/fs/cgroup/system.slice/memory.max":               "17179869184\n",
			"sys/fs/cgroup/system.slice/igloo.service/memory.max": "max\n",
		}, 16 << 30, "cgroup"},
		{"container (cgroup v1)", map[string]string{
			"proc/meminfo":     meminfo,
			"proc/self/cgroup": "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n",
			"sys/fs/cgroup/memory/memory.limit_in_bytes": "4294967296\n",
		}, 4 << 30, "cgroup"},
		{"cgroup v1 without a limit", map[string]string{
			"proc/meminfo":     meminfo,
			"proc/self/cgroup": "4:memory:/\n",
			"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
		}, 30 << 30, "machine"},
		{"limit above the machine's memory", map[string]string{
			"proc/meminfo":             meminfo,
			"proc/self/cgroup":         "0::/\n",
			"sys/fs/cgroup/memory.max": "68719476736\n",
		}, 30 << 30, "machine"},
		{"nothing to read", map[string]string{}, 0, ""},
	} {
		got, source := total(files(t, c.files))
		if got != c.want || source != c.source {
			t.Errorf("%s: %d from %q, want %d from %q", c.name, got, source, c.want, c.source)
		}
	}
}

func TestPlan(t *testing.T) {
	// The incident's machine: 30 GiB, DuckDB at its default of 80%.
	total := int64(30 << 30)
	duckdb := total * 8 / 10
	p := Plan{Total: total, DuckDB: duckdb, Go: GoLimit(total, duckdb)}
	if p.Go != total-duckdb-total/20 || p.Overcommitted() {
		t.Errorf("Go gets %s, overcommitted %v", Format(p.Go), p.Overcommitted())
	}
	if m := p.Maintenance(); m != p.Go/4 {
		t.Errorf("maintenance gets %s of %s", Format(m), Format(p.Go))
	}

	// DuckDB given all of it: Go still gets its minimum, and the plan says
	// it does not fit.
	p = Plan{Total: total, DuckDB: total, Go: GoLimit(total, total)}
	if p.Go != MinGo || !p.Overcommitted() {
		t.Errorf("DuckDB at 100%%: Go %s, overcommitted %v", Format(p.Go), p.Overcommitted())
	}

	// Maintenance's share stays between 256 MiB (or half of Go's limit)
	// and 4 GiB, and is left to the default without a Go limit.
	for goLimit, want := range map[int64]int64{MinGo: MinGo / 2, 1 << 30: 256 << 20, 8 << 30: 2 << 30, 64 << 30: 4 << 30, 0: 0} {
		if got := (Plan{Go: goLimit}).Maintenance(); got != want {
			t.Errorf("Go %s: maintenance %s, want %s", Format(goLimit), Format(got), Format(want))
		}
	}
}

// TestDuckDBLimit checks the limit Igloo gives DuckDB where none is
// configured: what the process's memory leaves after High, which Adapt
// keeps free, the reserve, and an eighth of the limit, which DuckDB holds
// beyond it as it spills; at least half of the memory.
func TestDuckDBLimit(t *testing.T) {
	const gib = float64(1 << 30)
	// The benchmark's replica: 61.6 GiB, where DuckDB's own default gave it
	// 49.3, and a count(DISTINCT) of a billion IDs was canceled with 2.6
	// GiB left after taking 5 GiB more within a second, with 7.6 left
	// before. 4.1 GiB less for DuckDB leaves the watch 6.7 GiB.
	total := int64(66_169_339_904)
	l := DuckDBLimit(total)
	if got := float64(l) / gib; got < 45.1 || got > 45.3 {
		t.Errorf("61.6 GiB: DuckDB %.2f GiB", got)
	}
	others := total - total*8/10 - int64(76)<<30/10
	if after := total - l - others - 5<<30; after < Low(total) || float64(after)/gib < 6.5 {
		t.Errorf("the benchmark's statement leaves %s", Format(after))
	}
	// What is left covers High, the reserve and an eighth of the limit, and
	// Go's limit, past the reserve, all but High and that eighth.
	if rest := total - l; rest < High(total)+Reserve(total)+l/8 {
		t.Errorf("61.6 GiB: %s left beside DuckDB's %s", Format(rest), Format(l))
	}
	for _, c := range []struct{ total, want int64 }{
		{8 << 30, 6007 << 20}, // (8192 - 1024 - 409.6) MiB * 8/9
		{1 << 30, 512 << 20},  // half: High and the reserve take 768 MiB
		{0, 0},
	} {
		if got := DuckDBLimit(c.total); got>>20 != c.want>>20 {
			t.Errorf("%s: DuckDB %s, want %s", Format(c.total), Format(got), Format(c.want))
		}
	}
}

func TestFormat(t *testing.T) {
	for n, want := range map[int64]string{512: "512 B", 1536: "1.5 KiB", 3 << 20: "3.0 MiB", 30 << 30: "30.0 GiB"} {
		if got := Format(n); got != want {
			t.Errorf("Format(%d) = %q, want %q", n, got, want)
		}
	}
}
