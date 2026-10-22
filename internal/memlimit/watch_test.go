package memlimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestAvailable(t *testing.T) {
	const machine = "MemTotal:       31457280 kB\nMemAvailable:    8388608 kB\n" // 8 GiB available
	for _, c := range []struct {
		name  string
		files map[string]string
		want  int64
		ok    bool
	}{
		{"machine", map[string]string{
			"proc/meminfo":             machine,
			"proc/self/cgroup":         "0::/\n",
			"sys/fs/cgroup/memory.max": "max\n",
		}, 8 << 30, true},
		{"container (cgroup v2): its limit less its working set", map[string]string{
			"proc/meminfo":                 machine,
			"proc/self/cgroup":             "0::/\n",
			"sys/fs/cgroup/memory.max":     "4294967296\n",                      // 4 GiB
			"sys/fs/cgroup/memory.current": "3758096384\n",                      // 3.5 GiB, of which
			"sys/fs/cgroup/memory.stat":    "anon 1\ninactive_file 536870912\n", // 0.5 GiB reclaimable
		}, 1 << 30, true},
		{"container (cgroup v1)", map[string]string{
			"proc/meminfo":     machine,
			"proc/self/cgroup": "4:memory:/docker/abc\n",
			"sys/fs/cgroup/memory/memory.limit_in_bytes": "2147483648\n",
			"sys/fs/cgroup/memory/memory.usage_in_bytes": "1610612736\n",
			"sys/fs/cgroup/memory/memory.stat":           "total_inactive_file 0\n",
		}, 512 << 20, true},
		{"a container over its limit has none left", map[string]string{
			"proc/meminfo":                 machine,
			"proc/self/cgroup":             "0::/\n",
			"sys/fs/cgroup/memory.max":     "1073741824\n",
			"sys/fs/cgroup/memory.current": "1200000000\n",
		}, 0, true},
		{"nothing to read", map[string]string{}, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := available(files(t, c.files))
			if got != c.want || ok != c.ok {
				t.Errorf("available: %d %v, want %d %v", got, ok, c.want, c.ok)
			}
		})
	}
}

// TestWatch checks when Watch sheds: only below Low, and after shedding,
// not again while the canceled statement's memory comes back, unless
// memory keeps running out.
func TestWatch(t *testing.T) {
	const total = 32 << 30 // Low is 2 GiB
	var mu sync.Mutex
	left := int64(4 << 30)
	set := func(n int64) {
		mu.Lock()
		left = n
		mu.Unlock()
	}
	sheds := make(chan int64, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watch(ctx, total, time.Millisecond, func() (int64, bool) {
		mu.Lock()
		defer mu.Unlock()
		return left, true
	}, func(n int64) bool {
		sheds <- n
		return true
	})
	quiet := func(what string) {
		t.Helper()
		select {
		case n := <-sheds:
			t.Fatalf("%s: shed at %d bytes left", what, n)
		case <-time.After(50 * time.Millisecond):
		}
	}
	quiet("with plenty left")
	set(1800 << 20)
	select {
	case <-sheds:
	case <-time.After(time.Second):
		t.Fatal("did not shed below Low")
	}
	// Memory stays low while the canceled statement lets go of it.
	quiet("while the memory comes back")
	// Another statement keeps eating it: shed again.
	set(500 << 20)
	select {
	case <-sheds:
	case <-time.After(time.Second):
		t.Fatal("did not shed again as memory kept running out")
	}
}
