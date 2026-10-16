package activity

import (
	"testing"
	"time"
)

func TestTracker(t *testing.T) {
	tr := New()
	if running, idle := tr.Snapshot(); running != 0 || idle < 0 || idle > time.Second {
		t.Fatalf("new tracker: running %d, idle %v", running, idle)
	}

	end1 := tr.Begin()
	end2 := tr.Begin()
	if running, _ := tr.Snapshot(); running != 2 {
		t.Fatalf("running = %d, want 2", running)
	}
	end1()
	end1() // a second call must not count twice
	if running, _ := tr.Snapshot(); running != 1 {
		t.Fatalf("running = %d, want 1", running)
	}
	end2()
	time.Sleep(20 * time.Millisecond)
	running, idle := tr.Snapshot()
	if running != 0 || idle < 20*time.Millisecond {
		t.Fatalf("after end: running %d, idle %v", running, idle)
	}
}

func TestNilTracker(t *testing.T) {
	var tr *Tracker
	tr.Begin()()
	if running, idle := tr.Snapshot(); running != 0 || idle != 0 {
		t.Fatalf("nil tracker: running %d, idle %v", running, idle)
	}
}
