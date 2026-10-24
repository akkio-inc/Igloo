//go:build linux

package prefetch

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpenWatch tells a tracked block opened, by anyone, from the kernel's
// file events, and gone once deleted; untracked files are no concern.
func TestOpenWatch(t *testing.T) {
	dir := t.TempDir()
	w, err := newOpenWatch(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	w.track("a")
	if err := writeBlock(filepath.Join(dir, "a"), "s3://b/f", "", []byte("block")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b"), []byte("other"), 0o644); err != nil {
		t.Fatal(err)
	}
	if read, ok := w.readSince("a"); read || !ok {
		t.Errorf("a block no one opened: read %v, there %v", read, ok)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := os.ReadFile(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the open of a", func() bool { read, _ := w.readSince("a"); return read })
	if _, ok := w.readSince("b"); ok {
		t.Error("an untracked file is tracked")
	}
	if err := os.Remove(filepath.Join(dir, "a")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the deletion of a", func() bool { _, ok := w.readSince("a"); return !ok })
	w.untrack("a")
	if len(w.blocks) != 0 {
		t.Errorf("blocks left tracked: %v", w.blocks)
	}
}
