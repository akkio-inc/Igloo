//go:build !linux

package prefetch

import "errors"

// openWatch is Linux's alone (see opens_linux.go).
type openWatch struct{}

func newOpenWatch(string) (*openWatch, error) {
	return nil, errors.New("no file events on this platform")
}

func (*openWatch) track(string)                     {}
func (*openWatch) untrack(string)                   {}
func (*openWatch) readSince(string) (read, ok bool) { return false, false }
func (*openWatch) close()                           {}
