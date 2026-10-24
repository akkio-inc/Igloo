package prefetch

import (
	"os"
	"syscall"
	"time"
)

// accessTime is a file's access time.
func accessTime(fi os.FileInfo) (time.Time, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec), true
}
