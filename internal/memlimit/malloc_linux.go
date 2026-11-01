//go:build linux && cgo

package memlimit

/*
#include <malloc.h>
#include <stdlib.h>

// igloo_mapped_after_free reports whether an allocation of size bytes is
// mapped of its own, after one of large bytes was freed, which raises glibc's
// threshold to that size unless it is fixed.
static int igloo_mapped_after_free(size_t large, size_t size) {
	// volatile, or the compiler takes out what it sees is never used.
	void *volatile first = malloc(large);
	free(first);
	struct mallinfo2 before = mallinfo2();
	void *volatile p = malloc(size);
	struct mallinfo2 after = mallinfo2();
	free(p);
	return after.hblks > before.hblks;
}
*/
import "C"

import (
	"os"
	"strings"
)

// mmapThreshold is the size from which glibc maps an allocation of its own,
// which goes back to the system when freed, rather than taking it from an
// arena, which keeps it. By default the threshold rises to the largest
// mapped allocation freed, up to 32 MiB, and DuckDB's buffers of 256 KiB, once
// one larger is freed, stay in arenas that fragment: after a sort that
// returned its rows, 3.4 GiB stayed with the process, against 0.5 GiB with
// the threshold fixed here, with the same CPU seconds over TPC-H, alone and
// with sessions at once.
const mmapThreshold = 128 << 10

// fixMmapThreshold fixes glibc's threshold, unless the operator's
// GLIBC_TUNABLES set it.
func fixMmapThreshold() {
	if !strings.Contains(os.Getenv("GLIBC_TUNABLES"), "mmap_threshold") {
		C.mallopt(C.M_MMAP_THRESHOLD, mmapThreshold)
	}
}

// mappedAfterFree reports whether an allocation of size bytes is mapped of its
// own, after one of large bytes was freed.
func mappedAfterFree(large, size int) bool {
	return C.igloo_mapped_after_free(C.size_t(large), C.size_t(size)) != 0
}
