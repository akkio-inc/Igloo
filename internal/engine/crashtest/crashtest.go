// Package crashtest crashes a thread that Go does not run, as a crash on one
// of DuckDB's worker threads would, or throws a C++ exception there that
// nothing catches. Only tests use it (cgo is not allowed in test files).
package crashtest

/*
#cgo linux LDFLAGS: -lpthread
#cgo CXXFLAGS: -std=c++17
#include <pthread.h>
#include <stddef.h>

static void *crash(void *arg) {
	*(volatile int *)arg = 1;
	return NULL;
}

static void crash_foreign_thread(void) {
	pthread_t t;
	if (pthread_create(&t, NULL, crash, NULL) == 0) {
		pthread_detach(t);
	}
}

void igloo_throw_foreign_thread(void);
void igloo_throw_in_destructor(void);
*/
import "C"

// CrashForeignThread starts a thread outside Go that writes through a nil
// pointer.
func CrashForeignThread() { C.crash_foreign_thread() }

// ThrowForeignThread starts a C++ thread outside Go that throws an
// exception nothing catches, as one escaping a DuckDB worker would.
func ThrowForeignThread() { C.igloo_throw_foreign_thread() }

// ThrowInDestructor throws a C++ exception out of a destructor on the calling
// thread, as the iceberg extension's scan state did at the memory limit, which
// calls std::terminate. It returns only if the handler does not end the thread.
func ThrowInDestructor() { C.igloo_throw_in_destructor() }
