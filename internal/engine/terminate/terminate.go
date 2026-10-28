// Package terminate ends the process at once when a C++ exception escapes
// DuckDB or one of its extensions, rather than abort it.
//
// C++ calls std::terminate for an exception that nothing catches: one
// thrown on a thread DuckDB started that its code lets out, or one thrown
// while another unwinds, as when a cleanup that pins memory runs out of it
// too. The default handler prints the exception and calls abort(), which
// raises SIGABRT, and Go's signal handler takes that on a thread Go does
// not run. On two workers whose memory ran out ("failed to pin block of
// size 256.0 KiB"), the process neither died nor went on: one thread spun
// in Go's runtime and every other waited, for over half an hour, while
// Docker, seeing it run, restarted nothing and the statements whose tasks
// it held waited too.
//
// The handler here writes to standard error what escaped (its type and its
// message) and the frames of the thread it escaped on, and calls
// _exit(134), the status of a process aborted by SIGABRT, with nothing that
// could wait on a lock or allocate: no stack unwinding, no Go code, no
// signal. The process ends, and whatever runs it restarts it.
package terminate

/*
#cgo CXXFLAGS: -std=c++17
void igloo_set_terminate(void);
*/
import "C"

// Install sets the process's C++ terminate handler. It holds for every
// thread, DuckDB's own and its extensions', which share the process's C++
// runtime.
func Install() { C.igloo_set_terminate() }
