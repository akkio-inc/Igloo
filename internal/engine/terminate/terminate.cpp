// The process's C++ terminate handler (see terminate.go).

#include <cxxabi.h>
#include <exception>
#include <execinfo.h>
#include <string.h>
#include <typeinfo>
#include <unistd.h>

namespace {

// put writes s to standard error with write(2) alone, which, like strlen
// and _exit, is safe in a signal handler and allocates nothing.
void put(const char *s) {
	size_t n = strlen(s);
	while (n > 0) {
		ssize_t w = write(2, s, n);
		if (w <= 0) {
			return;
		}
		s += w;
		n -= static_cast<size_t>(w);
	}
}

// exitOnTerminate says that an exception escaped, of which type, as its
// mangled name (demangling allocates), and what it says, then the frames of
// the thread it escaped on, as offsets into the binary (backtrace_symbols_fd
// writes them out without allocating), and ends the process at once.
// std::terminate runs on that thread's stack, which nothing unwound, so the
// frames are those of the throw.
[[noreturn]] void exitOnTerminate() {
	put("igloo: a C++ exception escaped DuckDB or an extension");
	std::type_info *t = abi::__cxa_current_exception_type();
	if (t != nullptr) {
		put(" (");
		put(t->name());
		put(")");
		// Rethrown to be caught here: the exception thrown is in hand
		// already, so this copies and allocates nothing.
		try {
			std::rethrow_exception(std::current_exception());
		} catch (const std::exception &e) {
			put(": ");
			put(e.what());
		} catch (...) {
		}
	}
	put("; exiting\n");
	void *frames[64];
	int n = backtrace(frames, 64);
	backtrace_symbols_fd(frames, n, 2);
	_exit(134);
}

} // namespace

extern "C" void igloo_set_terminate(void) {
	// The first backtrace loads the unwinder, which allocates and takes
	// the loader's lock; done here, it is not done in exitOnTerminate.
	void *frame[1];
	backtrace(frame, 1);
	std::set_terminate(exitOnTerminate);
}
