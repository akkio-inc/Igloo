// The process's C++ terminate handler (see terminate.go).

#include <cxxabi.h>
#include <exception>
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

// exitOnTerminate says that an exception escaped, and of which type, as
// its mangled name (demangling allocates), and ends the process at once.
[[noreturn]] void exitOnTerminate() {
	put("igloo: a C++ exception escaped DuckDB or an extension");
	std::type_info *t = abi::__cxa_current_exception_type();
	if (t != nullptr) {
		put(" (");
		put(t->name());
		put(")");
	}
	put("; exiting\n");
	_exit(134);
}

} // namespace

extern "C" void igloo_set_terminate(void) {
	std::set_terminate(exitOnTerminate);
}
