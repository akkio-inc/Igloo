// The process's C++ terminate handler (see terminate.go).

#include <atomic>
#include <cxxabi.h>
#include <exception>
#include <execinfo.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <typeinfo>
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif

namespace {

// putTo writes the n bytes of s to fd with write(2) alone, which, like strlen
// and _exit, is safe in a signal handler and allocates nothing.
void putTo(int fd, const char *s, size_t n) {
	while (n > 0) {
		ssize_t w = write(fd, s, n);
		if (w <= 0) {
			return;
		}
		s += w;
		n -= static_cast<size_t>(w);
	}
}

void put(const char *s) {
	putTo(2, s, strlen(s));
}

// statementDepth is how deep the thread is in statements' work (see
// igloo_statement_enter): a thread Go runs a session's statement on, which
// nothing else uses while it does.
thread_local int statementDepth = 0;

// parkFd is where a parked thread's record goes (see igloo_park_fd), -1 for
// nowhere; parked counts the threads parked, of which there may be mostParked.
std::atomic<int> parkFd {-1};
std::atomic<int> parked {0};
constexpr int mostParked = 64;

long threadID() {
#if defined(__linux__)
	return syscall(SYS_gettid);
#else
	uint64_t id = 0;
	pthread_threadid_np(nullptr, &id);
	return static_cast<long>(id);
#endif
}

// park writes, to parkFd, the thread's id and what escaped, on one line of at
// most a kilobyte, with a buffer on the stack, and then waits for good.
[[noreturn]] void park(const char *what) {
	char line[1100];
	size_t n = 0;
	char digits[24];
	size_t d = 0;
	for (long id = threadID(); id > 0 && d < sizeof(digits); id /= 10) {
		digits[d++] = static_cast<char>('0' + id % 10);
	}
	while (d > 0) {
		line[n++] = digits[--d];
	}
	line[n++] = '\t';
	for (const char *c = what; c != nullptr && *c != 0 && n < 1024; c++) {
		line[n++] = *c == '\n' || *c == '\t' ? ' ' : *c;
	}
	line[n++] = '\n';
	putTo(parkFd.load(), line, n);
	for (;;) {
		pause();
	}
}

// onTerminate says that an exception escaped, of which type, as its mangled
// name (demangling allocates), and what it says, then the frames of the thread
// it escaped on, as offsets into the binary (backtrace_symbols_fd writes them
// out without allocating). std::terminate runs on that thread's stack, which
// nothing unwound past where the exception stopped, so the frames are those of
// the throw. On a thread a session's statement runs on, it parks the thread,
// which then holds nothing but what its statement held, for Go to end the
// statement and its session; on any other, or past mostParked, it ends the
// process at once.
[[noreturn]] void onTerminate() {
	put("igloo: a C++ exception escaped DuckDB or an extension");
	const char *what = nullptr;
	std::type_info *t = abi::__cxa_current_exception_type();
	if (t != nullptr) {
		put(" (");
		put(t->name());
		put(")");
		// Rethrown to be caught here: the exception thrown is in hand
		// already, so this copies and allocates nothing. It stays alive,
		// the one terminate was called for, while the thread does.
		try {
			std::rethrow_exception(std::current_exception());
		} catch (const std::exception &e) {
			what = e.what();
		} catch (...) {
		}
		if (what != nullptr) {
			put(": ");
			put(what);
		}
	}
	bool parks = statementDepth > 0 && parkFd.load() >= 0 && parked.fetch_add(1) < mostParked;
	put(parks ? "; parking its statement's thread, whose session ends\n" : "; exiting\n");
	void *frames[64];
	int n = backtrace(frames, 64);
	backtrace_symbols_fd(frames, n, 2);
	if (parks) {
		park(what);
	}
	_exit(134);
}

} // namespace

extern "C" void igloo_set_terminate(void) {
	// The first backtrace loads the unwinder, which allocates and takes
	// the loader's lock; done here, it is not done in onTerminate.
	void *frame[1];
	backtrace(frame, 1);
	std::set_terminate(onTerminate);
}

extern "C" void igloo_park_fd(int fd) {
	parkFd.store(fd);
}

extern "C" long igloo_statement_enter(void) {
	statementDepth++;
	return threadID();
}

extern "C" void igloo_statement_leave(void) {
	statementDepth--;
}
