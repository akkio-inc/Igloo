// An exception that escapes a thread outside Go, and one thrown out of a
// destructor on the calling thread (see crashtest.go).

#include <stdexcept>
#include <thread>

extern "C" void igloo_throw_foreign_thread(void) {
	std::thread([] { throw std::runtime_error("thrown on a thread outside Go, and not caught"); }).detach();
}

namespace {

// Drainer throws as it goes, as the iceberg extension's scan state does when the
// manifest tasks it waits for ran out of memory: a destructor is noexcept, so
// the exception calls std::terminate.
struct Drainer {
	~Drainer() {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wterminate"
		throw std::runtime_error("Out of Memory Error: failed to pin block of size 4.0 KiB, thrown out of a destructor");
#pragma GCC diagnostic pop
	}
};

void drain() {
	Drainer d;
}

} // namespace

extern "C" void igloo_throw_in_destructor(void) {
	drain();
}
