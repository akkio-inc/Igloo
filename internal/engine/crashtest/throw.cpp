// An exception that escapes a thread outside Go (see crashtest.go).

#include <stdexcept>
#include <thread>

extern "C" void igloo_throw_foreign_thread(void) {
	std::thread([] { throw std::runtime_error("thrown on a thread outside Go, and not caught"); }).detach();
}
