//===----------------------------------------------------------------------===//
// rb: accounting for the memory bitmaps take (see rb_memory.hpp).
//===----------------------------------------------------------------------===//
#include "rb_memory.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "roaring.h"

#include <cstdlib>
#include <cstring>

namespace duckdb {
namespace rb {

const char *const BUDGET_EXCEEDED = "rb: bitmap memory budget exceeded";

// DuckDB is asked for room in steps of at least this much, so the lock and the buffer manager are seldom touched.
static constexpr idx_t RESERVE_STEP = 4ULL << 20;

static thread_local Account *current = nullptr;

// What this thread's allocations and frees of the current account's blocks have changed, not yet added to the
// account: shared counters touched on every allocation would bounce between cores, and the many small partial
// states of a GROUP BY allocate often. Flushed when the thread leaves the account's scope, and before Sync.
struct Pending {
	Account *account = nullptr;
	int64_t bytes = 0;
	int64_t refs = 0;
};
static thread_local Pending pending;

static void FlushPending() {
	if (pending.account && (pending.bytes != 0 || pending.refs != 0)) {
		pending.account->Add(pending.bytes);
		pending.account->AddRefs(pending.refs);
	}
	pending.bytes = 0;
	pending.refs = 0;
}

Account::Account(BufferManager &buffer_manager_p, idx_t budget_p) : buffer_manager(buffer_manager_p), budget(budget_p) {
}

Account::~Account() {
	if (reserved > 0) {
		buffer_manager.FreeReservedMemory(reserved);
	}
}

void Account::Release() {
	AddRefs(-1);
}

void Account::AddRefs(int64_t delta) {
	// a scope's pending references are added while the aggregate holds its own (AccountRef), so only the last
	// release of all can reach zero
	if (refs.fetch_add(delta, std::memory_order_acq_rel) + delta == 0) {
		delete this;
	}
}

void Account::Sync() {
	if (pending.account == this) {
		FlushPending();
	}
	const int64_t now = bytes.load(std::memory_order_relaxed);
	if (now <= reserved_hint.load(std::memory_order_relaxed)) {
		return;
	}
	std::lock_guard<std::mutex> guard(lock);
	if (budget > 0 && now > static_cast<int64_t>(budget)) {
		throw OutOfMemoryException("%s: the bitmaps of one COUNT(DISTINCT) hold %s, past rb_memory_budget (%s)",
		                           BUDGET_EXCEEDED, StringUtil::BytesToHumanReadableString(now),
		                           StringUtil::BytesToHumanReadableString(budget));
	}
	if (now <= static_cast<int64_t>(reserved)) {
		return;
	}
	const idx_t need = static_cast<idx_t>(now) - reserved;
	const idx_t step = MaxValue<idx_t>(need, MaxValue<idx_t>(RESERVE_STEP, reserved / 8));
	try {
		buffer_manager.ReserveMemory(step);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw OutOfMemoryException("%s: the bitmaps of one COUNT(DISTINCT) hold %s and DuckDB has no room for more: %s",
		                           BUDGET_EXCEEDED, StringUtil::BytesToHumanReadableString(now), error.RawMessage());
	}
	reserved += step;
	reserved_hint.store(static_cast<int64_t>(reserved), std::memory_order_relaxed);
}

AccountScope::AccountScope(Account *account) : previous(current) {
	FlushPending();
	current = account;
	pending.account = account;
}

AccountScope::~AccountScope() {
	FlushPending();
	current = previous;
	pending.account = previous;
}

//===--------------------------------------------------------------------===//
// Counting allocator. Every block carries a header naming its size and account, so a block is uncharged from the
// account that paid for it, whichever thread frees it. Aligned blocks also record where the underlying block starts.
//===--------------------------------------------------------------------===//
struct alignas(16) Header {
	size_t size;
	Account *account;
};
static_assert(sizeof(Header) == 16, "the header keeps blocks 16-byte aligned");

static void Charge(Header &h, size_t size) {
	h.size = size;
	h.account = current;
	if (h.account) {
		pending.bytes += static_cast<int64_t>(size);
		pending.refs++;
	}
}

static void Uncharge(const Header &h) {
	if (!h.account) {
		return;
	}
	if (h.account == pending.account) {
		pending.bytes -= static_cast<int64_t>(h.size);
		pending.refs--;
	} else {
		h.account->Add(-static_cast<int64_t>(h.size));
		h.account->Release();
	}
}

void *Allocate(size_t size) {
	auto h = static_cast<Header *>(std::malloc(sizeof(Header) + size));
	if (!h) {
		return nullptr;
	}
	Charge(*h, size);
	return h + 1;
}

static void *AllocateZeroed(size_t count, size_t size) {
	if (size != 0 && count > (SIZE_MAX - sizeof(Header)) / size) {
		return nullptr;
	}
	void *p = Allocate(count * size);
	if (p) {
		std::memset(p, 0, count * size);
	}
	return p;
}

void Free(void *ptr) {
	if (!ptr) {
		return;
	}
	auto h = static_cast<Header *>(ptr) - 1;
	Uncharge(*h);
	std::free(h);
}

void *Reallocate(void *ptr, size_t size) {
	if (!ptr) {
		return Allocate(size);
	}
	auto h = static_cast<Header *>(ptr) - 1;
	Header old = *h;
	auto grown = static_cast<Header *>(std::realloc(h, sizeof(Header) + size));
	if (!grown) {
		return nullptr;
	}
	// the block keeps its account; only its size changes
	if (old.account == pending.account && old.account) {
		pending.bytes += static_cast<int64_t>(size) - static_cast<int64_t>(old.size);
	} else if (old.account) {
		old.account->Add(static_cast<int64_t>(size) - static_cast<int64_t>(old.size));
	}
	grown->size = size;
	return grown + 1;
}

struct AlignedHeader {
	void *block;
	Header header;
};

static void *AllocateAligned(size_t alignment, size_t size) {
	if (alignment < alignof(AlignedHeader)) {
		alignment = alignof(AlignedHeader);
	}
	const size_t extra = alignment + sizeof(AlignedHeader);
	void *block = std::malloc(extra + size);
	if (!block) {
		return nullptr;
	}
	auto start = reinterpret_cast<uintptr_t>(block) + sizeof(AlignedHeader);
	auto aligned = (start + alignment - 1) & ~(static_cast<uintptr_t>(alignment) - 1);
	auto h = reinterpret_cast<AlignedHeader *>(aligned) - 1;
	h->block = block;
	Charge(h->header, size);
	return reinterpret_cast<void *>(aligned);
}

static void FreeAligned(void *ptr) {
	if (!ptr) {
		return;
	}
	auto h = static_cast<AlignedHeader *>(ptr) - 1;
	Uncharge(h->header);
	std::free(h->block);
}

void InstallMemoryHook() {
	static std::once_flag once;
	std::call_once(once, [] {
		roaring_memory_t hook;
		hook.malloc = Allocate;
		hook.realloc = Reallocate;
		hook.calloc = AllocateZeroed;
		hook.free = Free;
		hook.aligned_malloc = AllocateAligned;
		hook.aligned_free = FreeAligned;
		roaring_init_memory_hook(hook);
	});
}

} // namespace rb
} // namespace duckdb
