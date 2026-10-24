//===----------------------------------------------------------------------===//
// rb: accounting for the memory bitmaps take.
//
// CRoaring allocates with malloc, outside DuckDB's buffer manager, so without
// this the memory a query's bitmaps take would count toward no memory_limit
// and could take a process past it. CRoaring's memory hook sends every
// allocation through a counter, charged to the aggregate running on the thread
// (see AccountScope). After each step that grows its bitmaps, an aggregate
// reserves the growth with DuckDB's buffer manager (MemoryTag::EXTENSION),
// which evicts other buffers to make room or refuses; it gives the
// reservation back when its last bitmap is freed. A refusal, or growth past
// rb_memory_budget, fails the statement with an error that starts
// "rb: bitmap memory budget exceeded", which Igloo answers by running the
// statement again with rb_count_distinct = 'off', on the hash path.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {
class BufferManager;

namespace rb {

//! The prefix of the error a statement fails with when its bitmaps do not fit.
extern const char *const BUDGET_EXCEEDED;

//! The memory one aggregate's bitmaps take, across threads.
class Account {
public:
	Account(BufferManager &buffer_manager, idx_t budget);

	//! Reserves with DuckDB what the bitmaps have grown to, or throws.
	void Sync();
	//! Bytes the bitmaps hold now
	int64_t Bytes() const {
		return bytes.load(std::memory_order_relaxed);
	}

	void Retain() {
		refs.fetch_add(1, std::memory_order_relaxed);
	}
	//! Drops a reference: the aggregate's (see AccountRef) or one of its live allocations. The last gives DuckDB
	//! the reservation back and frees the account.
	void Release();

	void Add(int64_t delta) {
		bytes.fetch_add(delta, std::memory_order_relaxed);
	}
	//! Adds or drops references in bulk (see AccountScope); dropping the last frees the account.
	void AddRefs(int64_t delta);

private:
	~Account();

	BufferManager &buffer_manager;
	const idx_t budget;
	std::atomic<int64_t> bytes {0};
	std::atomic<int64_t> refs {1};
	std::atomic<int64_t> reserved_hint {0};
	std::mutex lock;
	idx_t reserved = 0;
};

//! The aggregate's own reference to its account, held in its bind data.
class AccountRef {
public:
	explicit AccountRef(Account *account) : account(account) {
	}
	AccountRef(const AccountRef &other) : account(other.account) {
		account->Retain();
	}
	AccountRef &operator=(const AccountRef &) = delete;
	~AccountRef() {
		account->Release();
	}
	Account &operator*() const {
		return *account;
	}
	Account *get() const {
		return account;
	}

private:
	Account *account;
};

//! Charges this thread's allocations to account while it lives (nested scopes restore the outer one). The
//! charges are added to the account in bulk, when the scope ends or the account syncs.
class AccountScope {
public:
	explicit AccountScope(Account *account);
	~AccountScope();

private:
	Account *previous;
};

//! Installs the CRoaring memory hook; once per process.
void InstallMemoryHook();

//! malloc/realloc/free that count toward the current account, for the extension's own buffers.
void *Allocate(size_t size);
void *Reallocate(void *ptr, size_t size);
void Free(void *ptr);

} // namespace rb
} // namespace duckdb
