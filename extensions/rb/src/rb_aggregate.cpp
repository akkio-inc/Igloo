//===----------------------------------------------------------------------===//
// rb: aggregates into roaring bitmaps.
//
//   rb_agg(UINTEGER) -> BLOB                  the set of non-NULL ids; empty input gives an empty bitmap
//   rb_or_agg(BLOB) -> BLOB                   the union of non-NULL bitmaps; empty input gives an empty bitmap
//   rb_count_distinct(UINTEGER) -> BIGINT     COUNT(DISTINCT id)
//   rb_count_distinct(UINTEGER, k) -> BIGINT  COUNT(DISTINCT id, k) for k a DATE or an integer (either order;
//                                             (UINTEGER, UINTEGER) takes the first as the id): rows with a NULL
//                                             are skipped, as COUNT(DISTINCT a, b) does, and one bitmap of ids is
//                                             kept for each k, so k should have few values (dates, codes)
//
// Ids are buffered in each group's state and added in sorted batches, which merge linearly where adding ids one at
// a time in random order inserts into sorted arrays. A group's set is a sorted array while its ids are sparse and a
// roaring bitmap once they are dense, and a group that never fills its buffer is counted from the buffer: the many
// small groups of a GROUP BY that fits in memory cost little more than DuckDB's hash, and a few ids in each 2^16
// range of a large domain take no roaring containers.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"
#include "rb_memory.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>

namespace duckdb {
namespace rb {

//! The fewest and most ids buffered before a batch goes into a group's set (see IdState::Limit)
static constexpr uint32_t FLUSH_AT = 1024;
static constexpr uint32_t MAX_FLUSH_AT = 16384;

template <class T>
static void Push(T *&buf, uint32_t &n, uint32_t &cap, const T &value) {
	if (n == cap) {
		uint32_t next = cap ? MinValue<uint32_t>(cap * 2, FLUSH_AT) : 16;
		auto grown = static_cast<T *>(Reallocate(buf, next * sizeof(T)));
		if (!grown) {
			throw OutOfMemoryException("rb: could not grow a buffer of %u ids", next);
		}
		buf = grown;
		cap = next;
	}
	buf[n++] = value;
}

static void OrInto(roaring_bitmap_t *&target, Bitmap src) {
	if (!target) {
		target = src.release();
	} else {
		roaring_bitmap_or_inplace(target, src.get());
	}
}

static Bitmap FromSorted(const uint32_t *ids, size_t n) {
	Bitmap bm(roaring_bitmap_create());
	roaring_bitmap_add_many(bm.get(), n, ids);
	return bm;
}

//===--------------------------------------------------------------------===//
// One set of ids
//===--------------------------------------------------------------------===//

//! A set of distinct ids as a sorted array, for sets too sparse for roaring bitmaps: a roaring container costs some
//! 50 bytes of its own besides 2 bytes an id, which a few ids in each 2^16 range of a large domain do not repay.
struct FlatSet {
	uint32_t size;
	//! distinct high 16 bits among the ids: the roaring containers they would take
	uint32_t keys;
	uint32_t ids[1];

	static FlatSet *Make(uint32_t capacity) {
		auto set = static_cast<FlatSet *>(Allocate(offsetof(FlatSet, ids) + capacity * sizeof(uint32_t)));
		if (!set) {
			throw OutOfMemoryException("rb: could not allocate a set of %u ids", capacity);
		}
		set->size = set->keys = 0;
		return set;
	}
	//! The union of a (may be null) and the sorted, distinct ids b[0..m), as a new set; frees a.
	static FlatSet *Merge(FlatSet *a, const uint32_t *b, uint32_t m) {
		const uint32_t n = a ? a->size : 0;
		auto out = Make(n + m);
		uint32_t i = 0, j = 0, k = 0, keys = 0;
		uint32_t last_key = 0;
		auto put = [&](uint32_t id) {
			if (k == 0 || out->ids[k - 1] != id) {
				const uint32_t key = id >> 16;
				if (k == 0 || key != last_key) {
					keys++;
					last_key = key;
				}
				out->ids[k++] = id;
			}
		};
		while (i < n && j < m) {
			put(a->ids[i] <= b[j] ? a->ids[i++] : b[j++]);
		}
		while (i < n) {
			put(a->ids[i++]);
		}
		while (j < m) {
			put(b[j++]);
		}
		out->size = k;
		out->keys = keys;
		Free(a);
		return out;
	}
	//! Whether roaring containers would hold these ids in less room than the array: dozens to a container
	bool Dense() const {
		return size >= FLUSH_AT && size >= 32 * keys;
	}
};

//! Ids are first buffered, unsorted, in the state itself and then in a buffer; batches go sorted into a flat set
//! while the ids are sparse, and into a roaring bitmap once they are dense. A group that never fills its buffer
//! is counted from the buffer, with no set at all.
struct IdState {
	//! Ids a state holds in itself before it allocates a buffer: most partial states of a GROUP BY with many groups
	//! see only a few ids, since DuckDB's hash aggregate starts new partial states as it goes.
	static constexpr uint32_t INLINE = 4;

	roaring_bitmap_t *bm; // the set once dense (then flat is null)
	FlatSet *flat;        // the set while sparse
	uint32_t *heap;       // the buffer once it outgrows inline_ids, of cap ids
	uint32_t n;
	uint32_t cap;
	uint32_t inline_ids[INLINE];

	uint32_t *Buffer() {
		return heap ? heap : inline_ids;
	}
	uint64_t SetSize() const {
		return bm ? roaring_bitmap_get_cardinality(bm) : flat ? flat->size : 0;
	}
	//! The set as a roaring bitmap from now on (an empty one if none)
	roaring_bitmap_t *&AsBitmap() {
		if (!bm) {
			if (flat) {
				bm = FromSorted(flat->ids, flat->size).release();
				Free(flat);
				flat = nullptr;
			} else {
				bm = roaring_bitmap_create();
			}
		}
		return bm;
	}
	//! Adds the sorted, distinct ids[0..m) to the set.
	void AddSorted(const uint32_t *ids, uint32_t m) {
		if (m == 0) {
			return;
		}
		if (bm) {
			OrInto(bm, FromSorted(ids, m));
			return;
		}
		flat = FlatSet::Merge(flat, ids, m);
		if (flat->Dense()) {
			AsBitmap();
		}
	}
	void Flush() {
		if (n == 0) {
			return;
		}
		auto buf = Buffer();
		std::sort(buf, buf + n);
		const auto m = static_cast<uint32_t>(std::unique(buf, buf + n) - buf);
		n = 0;
		AddSorted(buf, m);
	}
	void Add(uint32_t id) {
		if (!heap) {
			if (n < INLINE) {
				inline_ids[n++] = id;
				return;
			}
			Grow();
		} else if (n == cap) {
			if (cap < Limit()) {
				Grow();
			} else {
				Flush();
			}
		}
		heap[n++] = id;
	}
	//! How many ids to buffer before adding them to the set: a quarter of what it holds, within bounds. Each batch
	//! rewrites what of the set it touches, so batches that grow with the set keep the work linear, while the
	//! buffer stays smaller than the set.
	uint32_t Limit() const {
		const uint64_t quarter = SetSize() / 4;
		return static_cast<uint32_t>(MinValue<uint64_t>(MaxValue<uint64_t>(quarter, FLUSH_AT), MAX_FLUSH_AT));
	}
	void Grow() {
		uint32_t next = heap ? cap * 2 : 32;
		auto grown = static_cast<uint32_t *>(Reallocate(heap, next * sizeof(uint32_t)));
		if (!grown) {
			throw OutOfMemoryException("rb: could not grow a buffer of %u ids", next);
		}
		if (!heap) {
			std::memcpy(grown, inline_ids, n * sizeof(uint32_t));
		}
		heap = grown;
		cap = next;
	}
	//! Adds other's ids and set here. destructive: other may be emptied, and its buffer and set taken.
	void Combine(IdState &other, bool destructive) {
		auto obuf = other.Buffer();
		if (destructive && n == 0 && other.heap) {
			std::swap(heap, other.heap);
			std::swap(cap, other.cap);
			n = other.n;
			other.n = 0;
		} else {
			for (uint32_t k = 0; k < other.n; k++) {
				Add(obuf[k]);
			}
			if (destructive) {
				other.n = 0;
			}
		}
		if (other.flat) {
			if (destructive && !bm && !flat) {
				flat = other.flat;
				other.flat = nullptr;
			} else {
				AddSorted(other.flat->ids, other.flat->size);
			}
		}
		if (other.bm) {
			if (destructive && !bm && !flat) {
				bm = other.bm;
				other.bm = nullptr;
			} else if (!bm && !flat) {
				bm = roaring_bitmap_copy(other.bm);
			} else {
				roaring_bitmap_or_inplace(AsBitmap(), other.bm);
			}
		}
	}
	void Destroy() {
		if (bm) {
			roaring_bitmap_free(bm);
		}
		Free(flat);
		Free(heap);
		std::memset(this, 0, sizeof(*this));
	}
	//! The ids so far as one bitmap (an empty one if none)
	roaring_bitmap_t *Finish() {
		Flush();
		return AsBitmap();
	}
	//! How many distinct ids
	int64_t Count() {
		if (!bm && !flat) {
			// never outgrew the buffer: count it without making a set (leaving each id in it once)
			auto buf = Buffer();
			std::sort(buf, buf + n);
			n = static_cast<uint32_t>(std::unique(buf, buf + n) - buf);
			return static_cast<int64_t>(n);
		}
		Flush();
		return static_cast<int64_t>(SetSize());
	}
};

//===--------------------------------------------------------------------===//
// One bitmap of ids per key k
//===--------------------------------------------------------------------===//
struct KeyedId {
	int64_t key;
	uint32_t id;
	bool operator<(const KeyedId &o) const {
		return key < o.key || (key == o.key && id < o.id);
	}
};

struct KeyedBitmaps {
	vector<std::pair<int64_t, roaring_bitmap_t *>> items; // sorted by key
	~KeyedBitmaps() {
		for (auto &e : items) {
			if (e.second) {
				roaring_bitmap_free(e.second);
			}
		}
	}
	roaring_bitmap_t *&Slot(int64_t key) {
		auto it = std::lower_bound(items.begin(), items.end(), key,
		                           [](const std::pair<int64_t, roaring_bitmap_t *> &e, int64_t k) { return e.first < k; });
		if (it == items.end() || it->first != key) {
			it = items.insert(it, std::make_pair(key, static_cast<roaring_bitmap_t *>(nullptr)));
		}
		return it->second;
	}
};

struct PairState {
	KeyedBitmaps *map;
	KeyedId *buf;
	uint32_t n;
	uint32_t cap;

	void Flush() {
		if (n == 0) {
			return;
		}
		if (!map) {
			map = new KeyedBitmaps();
		}
		std::sort(buf, buf + n);
		vector<uint32_t> ids;
		ids.reserve(n);
		uint32_t i = 0;
		while (i < n) {
			const int64_t key = buf[i].key;
			ids.clear();
			for (; i < n && buf[i].key == key; i++) {
				ids.push_back(buf[i].id);
			}
			OrInto(map->Slot(key), FromSorted(ids.data(), ids.size()));
		}
		n = 0;
	}
	void Add(int64_t key, uint32_t id) {
		Push(buf, n, cap, KeyedId {key, id});
		if (n == FLUSH_AT) {
			Flush();
		}
	}
	void Destroy() {
		delete map;
		Free(buf);
		std::memset(this, 0, sizeof(*this));
	}
	int64_t Count() {
		Flush();
		int64_t c = 0;
		if (map) {
			for (auto &e : map->items) {
				if (e.second) {
					c += static_cast<int64_t>(roaring_bitmap_get_cardinality(e.second));
				}
			}
		}
		return c;
	}
};

//===--------------------------------------------------------------------===//
// Memory: each aggregate charges its bitmaps to an account (see rb_memory.hpp)
//===--------------------------------------------------------------------===//
struct RbBindData : public FunctionData {
	explicit RbBindData(Account *account) : account(account) {
	}
	AccountRef account;
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RbBindData>(*this);
	}
	bool Equals(const FunctionData &) const override {
		return true; // equal aggregates may share one computation, and so one account
	}
};

static unique_ptr<FunctionData> RbBind(ClientContext &context, AggregateFunction &, vector<unique_ptr<Expression>> &) {
	return make_uniq<RbBindData>(NewAccount(context));
}

static Account *AccountOf(AggregateInputData &aggr) {
	return aggr.bind_data ? aggr.bind_data->Cast<RbBindData>().account.get() : nullptr;
}

//! Charges this thread's allocations to the aggregate's account, and reserves what they grew to on the way out.
class Charged {
public:
	explicit Charged(AggregateInputData &aggr) : account(AccountOf(aggr)), scope(account) {
	}
	void Sync() {
		if (account) {
			account->Sync();
		}
	}

private:
	Account *account;
	AccountScope scope;
};

//===--------------------------------------------------------------------===//
// Callbacks
//===--------------------------------------------------------------------===//
template <class STATE>
static idx_t StateSize(const AggregateFunction &) {
	return sizeof(STATE);
}

template <class STATE>
static void StateInit(const AggregateFunction &, data_ptr_t state) {
	std::memset(state, 0, sizeof(STATE));
}

template <class STATE>
static void StateDestroy(Vector &states, AggregateInputData &, idx_t count) {
	auto sdata = FlatVector::GetData<STATE *>(states);
	for (idx_t i = 0; i < count; i++) {
		sdata[i]->Destroy();
	}
}

static void IdUpdate(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states, idx_t count) {
	Charged charged(aggr);
	UnifiedVectorFormat idata, sdata;
	inputs[0].ToUnifiedFormat(count, idata);
	states.ToUnifiedFormat(count, sdata);
	auto ids = UnifiedVectorFormat::GetData<uint32_t>(idata);
	auto sts = UnifiedVectorFormat::GetData<IdState *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto ii = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(ii)) {
			sts[sdata.sel->get_index(i)]->Add(ids[ii]);
		}
	}
	charged.Sync();
}

static void IdSimpleUpdate(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state, idx_t count) {
	Vector states(Value::POINTER(CastPointerToValue(state)));
	IdUpdate(inputs, aggr, n, states, count);
}

static void OrUpdate(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states, idx_t count) {
	Charged charged(aggr);
	UnifiedVectorFormat idata, sdata;
	inputs[0].ToUnifiedFormat(count, idata);
	states.ToUnifiedFormat(count, sdata);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(idata);
	auto sts = UnifiedVectorFormat::GetData<IdState *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto ii = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(ii)) {
			OrInto(sts[sdata.sel->get_index(i)]->AsBitmap(), ReadBitmap(blobs[ii]));
		}
	}
	charged.Sync();
}

static void OrSimpleUpdate(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state, idx_t count) {
	Vector states(Value::POINTER(CastPointerToValue(state)));
	OrUpdate(inputs, aggr, n, states, count);
}

// DuckDB's hash aggregate abandons partial states and combines them later when more than two threads run, so most
// combines are of states that never filled their buffer: those pass their ids on rather than building a bitmap.
static void IdCombine(Vector &source, Vector &target, AggregateInputData &aggr, idx_t count) {
	Charged charged(aggr);
	auto src = FlatVector::GetData<IdState *>(source);
	auto tgt = FlatVector::GetData<IdState *>(target);
	const bool destructive = aggr.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE;
	for (idx_t i = 0; i < count; i++) {
		tgt[i]->Combine(*src[i], destructive);
	}
	charged.Sync();
}

template <class K>
static inline int64_t KeyValue(K k) {
	return static_cast<int64_t>(k);
}

template <>
inline int64_t KeyValue(date_t k) {
	return static_cast<int64_t>(k.days);
}

template <class HI, class LO, bool ID_FIRST>
static void PairUpdate(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states, idx_t count) {
	Charged charged(aggr);
	UnifiedVectorFormat adata, bdata, sdata;
	inputs[0].ToUnifiedFormat(count, adata);
	inputs[1].ToUnifiedFormat(count, bdata);
	states.ToUnifiedFormat(count, sdata);
	auto as = UnifiedVectorFormat::GetData<HI>(adata);
	auto bs = UnifiedVectorFormat::GetData<LO>(bdata);
	auto sts = UnifiedVectorFormat::GetData<PairState *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto ai = adata.sel->get_index(i);
		auto bi = bdata.sel->get_index(i);
		if (!adata.validity.RowIsValid(ai) || !bdata.validity.RowIsValid(bi)) {
			continue;
		}
		auto &st = *sts[sdata.sel->get_index(i)];
		if constexpr (ID_FIRST) {
			st.Add(KeyValue(bs[bi]), static_cast<uint32_t>(as[ai]));
		} else {
			st.Add(KeyValue(as[ai]), static_cast<uint32_t>(bs[bi]));
		}
	}
	charged.Sync();
}

template <class HI, class LO, bool ID_FIRST>
static void PairSimpleUpdate(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state, idx_t count) {
	Vector states(Value::POINTER(CastPointerToValue(state)));
	PairUpdate<HI, LO, ID_FIRST>(inputs, aggr, n, states, count);
}

static void PairCombine(Vector &source, Vector &target, AggregateInputData &aggr, idx_t count) {
	Charged charged(aggr);
	auto src = FlatVector::GetData<PairState *>(source);
	auto tgt = FlatVector::GetData<PairState *>(target);
	const bool destructive = aggr.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE;
	for (idx_t i = 0; i < count; i++) {
		auto &s = *src[i];
		auto &t = *tgt[i];
		for (uint32_t k = 0; k < s.n; k++) {
			t.Add(s.buf[k].key, s.buf[k].id);
		}
		if (destructive) {
			s.n = 0;
		}
		if (!s.map) {
			continue;
		}
		if (!t.map && destructive) {
			t.map = s.map;
			s.map = nullptr;
			continue;
		}
		if (!t.map) {
			t.map = new KeyedBitmaps();
		}
		for (auto &e : s.map->items) {
			if (!e.second) {
				continue;
			}
			auto &slot = t.map->Slot(e.first);
			if (destructive) {
				OrInto(slot, Bitmap(e.second));
				e.second = nullptr;
			} else if (!slot) {
				slot = roaring_bitmap_copy(e.second);
			} else {
				roaring_bitmap_or_inplace(slot, e.second);
			}
		}
	}
	charged.Sync();
}

// Finalize: DuckDB passes a constant state vector for an ungrouped aggregate, a flat one otherwise.
template <class STATE, class T, class FUN>
static void FinalizeEach(Vector &states, Vector &result, idx_t count, idx_t offset, FUN fun) {
	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<T>(result)[0] = fun(**ConstantVector::GetData<STATE *>(states), result);
		return;
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto sdata = FlatVector::GetData<STATE *>(states);
	auto rdata = FlatVector::GetData<T>(result);
	for (idx_t i = 0; i < count; i++) {
		rdata[i + offset] = fun(*sdata[i], result);
	}
}

static void IdFinalizeCount(Vector &states, AggregateInputData &aggr, Vector &result, idx_t count, idx_t offset) {
	Charged charged(aggr);
	FinalizeEach<IdState, int64_t>(states, result, count, offset, [](IdState &st, Vector &) { return st.Count(); });
}

static void IdFinalizeBlob(Vector &states, AggregateInputData &aggr, Vector &result, idx_t count, idx_t offset) {
	Charged charged(aggr);
	FinalizeEach<IdState, string_t>(states, result, count, offset,
	                                [](IdState &st, Vector &res) { return WriteBitmap(res, st.Finish()); });
}

static void PairFinalizeCount(Vector &states, AggregateInputData &aggr, Vector &result, idx_t count, idx_t offset) {
	Charged charged(aggr);
	FinalizeEach<PairState, int64_t>(states, result, count, offset,
	                                 [](PairState &st, Vector &) { return st.Count(); });
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//
static AggregateFunction Unordered(AggregateFunction f) {
	f.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	f.distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	return f;
}

static AggregateFunction IdAggregate(const LogicalType &input, const LogicalType &ret, aggregate_update_t update,
                                     aggregate_simple_update_t simple, aggregate_finalize_t finalize) {
	// SPECIAL_HANDLING: NULL inputs reach the update, which skips them; an empty group still finalizes
	return Unordered(AggregateFunction({input}, ret, StateSize<IdState>, StateInit<IdState>, update, IdCombine,
	                                   finalize, FunctionNullHandling::SPECIAL_HANDLING, simple, RbBind,
	                                   StateDestroy<IdState>));
}

template <class K>
static void AddPairOverloads(AggregateFunctionSet &set, const LogicalType &key) {
	auto id = LogicalType::UINTEGER;
	set.AddFunction(Unordered(AggregateFunction({id, key}, LogicalType::BIGINT, StateSize<PairState>,
	                                            StateInit<PairState>, PairUpdate<uint32_t, K, true>, PairCombine,
	                                            PairFinalizeCount, FunctionNullHandling::SPECIAL_HANDLING,
	                                            PairSimpleUpdate<uint32_t, K, true>, RbBind,
	                                            StateDestroy<PairState>)));
	if (key.id() == LogicalTypeId::UINTEGER) {
		return; // (UINTEGER, UINTEGER): the first is the id
	}
	set.AddFunction(Unordered(AggregateFunction({key, id}, LogicalType::BIGINT, StateSize<PairState>,
	                                            StateInit<PairState>, PairUpdate<K, uint32_t, false>, PairCombine,
	                                            PairFinalizeCount, FunctionNullHandling::SPECIAL_HANDLING,
	                                            PairSimpleUpdate<K, uint32_t, false>, RbBind,
	                                            StateDestroy<PairState>)));
}

void RegisterAggregates(ExtensionLoader &loader) {
	AggregateFunctionSet agg("rb_agg");
	agg.AddFunction(IdAggregate(LogicalType::UINTEGER, LogicalType::BLOB, IdUpdate, IdSimpleUpdate, IdFinalizeBlob));
	loader.RegisterFunction(agg);

	AggregateFunctionSet oragg("rb_or_agg");
	oragg.AddFunction(IdAggregate(LogicalType::BLOB, LogicalType::BLOB, OrUpdate, OrSimpleUpdate, IdFinalizeBlob));
	loader.RegisterFunction(oragg);

	AggregateFunctionSet count("rb_count_distinct");
	count.AddFunction(
	    IdAggregate(LogicalType::UINTEGER, LogicalType::BIGINT, IdUpdate, IdSimpleUpdate, IdFinalizeCount));
	AddPairOverloads<date_t>(count, LogicalType::DATE);
	AddPairOverloads<int8_t>(count, LogicalType::TINYINT);
	AddPairOverloads<int16_t>(count, LogicalType::SMALLINT);
	AddPairOverloads<int32_t>(count, LogicalType::INTEGER);
	AddPairOverloads<int64_t>(count, LogicalType::BIGINT);
	AddPairOverloads<uint8_t>(count, LogicalType::UTINYINT);
	AddPairOverloads<uint16_t>(count, LogicalType::USMALLINT);
	AddPairOverloads<uint32_t>(count, LogicalType::UINTEGER);
	loader.RegisterFunction(count);
}

} // namespace rb
} // namespace duckdb
