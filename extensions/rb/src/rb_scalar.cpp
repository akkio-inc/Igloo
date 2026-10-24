//===----------------------------------------------------------------------===//
// rb: scalar functions over bitmaps in BLOBs. Each returns NULL when an argument is NULL.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"
#include "rb_memory.hpp"

#include <memory>

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
namespace rb {

Bitmap ReadBitmap(const string_t &blob) {
	const char *data = blob.GetData();
	const size_t size = blob.GetSize();
	// the portable format's size, read from its header and container headers: 0 if they do not fit
	const size_t expected = roaring_bitmap_portable_deserialize_size(data, size);
	if (expected == 0 || expected != size) {
		throw InvalidInputException("rb: invalid bitmap BLOB (%llu bytes, not a roaring bitmap in the portable format)",
		                            static_cast<unsigned long long>(size));
	}
	Bitmap bm(roaring_bitmap_portable_deserialize_safe(data, size));
	if (!bm) {
		throw InvalidInputException("rb: invalid bitmap BLOB (%llu bytes, could not be read)",
		                            static_cast<unsigned long long>(size));
	}
	const char *reason = nullptr;
	if (!roaring_bitmap_internal_validate(bm.get(), &reason)) {
		throw InvalidInputException("rb: invalid bitmap BLOB (%s)", reason ? reason : "inconsistent containers");
	}
	return bm;
}

string_t WriteBitmap(Vector &result, roaring_bitmap_t *bm) {
	roaring_bitmap_run_optimize(bm);
	const size_t size = roaring_bitmap_portable_size_in_bytes(bm);
	auto s = StringVector::EmptyString(result, size);
	roaring_bitmap_portable_serialize(bm, s.GetDataWriteable());
	s.Finalize();
	return s;
}

//===--------------------------------------------------------------------===//
// Memory: each expression charges the bitmaps it reads from its BLOBs to an account of its own (see rb_memory.hpp),
// which reserves them with DuckDB's buffer manager as they grow and fails the statement past rb_memory_budget, as an
// aggregate's states do. A call's bitmaps live while it runs (one for each argument, kept while the next row's BLOB
// is the same); a constant's, read at bind (rb_contains), as long as the expression.
//===--------------------------------------------------------------------===//
struct ScalarData : public FunctionData {
	explicit ScalarData(Account *account) : account(account) {
	}
	AccountRef account;
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ScalarData>(*this);
	}
	bool Equals(const FunctionData &) const override {
		return true; // equal expressions may share one computation, and so one account
	}
};

static unique_ptr<FunctionData> BindCharged(ClientContext &context, ScalarFunction &, vector<unique_ptr<Expression>> &) {
	return make_uniq<ScalarData>(NewAccount(context));
}

static Account &AccountOf(ExpressionState &state) {
	return *state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ScalarData>().account;
}

//! Reads blob's bitmap with the allocations charged to account (the thread is in its scope), and reserves them.
static Bitmap ReadCharged(Account &account, const string_t &blob) {
	auto bm = ReadBitmap(blob);
	account.Sync();
	return bm;
}

static void Cardinality(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &account = AccountOf(state);
	AccountScope scope(&account);
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(), [&](string_t blob) {
		return static_cast<int64_t>(roaring_bitmap_get_cardinality(ReadCharged(account, blob).get()));
	});
}

enum class SetOp { AND, OR, ANDNOT };

template <SetOp OP>
static void SetOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &account = AccountOf(state);
	AccountScope scope(&account);
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t a, string_t b) {
		    auto x = ReadCharged(account, a);
		    auto y = ReadCharged(account, b);
		    Bitmap z(OP == SetOp::AND  ? roaring_bitmap_and(x.get(), y.get())
		             : OP == SetOp::OR ? roaring_bitmap_or(x.get(), y.get())
		                               : roaring_bitmap_andnot(x.get(), y.get()));
		    account.Sync();
		    return WriteBitmap(result, z.get());
	    });
}

//! The bitmap of the BLOB read last, kept while the next one is the same BLOB: an argument that is the same
//! bitmap for every row, a constant or a subquery's, is read once a chunk rather than once a row (a dense
//! bitmap of hundreds of millions of ids takes tens of milliseconds to read and check). A BLOB short enough to
//! be inlined in its string_t has no address of its own to tell it by, and is read every time.
class BitmapCache {
public:
	explicit BitmapCache(Account &account) : account(account) {
	}
	const roaring_bitmap_t *Get(const string_t &blob) {
		if (blob.IsInlined()) {
			inlined.reset();
			inlined = ReadCharged(account, blob);
			return inlined.get();
		}
		if (!bm || blob.GetData() != data || blob.GetSize() != size) {
			bm.reset();
			bm = ReadCharged(account, blob);
			data = blob.GetData();
			size = blob.GetSize();
		}
		return bm.get();
	}

private:
	Account &account;
	Bitmap bm;
	Bitmap inlined;
	const char *data = nullptr;
	size_t size = 0;
};

template <SetOp OP>
static void SetCardinality(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &account = AccountOf(state);
	AccountScope scope(&account);
	BitmapCache xs(account), ys(account);
	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t a, string_t b) {
		    auto x = xs.Get(a);
		    auto y = ys.Get(b);
		    return static_cast<int64_t>(OP == SetOp::AND  ? roaring_bitmap_and_cardinality(x, y)
		                                : OP == SetOp::OR ? roaring_bitmap_or_cardinality(x, y)
		                                                  : roaring_bitmap_andnot_cardinality(x, y));
	    });
}

static void Contains(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &account = AccountOf(state);
	AccountScope scope(&account);
	BitmapCache bms(account);
	BinaryExecutor::Execute<string_t, uint32_t, bool>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t blob, uint32_t id) { return roaring_bitmap_contains(bms.Get(blob), id); });
}

//! rb_contains with a constant bitmap: one known when the statement is bound, such as a literal or a variable
//! (getvariable, which DuckDB binds to the variable's value). It is read once, at bind, and the argument goes:
//! otherwise every chunk gets its own copy of the BLOB, which DuckDB makes before the function sees it, and a
//! cohort's bitmap of tens of megabytes copied for each of millions of chunks costs far more than the scan.
//! A subquery's bitmap is not known at bind; a cross product hands each chunk a copy of it too, which is why
//! Igloo hands its own statements' bitmaps over in variables (see reportindex.go).
struct FixedBitmap : public ScalarData {
	explicit FixedBitmap(Account *account) : ScalarData(account) {
	}
	std::shared_ptr<roaring_bitmap_t> bm;
	std::shared_ptr<string> blob;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<FixedBitmap>(*this);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<FixedBitmap>();
		return blob == other.blob || *blob == *other.blob;
	}
};

static void ContainsFixed(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &fixed = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<FixedBitmap>();
	const roaring_bitmap_t *bm = fixed.bm.get();
	UnaryExecutor::Execute<uint32_t, bool>(args.data[0], result, args.size(),
	                                       [&](uint32_t id) { return roaring_bitmap_contains(bm, id); });
}

static unique_ptr<FunctionData> BindContains(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		return BindCharged(context, bound_function, arguments);
	}
	Value value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (value.IsNull()) {
		return BindCharged(context, bound_function, arguments); // NULL for every row, as without a constant
	}
	auto fixed = make_uniq<FixedBitmap>(NewAccount(context));
	fixed->blob = std::make_shared<string>(StringValue::Get(value));
	{
		AccountScope scope(fixed->account.get());
		fixed->bm = ReadCharged(*fixed->account, string_t(fixed->blob->data(), static_cast<uint32_t>(fixed->blob->size())));
	}
	Function::EraseArgument(bound_function, arguments, 0);
	bound_function.SetFunctionCallback(ContainsFixed);
	return std::move(fixed);
}

static void ToList(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &account = AccountOf(state);
	AccountScope scope(&account);
	const idx_t count = args.size();
	UnifiedVectorFormat input;
	args.data[0].ToUnifiedFormat(count, input);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(input);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto entries = FlatVector::GetData<list_entry_t>(result);
	auto &validity = FlatVector::Validity(result);
	idx_t offset = ListVector::GetListSize(result);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input.sel->get_index(i);
		if (!input.validity.RowIsValid(idx)) {
			validity.SetInvalid(i);
			entries[i] = list_entry_t(offset, 0);
			continue;
		}
		auto bm = ReadCharged(account, blobs[idx]);
		const idx_t n = roaring_bitmap_get_cardinality(bm.get());
		ListVector::Reserve(result, offset + n);
		auto &child = ListVector::GetEntry(result);
		roaring_bitmap_to_uint32_array(bm.get(), FlatVector::GetData<uint32_t>(child) + offset);
		entries[i] = list_entry_t(offset, n);
		offset += n;
		ListVector::SetListSize(result, offset);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void RegisterScalars(ExtensionLoader &loader) {
	const auto blob = LogicalType::BLOB;
	const auto bigint = LogicalType::BIGINT;
	loader.RegisterFunction(ScalarFunction("rb_cardinality", {blob}, bigint, Cardinality, BindCharged));
	loader.RegisterFunction(ScalarFunction("rb_and", {blob, blob}, blob, SetOperation<SetOp::AND>, BindCharged));
	loader.RegisterFunction(ScalarFunction("rb_or", {blob, blob}, blob, SetOperation<SetOp::OR>, BindCharged));
	loader.RegisterFunction(ScalarFunction("rb_andnot", {blob, blob}, blob, SetOperation<SetOp::ANDNOT>, BindCharged));
	loader.RegisterFunction(
	    ScalarFunction("rb_and_cardinality", {blob, blob}, bigint, SetCardinality<SetOp::AND>, BindCharged));
	loader.RegisterFunction(
	    ScalarFunction("rb_or_cardinality", {blob, blob}, bigint, SetCardinality<SetOp::OR>, BindCharged));
	loader.RegisterFunction(
	    ScalarFunction("rb_andnot_cardinality", {blob, blob}, bigint, SetCardinality<SetOp::ANDNOT>, BindCharged));
	loader.RegisterFunction(
	    ScalarFunction("rb_contains", {blob, LogicalType::UINTEGER}, LogicalType::BOOLEAN, Contains, BindContains));
	loader.RegisterFunction(
	    ScalarFunction("rb_to_list", {blob}, LogicalType::LIST(LogicalType::UINTEGER), ToList, BindCharged));
}

} // namespace rb
} // namespace duckdb
