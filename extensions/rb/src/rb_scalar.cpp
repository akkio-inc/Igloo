//===----------------------------------------------------------------------===//
// rb: scalar functions over bitmaps in BLOBs. Each returns NULL when an argument is NULL.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

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

static void Cardinality(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(), [](string_t blob) {
		return static_cast<int64_t>(roaring_bitmap_get_cardinality(ReadBitmap(blob).get()));
	});
}

enum class SetOp { AND, OR, ANDNOT };

template <SetOp OP>
static void SetOperation(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t a, string_t b) {
		    auto x = ReadBitmap(a);
		    auto y = ReadBitmap(b);
		    Bitmap z(OP == SetOp::AND  ? roaring_bitmap_and(x.get(), y.get())
		             : OP == SetOp::OR ? roaring_bitmap_or(x.get(), y.get())
		                               : roaring_bitmap_andnot(x.get(), y.get()));
		    return WriteBitmap(result, z.get());
	    });
}

//! The bitmap of the BLOB read last, kept while the next one is the same BLOB: an argument that is the same
//! bitmap for every row, a constant or a subquery's, is read once a chunk rather than once a row (a dense
//! bitmap of hundreds of millions of ids takes tens of milliseconds to read and check). A BLOB short enough to
//! be inlined in its string_t has no address of its own to tell it by, and is read every time.
class BitmapCache {
public:
	const roaring_bitmap_t *Get(const string_t &blob) {
		if (blob.IsInlined()) {
			inlined = ReadBitmap(blob);
			return inlined.get();
		}
		if (!bm || blob.GetData() != data || blob.GetSize() != size) {
			bm = ReadBitmap(blob);
			data = blob.GetData();
			size = blob.GetSize();
		}
		return bm.get();
	}

private:
	Bitmap bm;
	Bitmap inlined;
	const char *data = nullptr;
	size_t size = 0;
};

template <SetOp OP>
static void SetCardinality(DataChunk &args, ExpressionState &, Vector &result) {
	BitmapCache xs, ys;
	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t a, string_t b) {
		    auto x = xs.Get(a);
		    auto y = ys.Get(b);
		    return static_cast<int64_t>(OP == SetOp::AND  ? roaring_bitmap_and_cardinality(x, y)
		                                : OP == SetOp::OR ? roaring_bitmap_or_cardinality(x, y)
		                                                  : roaring_bitmap_andnot_cardinality(x, y));
	    });
}

static void Contains(DataChunk &args, ExpressionState &, Vector &result) {
	BitmapCache bms;
	BinaryExecutor::Execute<string_t, uint32_t, bool>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t blob, uint32_t id) { return roaring_bitmap_contains(bms.Get(blob), id); });
}

static void ToList(DataChunk &args, ExpressionState &, Vector &result) {
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
		auto bm = ReadBitmap(blobs[idx]);
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
	loader.RegisterFunction(ScalarFunction("rb_cardinality", {blob}, bigint, Cardinality));
	loader.RegisterFunction(ScalarFunction("rb_and", {blob, blob}, blob, SetOperation<SetOp::AND>));
	loader.RegisterFunction(ScalarFunction("rb_or", {blob, blob}, blob, SetOperation<SetOp::OR>));
	loader.RegisterFunction(ScalarFunction("rb_andnot", {blob, blob}, blob, SetOperation<SetOp::ANDNOT>));
	loader.RegisterFunction(ScalarFunction("rb_and_cardinality", {blob, blob}, bigint, SetCardinality<SetOp::AND>));
	loader.RegisterFunction(ScalarFunction("rb_or_cardinality", {blob, blob}, bigint, SetCardinality<SetOp::OR>));
	loader.RegisterFunction(
	    ScalarFunction("rb_andnot_cardinality", {blob, blob}, bigint, SetCardinality<SetOp::ANDNOT>));
	loader.RegisterFunction(
	    ScalarFunction("rb_contains", {blob, LogicalType::UINTEGER}, LogicalType::BOOLEAN, Contains));
	loader.RegisterFunction(
	    ScalarFunction("rb_to_list", {blob}, LogicalType::LIST(LogicalType::UINTEGER), ToList));
}

} // namespace rb
} // namespace duckdb
