//===----------------------------------------------------------------------===//
// rb: json_extract and json_extract_string with DuckDB's results, parsing a
// document that rows repeat once.
//
// DuckDB's json_extract(doc, path) parses doc again for every row. Only a
// constant path into a constant doc is computed once, for the whole vector.
// A path that changes from row to row into one document, as a lookup of each
// row's key in one map joined to every row is, parses the whole map for each
// row: a target encoder's UPDATE of 82,658 rows, 20 of their columns each
// looked up in one map of some 3,000 keys, parsed an 80 KB map 1.65 million
// times and took a minute.
//
// At load, the overloads of one path, a string or an integer, become these.
// They are DuckDB's own code (JSONExecutors::BinaryExecute and
// json_extract.cpp's value functions, built from the headers of the DuckDB
// that loads the extension), except that within a vector a row whose
// document is the string the row before read, or the same bytes, reads the
// document that row parsed. So they give DuckDB's values, NULLs and errors,
// computed from the same parse.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "json_executors.hpp"

#include <cstring>

namespace duckdb {
namespace rb {

namespace {

//! The document the last row read, for the rows after it with the same one. A string of up to 12 bytes is held
//! inside the string_t, a copy, so it is parsed every time: that costs next to nothing.
class LastDocument {
public:
	explicit LastDocument(yyjson_alc *alc_p) : alc(alc_p) {
	}

	yyjson_doc *Read(const string_t &input) {
		if (input.IsInlined()) {
			return JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, alc);
		}
		auto data = input.GetData();
		auto size = input.GetSize();
		if (doc && size == last_size && (data == last_data || memcmp(data, last_data, size) == 0)) {
			return doc;
		}
		doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, alc);
		last_data = data;
		last_size = size;
		return doc;
	}

private:
	yyjson_alc *alc;
	yyjson_doc *doc = nullptr;
	const char *last_data = nullptr;
	idx_t last_size = 0;
};

using value_function_t = string_t (*)(yyjson_val *, yyjson_alc *, Vector &, ValidityMask &, idx_t);

// json_extract.cpp's ExtractFromVal and ExtractStringFromVal.
string_t ExtractFromVal(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &, idx_t) {
	return JSONCommon::WriteVal<yyjson_val>(val, alc);
}

string_t ExtractStringFromVal(yyjson_val *val, yyjson_alc *alc, Vector &, ValidityMask &mask, idx_t idx) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		mask.SetInvalid(idx);
		return string_t {};
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		return string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	default:
		return JSONCommon::WriteVal<yyjson_val>(val, alc);
	}
}

//! JSONExecutors::BinaryExecute<string_t> with fun, reading each row's document through LastDocument.
template <value_function_t fun>
void Extract(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<JSONReadFunctionData>();
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();
	LastDocument last(alc);

	auto &inputs = args.data[0];
	if (info.constant) {
		const char *ptr = info.ptr;
		const idx_t &len = info.len;
		if (info.path_type == JSONCommon::JSONPathType::REGULAR) {
			UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
			    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
				    auto val = JSONCommon::GetUnsafe(last.Read(input)->root, ptr, len);
				    if (!val) {
					    mask.SetInvalid(idx);
					    return string_t {};
				    }
				    return fun(val, alc, result, mask, idx);
			    });
		} else {
			D_ASSERT(info.path_type == JSONCommon::JSONPathType::WILDCARD);
			vector<yyjson_val *> vals;
			UnaryExecutor::Execute<string_t, list_entry_t>(inputs, result, args.size(), [&](string_t input) {
				vals.clear();
				JSONCommon::GetWildcardPath(last.Read(input)->root, ptr, len, vals);

				auto current_size = ListVector::GetListSize(result);
				auto new_size = current_size + vals.size();
				if (ListVector::GetListCapacity(result) < new_size) {
					ListVector::Reserve(result, new_size);
				}

				auto &child_entry = ListVector::GetEntry(result);
				auto child_vals = FlatVector::GetData<string_t>(child_entry);
				auto &child_validity = FlatVector::Validity(child_entry);
				for (idx_t i = 0; i < vals.size(); i++) {
					child_vals[current_size + i] = fun(vals[i], alc, child_entry, child_validity, current_size + i);
				}

				ListVector::SetListSize(result, new_size);
				return list_entry_t {current_size, vals.size()};
			});
		}
	} else {
		D_ASSERT(info.path_type == JSONCommon::JSONPathType::REGULAR);
		unique_ptr<Vector> casted_paths;
		if (args.data[1].GetType().id() == LogicalTypeId::VARCHAR) {
			casted_paths = make_uniq<Vector>(args.data[1]);
		} else {
			casted_paths = make_uniq<Vector>(LogicalTypeId::VARCHAR);
			VectorOperations::DefaultCast(args.data[1], *casted_paths, args.size(), true);
		}
		const bool integral = args.data[1].GetType().IsIntegral();
		BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
		    inputs, *casted_paths, result, args.size(),
		    [&](string_t input, string_t path, ValidityMask &mask, idx_t idx) {
			    auto val = JSONCommon::Get(last.Read(input)->root, path, integral);
			    if (!val) {
				    mask.SetInvalid(idx);
				    return string_t {};
			    }
			    return fun(val, alc, result, mask, idx);
		    });
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	JSONAllocator::AddBuffer(result, alc);
}

using raw_function_t = void (*)(DataChunk &, ExpressionState &, Vector &);

//! Has the overloads of name that take a document, VARCHAR or JSON, and one path, VARCHAR or BIGINT, run fast. A
//! database without the json extension has no such function, and is left as it is.
void Replace(ExtensionLoader &loader, const string &name, raw_function_t fast) {
	auto entry = loader.TryGetFunction(name);
	if (!entry || entry->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
		return;
	}
	for (auto &function : entry->Cast<ScalarFunctionCatalogEntry>().functions.functions) {
		if (function.arguments.size() != 2 || function.arguments[0].id() != LogicalTypeId::VARCHAR ||
		    (function.arguments[1].id() != LogicalTypeId::VARCHAR && function.arguments[1].id() != LogicalTypeId::BIGINT) ||
		    function.varargs.id() != LogicalTypeId::INVALID || function.GetBindCallback() != JSONReadFunctionData::Bind) {
			continue;
		}
		auto have = function.function.target<raw_function_t>();
		if (have && *have == fast) {
			continue; // this database's already
		}
		function.function = fast;
	}
}

} // namespace

void RegisterJSON(ExtensionLoader &loader) {
	for (auto name : {"json_extract", "json_extract_path"}) {
		Replace(loader, name, Extract<ExtractFromVal>);
	}
	for (auto name : {"json_extract_string", "json_extract_path_text", "->>"}) {
		Replace(loader, name, Extract<ExtractStringFromVal>);
	}
}

} // namespace rb
} // namespace duckdb
