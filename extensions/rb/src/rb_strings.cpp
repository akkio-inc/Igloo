//===----------------------------------------------------------------------===//
// rb: trim, ltrim and rtrim with DuckDB's results, faster.
//
// DuckDB's trim decodes every character of a string to look up its Unicode
// category, the space separators (Zs) being what it trims, and copies what
// is left even when it trimmed nothing: some 300 ns a row for short strings.
// Igloo's reports trim the values they filter and group by: a report's four
// filters of TRIM(LOWER(x)) took 154 s of its 381 s of threads over tens of
// millions of values, where comparing the values as they were took 2.5 s.
//
// At load, the one-argument trim, ltrim and rtrim become these, which read a
// space by its byte and decode only the characters at the ends of a string
// that are not ASCII, for the other space separators. A string they leave as
// it is is that string, not a copy, and a trimmed one is the part of it that
// is left; the result keeps the input's strings alive. (DuckDB's lower and
// upper, which copy every string too, are no slower than the same done by
// bytes for ASCII.)
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace rb {

//! Unicode's space separators (category Zs), which DuckDB's trim trims.
static bool IsSpaceSeparator(uint32_t c) {
	return c == 0x20 || c == 0xA0 || c == 0x1680 || (c >= 0x2000 && c <= 0x200A) || c == 0x202F || c == 0x205F ||
	       c == 0x3000;
}

//! The code point that the UTF-8 at s, n bytes, starts with, and its length in len: 0 when none starts there.
static uint32_t Decode(const unsigned char *s, idx_t n, idx_t &len) {
	auto c = s[0];
	if (c < 0x80) {
		len = 1;
		return c;
	}
	if ((c & 0xE0) == 0xC0 && n >= 2) {
		len = 2;
		return (uint32_t(c & 0x1F) << 6) | (s[1] & 0x3F);
	}
	if ((c & 0xF0) == 0xE0 && n >= 3) {
		len = 3;
		return (uint32_t(c & 0x0F) << 12) | (uint32_t(s[1] & 0x3F) << 6) | (s[2] & 0x3F);
	}
	if ((c & 0xF8) == 0xF0 && n >= 4) {
		len = 4;
		return (uint32_t(c & 0x07) << 18) | (uint32_t(s[1] & 0x3F) << 12) | (uint32_t(s[2] & 0x3F) << 6) |
		       (s[3] & 0x3F);
	}
	len = 0;
	return 0;
}

template <bool LEFT, bool RIGHT>
static string_t Trim(string_t input) {
	auto data = reinterpret_cast<const unsigned char *>(input.GetData());
	idx_t size = input.GetSize();
	idx_t begin = 0;
	idx_t end = size;
	if (LEFT) {
		while (begin < size) {
			if (data[begin] == ' ') {
				begin++;
				continue;
			}
			if (data[begin] < 0x80) {
				break;
			}
			idx_t len;
			auto c = Decode(data + begin, size - begin, len);
			if (len == 0 || !IsSpaceSeparator(c)) {
				break;
			}
			begin += len;
		}
	}
	if (RIGHT) {
		while (end > begin) {
			auto last = data[end - 1];
			if (last == ' ') {
				end--;
				continue;
			}
			if (last < 0x80) {
				break;
			}
			// The character the last byte ends: back over its continuation bytes.
			idx_t start = end - 1;
			while (start > begin && end - start < 4 && (data[start] & 0xC0) == 0x80) {
				start--;
			}
			idx_t len;
			auto c = Decode(data + start, end - start, len);
			if (len != end - start || !IsSpaceSeparator(c)) {
				break;
			}
			end = start;
		}
	}
	if (begin == 0 && end == size) {
		return input;
	}
	// A string of 12 bytes or fewer is copied into the string_t; a longer one points into the input's.
	return string_t(input.GetData() + begin, UnsafeNumericCast<uint32_t>(end - begin));
}

template <bool LEFT, bool RIGHT>
static void TrimFunction(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), Trim<LEFT, RIGHT>);
	StringVector::AddHeapReference(result, args.data[0]);
}

using raw_function_t = void (*)(DataChunk &, ExpressionState &, Vector &);

//! Has the one-argument VARCHAR overload of name call fast.
static void Replace(ExtensionLoader &loader, const string &name, raw_function_t fast) {
	auto &entry = loader.GetFunction(name);
	for (auto &function : entry.functions.functions) {
		if (function.arguments.size() != 1 || function.arguments[0].id() != LogicalTypeId::VARCHAR ||
		    function.return_type.id() != LogicalTypeId::VARCHAR || function.varargs.id() != LogicalTypeId::INVALID) {
			continue;
		}
		auto have = function.function.target<raw_function_t>();
		if (have && *have == fast) {
			continue; // this database's already
		}
		function.function = fast;
	}
}

void RegisterStrings(ExtensionLoader &loader) {
	Replace(loader, "trim", TrimFunction<true, true>);
	Replace(loader, "ltrim", TrimFunction<true, false>);
	Replace(loader, "rtrim", TrimFunction<false, true>);
}

} // namespace rb
} // namespace duckdb
