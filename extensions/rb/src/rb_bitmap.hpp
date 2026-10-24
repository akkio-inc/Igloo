//===----------------------------------------------------------------------===//
// rb: roaring bitmaps for DuckDB (see extensions/rb/README.md)
//
// Shared helpers: reading a bitmap from a BLOB (validated) and writing one into
// a result vector, in CRoaring's portable format.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "roaring.h"

#include <memory>

namespace duckdb {
namespace rb {

struct BitmapDeleter {
	void operator()(roaring_bitmap_t *bm) const {
		roaring_bitmap_free(bm);
	}
};

//! A bitmap this code owns.
using Bitmap = std::unique_ptr<roaring_bitmap_t, BitmapDeleter>;

//! Reads a BLOB in CRoaring's portable format. Throws InvalidInputException on anything else: a truncated or
//! padded BLOB, or one whose containers are inconsistent.
Bitmap ReadBitmap(const string_t &blob);

//! Writes bm into result's string heap in the portable format, run-length encoding containers where that is
//! smaller (which changes bm's representation, not its contents).
string_t WriteBitmap(Vector &result, roaring_bitmap_t *bm);

void RegisterAggregates(ExtensionLoader &loader);
void RegisterScalars(ExtensionLoader &loader);
//! The optimizer rule that, with rb_count_distinct = 'always', counts COUNT(DISTINCT) of unsigned ids in bitmaps
void RegisterRewrite(DBConfig &config);

} // namespace rb
} // namespace duckdb
