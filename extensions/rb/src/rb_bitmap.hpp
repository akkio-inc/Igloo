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
//! igloo_set_distinct_counts and the pass that has scans report the counts (rb_stats.cpp).
void RegisterStats(ExtensionLoader &loader, DBConfig &config);
//! Has trim, ltrim and rtrim of a string run faster, with DuckDB's results (rb_strings.cpp).
void RegisterStrings(ExtensionLoader &loader);
//! igloo_sf_div, Snowflake's division as Igloo's sf_div macro divides, bound as one function (rb_divide.cpp).
void RegisterDivide(ExtensionLoader &loader);

} // namespace rb
} // namespace duckdb
