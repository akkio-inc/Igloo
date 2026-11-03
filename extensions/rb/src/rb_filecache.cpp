//===----------------------------------------------------------------------===//
// rb: what DuckDB holds of table files in memory, listed and put back.
//
// DuckDB keeps the ranges of files it reads in its external file cache, in
// buffers of its own memory, so a file's columns are read from storage once
// and then from memory. A stopped machine loses them, and its first queries
// read every range again, from the disk's read cache at the speed the disk
// is provisioned for (500 MB/s for TPC-H's 25 GB at scale 100: 50 s).
// Object storage serves the same ranges several times faster to many
// requests at once. So Igloo lists the cache's ranges while it runs
// (igloo_file_cache_ranges) and, after a start, reads them from object
// storage itself and hands them back (igloo_file_cache_put), each under the
// version DuckDB had for the file: none for Iceberg's data files, which
// DuckDB keeps without checking, and otherwise the ETag or modification time
// it checks against the file's own when a query opens it (see package
// engine's filecache.go).
//
// Only Igloo calls these: SQL from clients may not (see package pgwire's
// forbidden).
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache.hpp"

#include <cstring>
#include <set>

namespace duckdb {
namespace rb {

using CachedFile = ExternalFileCache::CachedFile;
using CachedFileRange = ExternalFileCache::CachedFileRange;
using Overlap = ExternalFileCache::CachedFileRangeOverlap;

//===--------------------------------------------------------------------===//
// igloo_file_cache_ranges(): path, location, nr_bytes, version_tag (a BLOB:
// a local file's is not text), last_modified (microseconds), file_size, and
// whether the range's buffer is in memory (loaded) or was dropped for room.
//===--------------------------------------------------------------------===//
struct RangeRow {
	string path;
	idx_t location;
	idx_t nr_bytes;
	string version_tag;
	int64_t last_modified;
	idx_t file_size;
	bool loaded;
};

struct RangesState : public GlobalTableFunctionState {
	vector<RangeRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> RangesBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                           vector<string> &names) {
	names = {"path", "location", "nr_bytes", "version_tag", "last_modified", "file_size", "loaded"};
	types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::BLOB,
	         LogicalType::BIGINT,  LogicalType::UBIGINT, LogicalType::BOOLEAN};
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> RangesInit(ClientContext &context, TableFunctionInitInput &) {
	auto state = make_uniq<RangesState>();
	auto &cache = ExternalFileCache::Get(context);
	if (!cache.IsEnabled()) {
		return std::move(state);
	}
	// The cache lists its files only with their ranges' places; a file's
	// metadata and its ranges' buffers come from the file's entry.
	std::set<string> paths;
	for (auto &info : cache.GetCachedFileInformation()) {
		paths.insert(info.path);
	}
	for (auto &path : paths) {
		auto file = cache.GetOrCreateCachedFile(path);
		auto guard = file->lock.GetSharedLock();
		for (auto &entry : file->Ranges(guard)) {
			auto &range = *entry.second;
			state->rows.push_back({path, range.location, range.nr_bytes, range.version_tag,
			                       file->LastModified(guard).value, file->FileSize(guard),
			                       !range.block_handle->GetMemory().IsUnloaded()});
		}
	}
	return std::move(state);
}

static void RangesFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<RangesState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		output.SetValue(0, count, Value(row.path));
		output.SetValue(1, count, Value::UBIGINT(row.location));
		output.SetValue(2, count, Value::UBIGINT(row.nr_bytes));
		output.SetValue(3, count, Value::BLOB_RAW(row.version_tag));
		output.SetValue(4, count, Value::BIGINT(row.last_modified));
		output.SetValue(5, count, Value::UBIGINT(row.file_size));
		output.SetValue(6, count, Value::BOOLEAN(row.loaded));
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// igloo_file_cache_put(path, location, data, version_tag, last_modified,
// file_size): puts data, the bytes of the file at path from location on,
// into the cache. 1 if it did; 0 if the cache is off, already holds them,
// or holds the file under another version (a file that changed since its
// ranges were listed); 2 if DuckDB has no memory to spare for them.
//===--------------------------------------------------------------------===//

//! Whether file's ranges hold [location, location + nr_bytes) whole, in memory.
static bool Holds(const unique_ptr<StorageLockKey> &guard, CachedFile &file, idx_t location, idx_t nr_bytes) {
	auto &ranges = file.Ranges(guard);
	auto it = ranges.upper_bound(location);
	if (it != ranges.begin()) {
		--it;
	}
	for (; it != ranges.end() && it->second->location <= location; ++it) {
		if (it->second->GetOverlap(nr_bytes, location) == Overlap::FULL &&
		    !it->second->block_handle->GetMemory().IsUnloaded()) {
			return true;
		}
	}
	return false;
}

//! Whether file is the version given, as a query opening it would decide (see ExternalFileCache::IsValid). A file
//! the cache has no metadata for yet takes the version given.
static bool SameVersion(const unique_ptr<StorageLockKey> &guard, CachedFile &file, const string &version_tag,
                        timestamp_t last_modified, idx_t file_size) {
	auto &tag = file.VersionTag(guard);
	auto &modified = file.LastModified(guard);
	if (tag.empty() && modified == timestamp_t(0) && file.Ranges(guard).empty()) {
		tag = version_tag;
		modified = last_modified;
		file.FileSize(guard) = file_size;
		file.CanSeek(guard) = true;
		file.OnDiskFile(guard) = false;
		return true;
	}
	if (!tag.empty() || !version_tag.empty()) {
		return tag == version_tag;
	}
	return modified == last_modified;
}

enum PutResult : int32_t { PUT_SKIPPED = 0, PUT_DONE = 1, PUT_NO_MEMORY = 2 };

static PutResult Put(ClientContext &context, const string &path, idx_t location, const string_t &data,
                const string &version_tag, timestamp_t last_modified, idx_t file_size) {
	auto &cache = ExternalFileCache::Get(context);
	const idx_t nr_bytes = data.GetSize();
	if (!cache.IsEnabled() || nr_bytes == 0 || location + nr_bytes > file_size) {
		return PUT_SKIPPED;
	}
	auto file = cache.GetOrCreateCachedFile(path);
	{
		auto guard = file->lock.GetExclusiveLock();
		if (!SameVersion(guard, *file, version_tag, last_modified, file_size) ||
		    Holds(guard, *file, location, nr_bytes)) {
			return PUT_SKIPPED;
		}
	}
	BufferHandle pin;
	try {
		pin = cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
	} catch (OutOfMemoryException &) {
		return PUT_NO_MEMORY;
	}
	memcpy(pin.Ptr(), data.GetData(), nr_bytes);
	auto range = make_shared_ptr<CachedFileRange>(pin.GetBlockHandle(), nr_bytes, location, version_tag);

	// As CachingFileHandle::TryInsertFileRange does: a query may have read
	// the range meanwhile, and ranges this one holds whole are dropped.
	auto guard = file->lock.GetExclusiveLock();
	if (!SameVersion(guard, *file, version_tag, last_modified, file_size) || Holds(guard, *file, location, nr_bytes)) {
		return PUT_SKIPPED;
	}
	auto &ranges = file->Ranges(guard);
	const idx_t end = location + nr_bytes;
	auto it = ranges.lower_bound(location);
	if (it != ranges.begin()) {
		--it;
	}
	while (it != ranges.end() && it->second->location < end) {
		auto &other = *it->second;
		if (other.GetOverlap(*range) == Overlap::FULL) {
			// It holds this one whole, but its buffer is gone (see Holds).
			it = ranges.erase(it);
			continue;
		}
		if (range->GetOverlap(other) == Overlap::FULL) {
			it = ranges.erase(it);
			continue;
		}
		++it;
	}
	range->AddCheckSum();
	ranges[location] = std::move(range);
	return PUT_DONE;
}

static void PutFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnifiedVectorFormat formats[6];
	for (idx_t i = 0; i < 6; i++) {
		args.data[i].ToUnifiedFormat(args.size(), formats[i]);
	}
	auto paths = UnifiedVectorFormat::GetData<string_t>(formats[0]);
	auto locations = UnifiedVectorFormat::GetData<uint64_t>(formats[1]);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(formats[2]);
	auto tags = UnifiedVectorFormat::GetData<string_t>(formats[3]);
	auto modified = UnifiedVectorFormat::GetData<int64_t>(formats[4]);
	auto sizes = UnifiedVectorFormat::GetData<uint64_t>(formats[5]);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<int32_t>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t row = 0; row < args.size(); row++) {
		idx_t at[6];
		bool null = false;
		for (idx_t i = 0; i < 6; i++) {
			at[i] = formats[i].sel->get_index(row);
			null = null || !formats[i].validity.RowIsValid(at[i]);
		}
		if (null) {
			validity.SetInvalid(row);
			continue;
		}
		out[row] = Put(context, paths[at[0]].GetString(), locations[at[1]], blobs[at[2]], tags[at[3]].GetString(),
		               timestamp_t(modified[at[4]]), sizes[at[5]]);
	}
}

void RegisterFileCache(ExtensionLoader &loader) {
	TableFunction ranges("igloo_file_cache_ranges", {}, RangesFunction, RangesBind, RangesInit);
	loader.RegisterFunction(ranges);
	ScalarFunction put("igloo_file_cache_put",
	                   {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::BLOB, LogicalType::BLOB,
	                    LogicalType::BIGINT, LogicalType::UBIGINT},
	                   LogicalType::INTEGER, PutFunction);
	put.SetVolatile();
	loader.RegisterFunction(put);
}

} // namespace rb
} // namespace duckdb
