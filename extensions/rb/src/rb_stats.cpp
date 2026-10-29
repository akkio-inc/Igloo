//===----------------------------------------------------------------------===//
// rb: distinct counts of Igloo's tables' columns for DuckDB's planner.
//
// DuckDB estimates the rows a scan returns after an equality filter
// (audience_id = 'x') as the table's rows over the column's distinct count,
// and the size of a join from its keys' distinct counts, when the scan's
// table function reports them. DuckDB-Iceberg reports none, so DuckDB takes
// a fifth of the table for any such filter: one audience of a few thousand
// rows in a lookup table of billions came out at hundreds of millions, and
// its join with tens of millions of rows of facts hashed the facts rather
// than the audience.
//
// Igloo keeps distinct counts of its tables' columns and hands them over with
// igloo_set_distinct_counts(spec), which replaces the database's counts. A
// pass before DuckDB's optimizers has every scan of a table with counts
// report them, along with whatever its table function reports itself; once
// the statement ends, scans report as before.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {
namespace rb {

//! A database's distinct counts: by lower-cased catalog.schema.table, then by lower-cased column.
struct DistinctCounts : public ObjectCacheEntry {
	static string ObjectType() {
		return "igloo_distinct_counts";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		// Never evicted: Igloo set them, and only Igloo replaces them.
		return optional_idx();
	}

	mutex lock;
	unordered_map<string, unordered_map<string, idx_t>> tables;
};

static shared_ptr<DistinctCounts> CountsOf(ClientContext &context) {
	return ObjectCache::GetObjectCache(context).GetOrCreate<DistinctCounts>(DistinctCounts::ObjectType());
}

//! igloo_set_distinct_counts(spec): spec is one line a column, "catalog.schema.table\tcolumn\tcount"; it replaces
//! the database's counts and returns how many columns it holds.
static void SetCounts(DataChunk &args, ExpressionState &state, Vector &result) {
	auto counts = CountsOf(state.GetContext());
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(), [&](string_t spec) {
		unordered_map<string, unordered_map<string, idx_t>> tables;
		int64_t columns = 0;
		for (auto &line : StringUtil::Split(spec.GetString(), '\n')) {
			if (line.empty()) {
				continue;
			}
			auto fields = StringUtil::Split(line, '\t');
			if (fields.size() != 3) {
				throw InvalidInputException("igloo_set_distinct_counts: \"%s\" is not table, column and count", line);
			}
			idx_t count;
			try {
				count = std::stoull(fields[2]);
			} catch (std::exception &) {
				throw InvalidInputException("igloo_set_distinct_counts: \"%s\" is not a count", fields[2]);
			}
			if (count > 0) {
				tables[StringUtil::Lower(fields[0])][StringUtil::Lower(fields[1])] = count;
				columns++;
			}
		}
		lock_guard<mutex> guard(counts->lock);
		counts->tables = std::move(tables);
		return columns;
	});
}

//! A scan that reports counts: its table function's own statistics, and the counts by column.
struct CountedScan {
	table_statistics_t statistics = nullptr;
	table_statistics_extended_t statistics_extended = nullptr;
	vector<idx_t> counts;
	vector<LogicalType> types;
};

//! The scans that report counts, by their bind data, which is theirs alone while their statement is planned.
static mutex scans_lock;
static unordered_map<const FunctionData *, CountedScan> scans;

static unique_ptr<BaseStatistics> Statistics(ClientContext &context, TableFunctionGetStatisticsInput &input) {
	CountedScan scan;
	{
		lock_guard<mutex> guard(scans_lock);
		auto it = scans.find(input.bind_data.get());
		if (it == scans.end()) {
			// A copy the optimizer made of a scan, with bind data of its own: as the scan of a table of many files
			// DuckDB-Iceberg reports nothing.
			return nullptr;
		}
		scan = it->second;
	}
	unique_ptr<BaseStatistics> stats;
	if (scan.statistics_extended) {
		stats = scan.statistics_extended(context, input);
	} else if (scan.statistics) {
		stats = scan.statistics(context, input.bind_data.get(), input.column_index.GetPrimaryIndex());
	}
	auto column = input.column_index.GetPrimaryIndex();
	if (input.column_index.HasChildren() || column >= scan.counts.size() || scan.counts[column] == 0) {
		return stats;
	}
	if (!stats) {
		stats = BaseStatistics::CreateUnknown(scan.types[column]).ToUnique();
	}
	stats->SetDistinctCount(scan.counts[column]);
	return stats;
}

//! The statement's scans that report counts, forgotten when it ends.
struct CountedScans : public ClientContextState {
	vector<const FunctionData *> mine;

	void QueryEnd() override {
		lock_guard<mutex> guard(scans_lock);
		for (auto data : mine) {
			scans.erase(data);
		}
		mine.clear();
	}
};

static void Count(ClientContext &context, LogicalOperator &op, DistinctCounts &counts, CountedScans &state) {
	for (auto &child : op.children) {
		Count(context, *child, counts, state);
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op.Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || !get.bind_data || get.function.statistics_extended == Statistics) {
		return;
	}
	auto key = StringUtil::Lower(table->ParentCatalog().GetName() + "." + table->ParentSchema().name + "." +
	                             table->name);
	CountedScan scan;
	{
		lock_guard<mutex> guard(counts.lock);
		auto it = counts.tables.find(key);
		if (it == counts.tables.end()) {
			return;
		}
		scan.counts.resize(get.names.size());
		for (idx_t i = 0; i < get.names.size(); i++) {
			auto column = it->second.find(StringUtil::Lower(get.names[i]));
			if (column != it->second.end()) {
				scan.counts[i] = column->second;
			}
		}
	}
	scan.types = get.returned_types;
	scan.statistics = get.function.statistics;
	scan.statistics_extended = get.function.statistics_extended;
	{
		lock_guard<mutex> guard(scans_lock);
		scans[get.bind_data.get()] = std::move(scan);
	}
	state.mine.push_back(get.bind_data.get());
	get.function.statistics = nullptr;
	get.function.statistics_extended = Statistics;
}

static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	auto counts = CountsOf(input.context);
	{
		lock_guard<mutex> guard(counts->lock);
		if (counts->tables.empty()) {
			return;
		}
	}
	auto state = input.context.registered_state->GetOrCreate<CountedScans>("igloo_counted_scans");
	Count(input.context, *plan, *counts, *state);
}

void RegisterStats(ExtensionLoader &loader, DBConfig &config) {
	ScalarFunction set("igloo_set_distinct_counts", {LogicalType::VARCHAR}, LogicalType::BIGINT, SetCounts);
	set.SetVolatile();
	loader.RegisterFunction(set);
	OptimizerExtension extension;
	extension.pre_optimize_function = PreOptimize;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace rb
} // namespace duckdb
