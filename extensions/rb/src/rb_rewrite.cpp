//===----------------------------------------------------------------------===//
// rb: with rb_count_distinct = 'always', COUNT(DISTINCT x) of a 32-bit or
// narrower unsigned integer counts into roaring bitmaps.
//
// An optimizer extension, run after DuckDB's own optimizers, replaces each
// such aggregate with rb_count_distinct(x), which returns the same BIGINT.
// It looks at nothing but the expression and its type: whether bitmaps pay
// off is Igloo's decision (its estimates of a statement's rows, which
// DuckDB's misjudge by 20-34x on Iceberg tables), made per statement with
// this setting. Left alone: a FILTER or ORDER BY on the aggregate, other
// types, window aggregates, and COUNT(DISTINCT) of a struct, as pair counts
// are written (Igloo rewrites those into counts of rows).
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {
namespace rb {

static bool Always(ClientContext &context) {
	Value v;
	return context.TryGetCurrentSetting("rb_count_distinct", v) && !v.IsNull() &&
	       StringUtil::Lower(v.ToString()) == "always";
}

//! Whether e is COUNT(DISTINCT x) of an unsigned integer that UINTEGER holds, with nothing else attached.
static bool Eligible(const BoundAggregateExpression &e) {
	if (e.function.name != "count" || !e.IsDistinct() || e.filter || (e.order_bys && !e.order_bys->orders.empty()) ||
	    e.children.size() != 1) {
		return false;
	}
	switch (e.children[0]->return_type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return true;
	default:
		return false;
	}
}

static unique_ptr<Expression> Rewrite(ClientContext &context, BoundAggregateExpression &e) {
	auto &entry = Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, SYSTEM_CATALOG, DEFAULT_SCHEMA,
	                                                               "rb_count_distinct");
	auto function = entry.functions.GetFunctionByArguments(context, {LogicalType::UINTEGER});
	vector<unique_ptr<Expression>> children;
	children.push_back(BoundCastExpression::AddCastToType(context, std::move(e.children[0]), LogicalType::UINTEGER));
	return FunctionBinder(context).BindAggregateFunction(std::move(function), std::move(children));
}

static void Visit(ClientContext &context, LogicalOperator &op) {
	for (auto &child : op.children) {
		Visit(context, *child);
	}
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	for (auto &expr : op.Cast<LogicalAggregate>().expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &aggregate = expr->Cast<BoundAggregateExpression>();
		if (Eligible(aggregate)) {
			auto rewritten = Rewrite(context, aggregate);
			D_ASSERT(rewritten->return_type == expr->return_type);
			rewritten->alias = expr->alias;
			expr = std::move(rewritten);
		}
	}
}

static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (plan && Always(input.context)) {
		Visit(input.context, *plan);
	}
}

void RegisterRewrite(DBConfig &config) {
	OptimizerExtension extension;
	extension.optimize_function = Optimize;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace rb
} // namespace duckdb
