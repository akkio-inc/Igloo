//===----------------------------------------------------------------------===//
// rb: comparisons with what a CASE of constants can never be.
//
// A view or a report names what a row is with a CASE whose results are constants,
//
//   SELECT *, CASE WHEN length(url) % 4 = 0 THEN 'search' ELSE 'page' END AS kind FROM visits
//
// and a statement over it asks for a kind there is none of, kind = 'click'. No row
// can pass. DuckDB does not see that: it reads the table, works out the CASE on
// every row, and joins and groups around it, for a statement that returns nothing.
// A pass before DuckDB's optimizers replaces such a comparison with false, and they
// take out what can then give no rows, tables and all. A dashboard's queries that
// asked for such a kind took a minute of the cores each at ten times its data, and
// next to nothing now.
//
// It follows a column through the projections above the CASE, by its binding. The
// results must all be constants and none NULL, so that the comparison is false and
// never NULL wherever it stands; a comparison under a collation or a cast of the
// column is not one it looks at.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {
namespace rb {

class ImpossibleComparisons : public LogicalOperatorVisitor {
public:
	explicit ImpossibleComparisons(ClientContext &context) : context(context) {
	}

	void VisitOperator(LogicalOperator &op) override {
		// From the bottom up, so that a column's results are known above it.
		VisitOperatorChildren(op);
		VisitOperatorExpressions(op);
		if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return;
		}
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			vector<Value> results;
			if (ResultsOf(*projection.expressions[i], results)) {
				columns[ColumnBinding(projection.table_index, i)] = std::move(results);
			}
		}
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundComparisonExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return nullptr;
		}
		const auto *results = ResultsOfColumn(*expr.left);
		auto &other = results ? *expr.right : *expr.left;
		if (!results) {
			results = ResultsOfColumn(*expr.right);
		}
		if (!results || !Impossible(*results, other)) {
			return nullptr;
		}
		return make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
	}

	unique_ptr<Expression> VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.GetExpressionType() != ExpressionType::COMPARE_IN || expr.children.size() < 2) {
			return nullptr;
		}
		const auto *results = ResultsOfColumn(*expr.children[0]);
		if (!results) {
			return nullptr;
		}
		for (idx_t i = 1; i < expr.children.size(); i++) {
			if (!Impossible(*results, *expr.children[i])) {
				return nullptr;
			}
		}
		return make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
	}

private:
	//! The results of a column known to be a CASE of constants, or nullptr.
	const vector<Value> *ResultsOfColumn(const Expression &expr) const {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return nullptr;
		}
		auto &column = expr.Cast<BoundColumnRefExpression>();
		if (column.depth != 0) {
			return nullptr;
		}
		auto found = columns.find(column.binding);
		return found == columns.end() ? nullptr : &found->second;
	}

	//! A constant's value, if expr is a constant that is not NULL.
	bool Constant(const Expression &expr, Value &value) const {
		if (!expr.IsFoldable()) {
			return false;
		}
		return ExpressionExecutor::TryEvaluateScalar(context, expr, value) && !value.IsNull();
	}

	//! Whether expr is a constant of the results' type that is none of them.
	bool Impossible(const vector<Value> &results, const Expression &expr) const {
		Value value;
		if (!Constant(expr, value)) {
			return false;
		}
		for (auto &result : results) {
			if (result.type() != value.type() || result == value) {
				return false;
			}
		}
		return true;
	}

	//! What expr can be, if that is a few constants.
	bool ResultsOf(const Expression &expr, vector<Value> &results) const {
		if (auto *known = ResultsOfColumn(expr)) {
			results = *known;
			return true;
		}
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_CASE) {
			return false;
		}
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		Value value;
		for (auto &check : case_expr.case_checks) {
			if (!Constant(*check.then_expr, value)) {
				return false;
			}
			results.push_back(value);
		}
		if (!case_expr.else_expr || !Constant(*case_expr.else_expr, value)) {
			return false;
		}
		results.push_back(value);
		return true;
	}

	ClientContext &context;
	column_binding_map_t<vector<Value>> columns;
};

static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (plan) {
		ImpossibleComparisons(input.context).VisitOperator(*plan);
	}
}

void RegisterFold(DBConfig &config) {
	OptimizerExtension extension;
	extension.pre_optimize_function = PreOptimize;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace rb
} // namespace duckdb
