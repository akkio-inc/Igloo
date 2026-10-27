//===----------------------------------------------------------------------===//
// rb: Snowflake's division, as Igloo's sf_div macro divides, bound as one
// function.
//
// The Snowflake translator writes a / b as sf_div(a, b), macros that pick
// how to divide by the operands' types: integers exactly, rounded half away
// from zero to 6 decimal places; operands with a scale, one at least a
// decimal, to the dividend's scale plus 6, at most 12 (or its own, if more);
// any other number as DuckDB's / divides it; and by zero as an error. DuckDB
// expands a macro into its body wherever it is called, and the sf_div
// macros name each operand some 25 times across their branches, all of
// which it binds: a dashboard's statement of 76 divisions spent three
// quarters of its 210 ms prepare binding them, and DuckDB binds a statement
// that reads an Iceberg catalog again each time it runs.
//
// igloo_sf_div(a, b) picks the branch once, from the types it is bound
// with, and gives what the macros give, as a DOUBLE: the same arithmetic,
// the same rounding (DuckDB's round), the same error. It refuses the
// operands the macros cannot bind, as anything but numbers and NULL.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include <cmath>

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
namespace rb {

enum class DivideMode : uint8_t {
	//! DuckDB's / : in FLOAT when it is that, else in DOUBLE
	PLAIN_FLOAT,
	PLAIN_DOUBLE,
	//! both operands integers: exactly, to 6 places
	INTEGERS,
	//! a decimal among integers and decimals: to digits places
	SCALED,
};

struct DivideData : public FunctionData {
	DivideMode mode = DivideMode::PLAIN_DOUBLE;
	int32_t digits = 0;
	//! whether each operand was a HUGEINT, which converts to DOUBLE as DuckDB converts a HUGEINT
	bool huge_left = false, huge_right = false;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DivideData>(*this);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DivideData>();
		return mode == other.mode && digits == other.digits && huge_left == other.huge_left &&
		       huge_right == other.huge_right;
	}
};

//! What the macros' sf_num_scale(typeof(x)) is for a value of type t: 0 for the integer types it names, a
//! decimal's scale, -1 (NULL) for any other number or a NULL; -2 for what is not a number.
static int32_t ScaleOf(const LogicalType &t) {
	switch (t.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return 0;
	case LogicalTypeId::DECIMAL:
		return DecimalType::GetScale(t);
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::SQLNULL:
		return -1;
	default:
		return -2;
	}
}

static unique_ptr<FunctionData> BindDivide(ClientContext &, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	LogicalType types[2];
	for (idx_t i = 0; i < 2; i++) {
		types[i] = arguments[i]->return_type;
		if (types[i].IsUnknown()) {
			// a parameter: bound again once its value's type is known (and a macro's, when it is used)
			throw ParameterNotResolvedException();
		}
		// a literal is typed as the integer it holds, as typeof() names it
		if (types[i].id() == LogicalTypeId::INTEGER_LITERAL) {
			types[i] = IntegerLiteral::GetType(types[i]);
		}
	}
	auto &a = types[0], &b = types[1];
	const int32_t s1 = ScaleOf(a), s2 = ScaleOf(b);
	if (s1 == -2 || s2 == -2) {
		throw BinderException("No function matches the given name and argument types '/(%s, %s)'. You might need "
		                      "to add explicit type casts.",
		                      arguments[0]->return_type.ToString(), arguments[1]->return_type.ToString());
	}
	auto data = make_uniq<DivideData>();
	LogicalType operand;
	if (s1 < 0 || s2 < 0) {
		// DuckDB's / divides in FLOAT when one side is a FLOAT and neither a DOUBLE
		const bool single = (a.id() == LogicalTypeId::FLOAT || b.id() == LogicalTypeId::FLOAT) &&
		                    a.id() != LogicalTypeId::DOUBLE && b.id() != LogicalTypeId::DOUBLE;
		data->mode = single ? DivideMode::PLAIN_FLOAT : DivideMode::PLAIN_DOUBLE;
		operand = single ? LogicalType::FLOAT : LogicalType::DOUBLE;
	} else if (s1 == 0 && s2 == 0) {
		data->mode = DivideMode::INTEGERS;
		data->huge_left = a.id() == LogicalTypeId::HUGEINT;
		data->huge_right = b.id() == LogicalTypeId::HUGEINT;
		operand = LogicalType::HUGEINT;
	} else {
		data->mode = DivideMode::SCALED;
		data->digits = MaxValue(s1, MinValue(s1 + 6, 12));
		operand = LogicalType::DOUBLE;
	}
	bound_function.arguments = {operand, operand};
	return std::move(data);
}

//! DuckDB's round(x, digits) of a DOUBLE, digits >= 0.
static double Round(double input, int32_t digits) {
	double modifier = std::pow(10, double(digits));
	double rounded = std::round(input * modifier) / modifier;
	if (std::isinf(rounded) || std::isnan(rounded)) {
		return input;
	}
	return rounded;
}

//! What the macros multiply a quotient by before they round it, to move it away from zero by more than its error.
static constexpr double NUDGE = 1 + 1e-15;

//! DuckDB's CAST of a HUGEINT to DOUBLE.
static double HugeToDouble(hugeint_t x) {
	if (x.upper == -1) {
		return -double(NumericLimits<uint64_t>::Maximum() - x.lower) - 1;
	}
	return double(x.lower) + double(x.upper) * (double(NumericLimits<uint64_t>::Maximum()) + 1);
}

static __int128 Wide(hugeint_t x) {
	return static_cast<__int128>(static_cast<unsigned __int128>(static_cast<uint64_t>(x.upper)) << 64 | x.lower);
}

static hugeint_t Narrow(__int128 x) {
	hugeint_t r;
	r.lower = static_cast<uint64_t>(x);
	r.upper = static_cast<int64_t>(x >> 64);
	return r;
}

//! sf_div_int: integers divided exactly, in BIGINT while it holds the operands, then in HUGEINT, rounded half
//! away from zero to 6 places; past HUGEINT's reach, the DOUBLE quotient rounded.
static double DivideIntegers(hugeint_t a_p, hugeint_t b_p, const DivideData &data) {
	const __int128 a = Wide(a_p), b = Wide(b_p);
	if (b == 0) {
		throw InvalidInputException("Division by zero");
	}
	const unsigned __int128 abs_a = a < 0 ? -static_cast<unsigned __int128>(a) : static_cast<unsigned __int128>(a);
	const unsigned __int128 abs_b = b < 0 ? -static_cast<unsigned __int128>(b) : static_cast<unsigned __int128>(b);
	const int sign = (a > 0) - (a < 0);
	const int sign_b = (b > 0) - (b < 0);
	static const unsigned __int128 E18 = 1000000000000000000ULL;
	if (abs_a < 4000000000000ULL && abs_b < E18) {
		const int64_t x = static_cast<int64_t>(abs_a), y = static_cast<int64_t>(abs_b);
		const int64_t q = (2 * x * 1000000 + y) / (2 * y);
		return double(int64_t(sign * sign_b) * q) / 1000000;
	}
	if (abs_a < E18 * 1000000000000ULL && abs_b < E18 * E18 * 10) {
		const __int128 q = static_cast<__int128>((2 * abs_a * 1000000 + abs_b) / (2 * abs_b));
		return HugeToDouble(Narrow(sign * sign_b * q)) / 1000000;
	}
	// DuckDB's / casts each operand to DOUBLE from its own type; a narrower one fits in a BIGINT
	const double x = data.huge_left ? HugeToDouble(a_p) : double(static_cast<int64_t>(a));
	const double y = data.huge_right ? HugeToDouble(b_p) : double(static_cast<int64_t>(b));
	return Round(x / y * NUDGE, 6);
}

static void Divide(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<DivideData>();
	auto &left = args.data[0], &right = args.data[1];
	const auto count = args.size();
	switch (data.mode) {
	case DivideMode::PLAIN_FLOAT:
		BinaryExecutor::Execute<float, float, double>(left, right, result, count, [](float a, float b) {
			if (b == 0) {
				throw InvalidInputException("Division by zero");
			}
			return double(a / b);
		});
		break;
	case DivideMode::PLAIN_DOUBLE:
		BinaryExecutor::Execute<double, double, double>(left, right, result, count, [](double a, double b) {
			if (b == 0) {
				throw InvalidInputException("Division by zero");
			}
			return a / b;
		});
		break;
	case DivideMode::SCALED: {
		const int32_t digits = data.digits;
		BinaryExecutor::Execute<double, double, double>(left, right, result, count, [digits](double a, double b) {
			if (b == 0) {
				throw InvalidInputException("Division by zero");
			}
			return Round(a / b * NUDGE, digits);
		});
		break;
	}
	case DivideMode::INTEGERS:
		BinaryExecutor::Execute<hugeint_t, hugeint_t, double>(
		    left, right, result, count, [&data](hugeint_t a, hugeint_t b) { return DivideIntegers(a, b, data); });
		break;
	}
}

void RegisterDivide(ExtensionLoader &loader) {
	ScalarFunction divide("igloo_sf_div", {LogicalType::ANY, LogicalType::ANY}, LogicalType::DOUBLE, Divide,
	                      BindDivide);
	divide.SetFallible();
	// Bound even with a NULL operand, which the macros refuse beside what is not a number (each row's NULLs are
	// NULL all the same).
	divide.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(divide);
}

} // namespace rb
} // namespace duckdb
