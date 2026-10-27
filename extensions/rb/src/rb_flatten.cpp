//===----------------------------------------------------------------------===//
// rb: Snowflake FLATTEN's lists, as Igloo's sf_flatten_ macros give them,
// bound as one function each.
//
// The Snowflake translator writes FLATTEN as unnests of four lists of its
// input x, which the sf_flatten_values, _strings, _keys and _indexes macros
// make with MODE m: a list of strings and a map of strings give theirs as
// they are, text and JSON those of the JSON they hold, any other type those
// of its JSON. The macros pick by typeof(x), a constant, so a view keeps
// working when its table's column changes type, but DuckDB binds every
// branch of a macro wherever it is called, and the casts, lambdas and
// nested macros of all four kinds took about a millisecond a call to bind:
// most of what a dashboard's statements of a dozen FLATTENs spent preparing,
// which DuckDB does again each time a statement that reads an Iceberg
// catalog runs.
//
// igloo_sf_flatten_values(x, m) and the others pick the kind once, from the
// type they are bound with, and bind only that kind's branch, of the same
// DuckDB functions the macros call (json_type, json_extract, json_keys,
// to_json and the others), so that they give the same lists, to the byte,
// and the same errors. For text, JSON and other types the branch replaces
// the call (bind_expression). A list's or a map's elements, which the
// macros read through list_transform's lambda, are read here: the element
// expression the lambda's body is, bound once, runs over the elements of
// the lists, and the results become the lists' elements in their order.
// MODE must be a constant, as the translator writes it.
//===----------------------------------------------------------------------===//
#include "rb_bitmap.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {
namespace rb {

//! Which of FLATTEN's lists a function makes.
enum class FlattenList : uint8_t { VALUES, STRINGS, KEYS, INDEXES };

//! The kinds of input the macros tell apart by typeof(x).
enum class FlattenKind : uint8_t {
	//! VARCHAR or JSON: the JSON the text is
	TEXT,
	//! VARCHAR[]: Igloo's ARRAY column, a list of strings
	LIST,
	//! MAP(VARCHAR, VARCHAR): Igloo's OBJECT column
	MAP,
	//! a NULL of no type, as a NULL literal is
	NULLS,
	//! anything else: the JSON it casts to
	OTHER,
};

static FlattenKind KindOf(const LogicalType &type_p) {
	// typeof(x) is the type's name; a literal is typed as what typeof names it.
	auto type = type_p;
	if (type.id() == LogicalTypeId::STRING_LITERAL) {
		type = LogicalType::VARCHAR;
	} else if (type.id() == LogicalTypeId::INTEGER_LITERAL) {
		type = IntegerLiteral::GetType(type);
	}
	const auto name = type.ToString();
	if (name == "VARCHAR" || name == "JSON") {
		return FlattenKind::TEXT;
	}
	if (name == "VARCHAR[]") {
		return FlattenKind::LIST;
	}
	if (name == "MAP(VARCHAR, VARCHAR)") {
		return FlattenKind::MAP;
	}
	if (type.id() == LogicalTypeId::SQLNULL) {
		return FlattenKind::NULLS;
	}
	return FlattenKind::OTHER;
}

static LogicalType ResultType(FlattenList list, FlattenKind kind) {
	switch (list) {
	case FlattenList::VALUES:
		// The macro's CASE over a NULL of no type comes out as JSON, not a list.
		return kind == FlattenKind::NULLS ? LogicalType::JSON() : LogicalType::LIST(LogicalType::JSON());
	case FlattenList::STRINGS:
	case FlattenList::KEYS:
		return LogicalType::LIST(LogicalType::VARCHAR);
	case FlattenList::INDEXES:
		return LogicalType::LIST(LogicalType::BIGINT);
	}
	throw InternalException("unknown FLATTEN list");
}

//! MODE, a constant: NULL, or its text.
struct FlattenMode {
	bool null = true;
	string text;

	//! m <> s and m = s as a CASE's WHEN reads them: a NULL is false either way.
	bool Not(const char *s) const {
		return !null && text != s;
	}
	bool Is(const char *s) const {
		return !null && text == s;
	}
};

struct FlattenData : public FunctionData {
	FlattenList list = FlattenList::VALUES;
	FlattenKind kind = FlattenKind::OTHER;
	FlattenMode mode;
	//! Set when the function reads a list's or a map's elements itself: the expression of each element, #0.
	unique_ptr<Expression> element;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<FlattenData>();
		copy->list = list;
		copy->kind = kind;
		copy->mode = mode;
		copy->element = element ? element->Copy() : nullptr;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<FlattenData>();
		return list == other.list && kind == other.kind && mode.null == other.mode.null &&
		       mode.text == other.mode.text && Expression::Equals(element, other.element);
	}
};

//! Builds bound expressions of DuckDB's own functions, as the binder binds the macros' bodies.
struct Bound {
	ClientContext &context;

	unique_ptr<Expression> Fn(const string &name, vector<unique_ptr<Expression>> args) const {
		ErrorData error;
		auto result = FunctionBinder(context).BindScalarFunction(DEFAULT_SCHEMA, name, std::move(args), error);
		if (!result) {
			error.Throw();
		}
		return result;
	}
	unique_ptr<Expression> Fn(const string &name, unique_ptr<Expression> a) const {
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(a));
		return Fn(name, std::move(args));
	}
	unique_ptr<Expression> Fn(const string &name, unique_ptr<Expression> a, unique_ptr<Expression> b) const {
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(a));
		args.push_back(std::move(b));
		return Fn(name, std::move(args));
	}
	static unique_ptr<Expression> Text(const string &s) {
		return make_uniq<BoundConstantExpression>(Value(s));
	}
	static unique_ptr<Expression> Null(const LogicalType &type) {
		return make_uniq<BoundConstantExpression>(Value(type));
	}
	unique_ptr<Expression> Cast(unique_ptr<Expression> e, const LogicalType &type) const {
		return BoundCastExpression::AddCastToType(context, std::move(e), type);
	}
	static unique_ptr<Expression> Equal(unique_ptr<Expression> e, const string &s) {
		return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(e), Text(s));
	}
	//! CASE WHEN when THEN then END, of type.
	unique_ptr<Expression> When(unique_ptr<Expression> when, unique_ptr<Expression> then,
	                            const LogicalType &type) const {
		return make_uniq<BoundCaseExpression>(std::move(when), Cast(std::move(then), type), Null(type));
	}
	//! CASE WHEN when THEN then ... END, of type.
	unique_ptr<Expression> Cases(vector<std::pair<unique_ptr<Expression>, unique_ptr<Expression>>> checks,
	                             const LogicalType &type) const {
		auto result = make_uniq<BoundCaseExpression>(type);
		for (auto &check : checks) {
			BoundCaseCheck c;
			c.when_expr = std::move(check.first);
			c.then_expr = Cast(std::move(check.second), type);
			result->case_checks.push_back(std::move(c));
		}
		result->else_expr = Null(type);
		return std::move(result);
	}

	//! sf_elem(e): an element of a list or map of strings as JSON, an object's or array's text as that JSON.
	unique_ptr<Expression> Elem(unique_ptr<Expression> e) const {
		auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->children.push_back(e->Copy());
		vector<std::pair<unique_ptr<Expression>, unique_ptr<Expression>>> checks;
		checks.emplace_back(std::move(is_null), Cast(Text("null"), LogicalType::JSON()));
		checks.emplace_back(Structured(e->Copy()), Cast(e->Copy(), LogicalType::JSON()));
		auto result = Cases(std::move(checks), LogicalType::JSON());
		result->Cast<BoundCaseExpression>().else_expr = Cast(Fn("to_json", std::move(e)), LogicalType::JSON());
		return result;
	}
	//! sf_elem_string(e): an element as text, an object's or array's text as json_extract_string reads it.
	unique_ptr<Expression> ElemString(unique_ptr<Expression> e) const {
		vector<std::pair<unique_ptr<Expression>, unique_ptr<Expression>>> checks;
		checks.emplace_back(Structured(e->Copy()), Fn("json_extract_string", e->Copy(), Text("$")));
		auto result = Cases(std::move(checks), LogicalType::VARCHAR);
		result->Cast<BoundCaseExpression>().else_expr = Cast(std::move(e), LogicalType::VARCHAR);
		return result;
	}
	//! (starts_with(e, '{') OR starts_with(e, '[')) AND json_valid(e)
	unique_ptr<Expression> Structured(unique_ptr<Expression> e) const {
		auto open = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR,
		                                                  Fn("starts_with", e->Copy(), Text("{")),
		                                                  Fn("starts_with", e->Copy(), Text("[")));
		return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(open),
		                                             Fn("json_valid", std::move(e)));
	}
};

//! The text or JSON the macros read the JSON of x from, for kind: CAST(x AS VARCHAR) for text and JSON,
//! sf_json(x), which is CAST(x AS JSON) for the other kinds.
static unique_ptr<Expression> JSONOf(const Bound &b, const Expression &x, FlattenKind kind) {
	return kind == FlattenKind::TEXT ? b.Cast(x.Copy(), LogicalType::VARCHAR) : b.Cast(x.Copy(), LogicalType::JSON());
}

//! What the macro's branch for text, JSON and other kinds gives: CASE json_type(j) WHEN 'ARRAY' THEN CASE WHEN
//! m <> 'OBJECT' THEN extract(j, '$[*]') END WHEN 'OBJECT' THEN CASE WHEN m <> 'ARRAY' THEN extract(j, '$.*')
//! END END, extract json_extract for values and json_extract_string for strings (which gives what
//! json_extract_string of each value gives).
static unique_ptr<Expression> JSONElements(const Bound &b, const FlattenData &data, const Expression &x,
                                           const LogicalType &type) {
	const string extract = data.list == FlattenList::STRINGS ? "json_extract_string" : "json_extract";
	auto json = JSONOf(b, x, data.kind);
	vector<std::pair<unique_ptr<Expression>, unique_ptr<Expression>>> checks;
	checks.emplace_back(Bound::Equal(b.Fn("json_type", json->Copy()), "ARRAY"),
	                    data.mode.Not("OBJECT") ? b.Fn(extract, json->Copy(), Bound::Text("$[*]")) : Bound::Null(type));
	checks.emplace_back(Bound::Equal(b.Fn("json_type", json->Copy()), "OBJECT"),
	                    data.mode.Not("ARRAY") ? b.Fn(extract, json->Copy(), Bound::Text("$.*")) : Bound::Null(type));
	return b.Cases(std::move(checks), type);
}

//! The call bound as the branch its kind takes, or nullptr where the function reads the elements itself.
static unique_ptr<Expression> BindBranch(FunctionBindExpressionInput &input) {
	auto &data = input.bind_data->Cast<FlattenData>();
	if (data.element) {
		return nullptr;
	}
	Bound b {input.context};
	const auto &x = *input.children[0];
	const auto type = ResultType(data.list, data.kind);
	switch (data.list) {
	case FlattenList::VALUES:
	case FlattenList::STRINGS:
		switch (data.kind) {
		case FlattenKind::NULLS:
			return Bound::Null(type);
		case FlattenKind::TEXT:
		case FlattenKind::OTHER:
			return JSONElements(b, data, x, type);
		case FlattenKind::LIST:
		case FlattenKind::MAP:
			// MODE leaves the list or map out (see BindFlatten).
			return Bound::Null(type);
		}
		break;
	case FlattenList::KEYS:
		// CASE WHEN m = 'ARRAY' OR typeof(x) = 'VARCHAR[]' THEN NULL ...
		if (data.kind == FlattenKind::LIST || data.mode.Is("ARRAY")) {
			return Bound::Null(type);
		}
		if (data.kind == FlattenKind::MAP) {
			return b.Fn("map_keys", x.Copy());
		} else {
			// The test first: arguments are evaluated in no set order, and one that moved json before the
			// other copied it read a NULL pointer (on x86-64, not on arm64).
			auto json = JSONOf(b, x, data.kind);
			auto is_object = Bound::Equal(b.Fn("json_type", json->Copy()), "OBJECT");
			return b.When(std::move(is_object), b.Fn("json_keys", std::move(json)), type);
		}
	case FlattenList::INDEXES:
		// CASE WHEN m = 'OBJECT' OR typeof(x) = 'MAP(VARCHAR, VARCHAR)' THEN NULL ...
		if (data.kind == FlattenKind::MAP || data.mode.Is("OBJECT")) {
			return Bound::Null(type);
		}
		if (data.kind == FlattenKind::LIST) {
			return b.Fn("range", b.Cast(b.Fn("len", x.Copy()), LogicalType::BIGINT));
		} else {
			// The test first, as for keys.
			auto json = JSONOf(b, x, data.kind);
			auto is_array = Bound::Equal(b.Fn("json_type", json->Copy()), "ARRAY");
			return b.When(std::move(is_array),
			              b.Fn("range", b.Cast(b.Fn("json_array_length", std::move(json)), LogicalType::BIGINT)), type);
		}
	}
	throw InternalException("unknown FLATTEN list");
}

template <FlattenList LIST>
static unique_ptr<FunctionData> BindFlatten(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	for (auto &argument : arguments) {
		if (argument->return_type.IsUnknown()) {
			// a parameter: bound again once its value's type is known (and a macro's, when it is used)
			throw ParameterNotResolvedException();
		}
	}
	auto data = make_uniq<FlattenData>();
	data->list = LIST;
	data->kind = KindOf(arguments[0]->return_type);
	Value constant;
	if (arguments[0]->IsFoldable() && ExpressionExecutor::TryEvaluateScalar(context, *arguments[0], constant) &&
	    constant.IsNull()) {
		// A constant NULL of any type, which DuckDB folds into the macros' branches: they come out as a NULL
		// of no type would (sf_flatten_values JSON, sf_flatten_strings unbound).
		data->kind = FlattenKind::NULLS;
	}
	const auto mode_type = arguments[1]->return_type.id();
	if (!arguments[1]->IsFoldable() || (mode_type != LogicalTypeId::VARCHAR && mode_type != LogicalTypeId::STRING_LITERAL &&
	                                    mode_type != LogicalTypeId::SQLNULL)) {
		throw BinderException("%s takes MODE as a constant string: 'OBJECT', 'ARRAY' or 'BOTH'", bound_function.name);
	}
	auto mode = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (!mode.IsNull()) {
		data->mode.null = false;
		data->mode.text = mode.ToString();
	}
	bound_function.return_type = ResultType(LIST, data->kind);
	if (LIST == FlattenList::STRINGS && data->kind == FlattenKind::NULLS) {
		// The macro's list_transform of a NULL of no type
		throw BinderException("Invalid LIST argument during lambda function binding!");
	}
	Bound b {context};
	auto e = make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, 0);
	if (LIST == FlattenList::VALUES || LIST == FlattenList::STRINGS) {
		// A list's elements unless MODE is 'OBJECT', a map's values unless it is 'ARRAY'; NULL otherwise.
		if (data->kind == FlattenKind::LIST && data->mode.Not("OBJECT")) {
			data->element = LIST == FlattenList::VALUES ? b.Elem(std::move(e)) : b.ElemString(std::move(e));
		} else if (data->kind == FlattenKind::MAP && data->mode.Not("ARRAY")) {
			data->element = LIST == FlattenList::VALUES
			                    ? b.Elem(std::move(e))
			                    : b.Fn("json_extract_string", b.Elem(std::move(e)), Bound::Text("$"));
		}
	}
	return std::move(data);
}

struct FlattenState : public FunctionLocalState {
	FlattenState(ClientContext &context, const Expression &element) : executor(context, element) {
	}
	ExpressionExecutor executor;
};

static unique_ptr<FunctionLocalState> InitFlatten(ExpressionState &state, const BoundFunctionExpression &,
                                                  FunctionData *bind_data) {
	auto &data = bind_data->Cast<FlattenData>();
	if (!data.element) {
		return nullptr;
	}
	return make_uniq<FlattenState>(state.GetContext(), *data.element);
}

//! list_transform(list, lambda e: element): each row's list of the element expression of its elements, in order;
//! NULL for a NULL list. For a map, of its values.
static void FlattenElements(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<FlattenData>();
	if (!data.element) {
		throw InternalException("%s was not bound to its branch", state.expr.Cast<BoundFunctionExpression>().function.name);
	}
	auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<FlattenState>();
	auto &input = args.data[0];
	const auto count = args.size();
	UnifiedVectorFormat lists;
	input.ToUnifiedFormat(count, lists);
	auto entries = UnifiedVectorFormat::GetData<list_entry_t>(lists);
	auto &elements = data.kind == FlattenKind::MAP ? MapVector::GetValues(input) : ListVector::GetEntry(input);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<list_entry_t>(result);
	auto &validity = FlatVector::Validity(result);
	ListVector::SetListSize(result, 0);
	const auto &element_type = data.element->return_type;

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t pending = 0;
	auto flush = [&]() {
		if (pending == 0) {
			return;
		}
		DataChunk chunk;
		chunk.InitializeEmpty({elements.GetType()});
		chunk.data[0].Slice(elements, sel, pending);
		chunk.SetCardinality(pending);
		Vector mapped(element_type, pending);
		local.executor.ExecuteExpression(chunk, mapped);
		ListVector::Append(result, mapped, pending);
		pending = 0;
		sel.Initialize(STANDARD_VECTOR_SIZE);
	};
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto idx = lists.sel->get_index(i);
		if (!lists.validity.RowIsValid(idx)) {
			validity.SetInvalid(i);
			continue;
		}
		const auto &entry = entries[idx];
		out[i] = list_entry_t(offset, entry.length);
		for (idx_t k = 0; k < entry.length; k++) {
			sel.set_index(pending++, entry.offset + k);
			if (pending == STANDARD_VECTOR_SIZE) {
				flush();
			}
		}
		offset += entry.length;
	}
	flush();
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <FlattenList LIST>
static ScalarFunction FlattenFunction(const string &name) {
	// MODE is ANY so that a parameter of it reaches the bind unresolved, as a macro's does when it is created.
	ScalarFunction f(name, {LogicalType::ANY, LogicalType::ANY}, ResultType(LIST, FlattenKind::OTHER),
	                 FlattenElements, BindFlatten<LIST>, nullptr, nullptr, InitFlatten);
	f.SetBindExpressionCallback(BindBranch);
	// Bound for a NULL input or MODE as the macros bind them, whose lists are not all NULL then (sf_flatten_keys
	// with a NULL MODE gives an object's keys).
	f.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return f;
}

void RegisterFlatten(ExtensionLoader &loader) {
	loader.RegisterFunction(FlattenFunction<FlattenList::VALUES>("igloo_sf_flatten_values"));
	loader.RegisterFunction(FlattenFunction<FlattenList::STRINGS>("igloo_sf_flatten_strings"));
	loader.RegisterFunction(FlattenFunction<FlattenList::KEYS>("igloo_sf_flatten_keys"));
	loader.RegisterFunction(FlattenFunction<FlattenList::INDEXES>("igloo_sf_flatten_indexes"));
}

} // namespace rb
} // namespace duckdb
