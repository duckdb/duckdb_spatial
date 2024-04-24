#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/wkt_reader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace spatial {

namespace core {

struct GeometryFromWKTBindData : public FunctionData {
	bool ignore_invalid = false;

	explicit GeometryFromWKTBindData(bool ignore_invalid) : ignore_invalid(ignore_invalid) {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<GeometryFromWKTBindData>(ignore_invalid);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

// TODO: we should implement our own WKT parser asap. This is a temporary and really inefficient solution.
// TODO: Ignore_invalid doesnt make sense here, we should just use a try_cast instead.
static void GeometryFromWKTFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<GeometryFromWKTBindData>();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	WKTReader reader(arena);
	UnaryExecutor::ExecuteWithNulls<string_t, geometry_t>(input, result, count,
	                                                      [&](string_t &wkt, ValidityMask &mask, idx_t idx) {
		                                                      try {
			                                                      auto geom = reader.Parse(wkt);
			                                                      return Geometry::Serialize(geom, result);
		                                                      } catch (InvalidInputException &error) {
			                                                      if (!info.ignore_invalid) {
				                                                      throw;
			                                                      }
			                                                      mask.SetInvalid(idx);
			                                                      return geometry_t {};
		                                                      }
	                                                      });
}

static unique_ptr<FunctionData> GeometryFromWKTBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw InvalidInputException("ST_GeomFromText requires at least one argument");
	}
	auto &input_type = arguments[0]->return_type;
	if (input_type.id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("ST_GeomFromText requires a string argument");
	}

	bool ignore_invalid = false;
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (arg->HasParameter()) {
			throw InvalidInputException("Parameters are not supported in ST_GeomFromText optional arguments");
		}
		if (!arg->IsFoldable()) {
			throw InvalidInputException(
			    "Non-constant arguments are not supported in ST_GeomFromText optional arguments");
		}
		if (arg->alias == "ignore_invalid") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw InvalidInputException("ST_GeomFromText optional argument 'ignore_invalid' must be a boolean");
			}
			ignore_invalid = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		}
	}
	return make_uniq<GeometryFromWKTBindData>(ignore_invalid);
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Deserializes a GEOMETRY from a WKT string, optionally ignoring invalid geometries
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr const DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------

void CoreScalarFunctions::RegisterStGeomFromText(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GeomFromText");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, core::GeoTypes::GEOMETRY(), GeometryFromWKTFunction,
	                               GeometryFromWKTBind, nullptr, nullptr, GeometryFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, core::GeoTypes::GEOMETRY(),
	                               GeometryFromWKTFunction, GeometryFromWKTBind, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));
	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_GeomFromText", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
