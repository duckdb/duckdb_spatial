#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

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
static void GeometryFromWKTFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (GeometryFromWKTBindData &)*func_expr.bind_info;

	auto ctx = GeosContextWrapper();

	auto reader = ctx.CreateWKTReader();

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    input, result, count, [&](string_t &wkt, ValidityMask &mask, idx_t idx) {
		    auto geos_geom = reader.Read(wkt);
		    if (geos_geom.get() == nullptr) {
			    if (!info.ignore_invalid) {
				    throw InvalidInputException("Invalid WKT string");
			    } else {
				    mask.SetInvalid(idx);
				    return string_t();
			    }
		    }
		    auto multidimensional = (GEOSHasZ_r(lstate.ctx.GetCtx(), geos_geom.get()) == 1);
		    if (multidimensional) {
			    throw InvalidInputException("3D/4D geometries are not supported");
		    }
		    return lstate.ctx.Serialize(result, geos_geom);
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

void GEOSScalarFunctions::RegisterStGeomFromText(DatabaseInstance &instance) {
	ScalarFunctionSet set("ST_GeomFromText");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, core::GeoTypes::GEOMETRY(), GeometryFromWKTFunction,
	                               GeometryFromWKTBind, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, core::GeoTypes::GEOMETRY(),
	                               GeometryFromWKTFunction, GeometryFromWKTBind, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));
	ExtensionUtil::RegisterFunction(instance, set);
}

} // namespace geos

} // namespace spatial
