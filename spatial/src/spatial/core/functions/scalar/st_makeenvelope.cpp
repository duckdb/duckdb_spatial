#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace spatial {

namespace core {

static void MakeEnvelopeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();

	auto &min_x_vec = args.data[0];
	auto &min_y_vec = args.data[1];
	auto &max_x_vec = args.data[2];
	auto &max_y_vec = args.data[3];

	using DOUBLE_TYPE = PrimitiveType<double>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;

	GenericExecutor::ExecuteQuaternary<DOUBLE_TYPE, DOUBLE_TYPE, DOUBLE_TYPE, DOUBLE_TYPE, GEOMETRY_TYPE>(
	    min_x_vec, min_y_vec, max_x_vec, max_y_vec, result, count,
	    [&](DOUBLE_TYPE x_min, DOUBLE_TYPE y_min, DOUBLE_TYPE x_max, DOUBLE_TYPE y_max) {
		    uint32_t capacity = 5;
		    auto envelope_geom = lstate.factory.CreatePolygon(1, &capacity, false, false);
		    auto &shell = envelope_geom[0];
		    // Create the exterior ring in CCW order
		    shell.AppendUnsafe({x_min.val, y_min.val});
		    shell.AppendUnsafe({x_min.val, y_max.val});
		    shell.AppendUnsafe({x_max.val, y_max.val});
		    shell.AppendUnsafe({x_max.val, y_min.val});
		    shell.AppendUnsafe({x_min.val, y_min.val});

		    return lstate.factory.Serialize(result, envelope_geom, false, false);
	    });
}

void CoreScalarFunctions::RegisterStMakeEnvelope(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_MakeEnvelope");

	set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               GeoTypes::GEOMETRY(), MakeEnvelopeFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
