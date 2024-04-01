#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void ExecutePreparedDistance(GEOSFunctionLocalState &lstate, Vector &left, Vector &right, idx_t count,
                                    Vector &result) {
	auto &ctx = lstate.ctx.GetCtx();

	// Optimize: if one of the arguments is a constant, we can prepare it once and reuse it
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &left_blob = FlatVector::GetData<geometry_t>(left)[0];
		auto left_geom = lstate.ctx.Deserialize(left_blob);
		auto left_prepared = make_uniq_geos(ctx, GEOSPrepare_r(ctx, left_geom.get()));

		UnaryExecutor::Execute<geometry_t, double>(right, result, count, [&](geometry_t &right_blob) {
			auto right_geometry = lstate.ctx.Deserialize(right_blob);
			double distance;
			GEOSPreparedDistance_r(ctx, left_prepared.get(), right_geometry.get(), &distance);
			return distance;
		});
	} else if (right.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	           left.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &right_blob = FlatVector::GetData<geometry_t>(right)[0];
		auto right_geom = lstate.ctx.Deserialize(right_blob);
		auto right_prepared = make_uniq_geos(ctx, GEOSPrepare_r(ctx, right_geom.get()));

		UnaryExecutor::Execute<geometry_t, double>(left, result, count, [&](geometry_t &left_blob) {
			auto left_geometry = lstate.ctx.Deserialize(left_blob);
			double distance;
			GEOSPreparedDistance_r(ctx, right_prepared.get(), left_geometry.get(), &distance);
			return distance;
		});
	} else {
		BinaryExecutor::Execute<geometry_t, geometry_t, double>(
		    left, right, result, count, [&](geometry_t &left_blob, geometry_t &right_blob) {
			    auto left_geometry = lstate.ctx.Deserialize(left_blob);
			    auto right_geometry = lstate.ctx.Deserialize(right_blob);
			    double distance;
			    GEOSDistance_r(ctx, left_geometry.get(), right_geometry.get(), &distance);
			    return distance;
		    });
	}
}

static void DistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto count = args.size();
	ExecutePreparedDistance(lstate, left, right, count, result);
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the distance between two geometries.
)";

static constexpr const char *DOC_EXAMPLE = R"(
select st_distance('POINT(0 0)'::geometry, 'POINT(1 1)'::geometry);
----
1.4142135623731
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStDistance(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Distance");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, DistanceFunction,
	                               nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::AddFunctionOverload(db, set);
	DocUtil::AddDocumentation(db, "ST_Distance", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
