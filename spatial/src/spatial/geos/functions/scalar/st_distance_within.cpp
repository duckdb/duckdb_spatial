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

using namespace core;

static void ExecutePreparedDistanceWithin(GEOSFunctionLocalState &lstate, Vector &left, Vector &right,
                                          Vector &distance_vec, idx_t count, Vector &result) {
	auto &ctx = lstate.ctx.GetCtx();

	// Optimize: if one of the arguments is a constant, we can prepare it once and reuse it
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &left_blob = FlatVector::GetData<geometry_t>(left)[0];
		auto left_geom = lstate.ctx.Deserialize(left_blob);
		auto left_prepared = make_uniq_geos(ctx, GEOSPrepare_r(ctx, left_geom.get()));

		BinaryExecutor::Execute<geometry_t, double, bool>(
		    right, distance_vec, result, count, [&](geometry_t &right_blob, double distance) {
			    auto right_geometry = lstate.ctx.Deserialize(right_blob);
			    auto ok = GEOSPreparedDistanceWithin_r(ctx, left_prepared.get(), right_geometry.get(), distance);
			    return ok == 1;
		    });
	} else if (right.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	           left.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &right_blob = FlatVector::GetData<geometry_t>(right)[0];
		auto right_geom = lstate.ctx.Deserialize(right_blob);
		auto right_prepared = make_uniq_geos(ctx, GEOSPrepare_r(ctx, right_geom.get()));

		BinaryExecutor::Execute<geometry_t, double, bool>(
		    left, distance_vec, result, count, [&](geometry_t &left_blob, double distance) {
			    auto left_geometry = lstate.ctx.Deserialize(left_blob);
			    auto ok = GEOSPreparedDistanceWithin_r(ctx, right_prepared.get(), left_geometry.get(), distance);
			    return ok == 1;
		    });
	} else {
		TernaryExecutor::Execute<geometry_t, geometry_t, double, bool>(
		    left, right, distance_vec, result, count,
		    [&](geometry_t &left_blob, geometry_t &right_blob, double distance) {
			    auto left_geometry = lstate.ctx.Deserialize(left_blob);
			    auto right_geometry = lstate.ctx.Deserialize(right_blob);
			    auto ok = GEOSDistanceWithin_r(ctx, left_geometry.get(), right_geometry.get(), distance);
			    return ok == 1;
		    });
	}
}

static void DistanceWithinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto &distance_vec = args.data[2];
	auto count = args.size();
	ExecutePreparedDistanceWithin(lstate, left, right, distance_vec, count, result);
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns if two geometries are within a target distance of each-other
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "relation"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStDistanceWithin(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_DWithin");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY(), LogicalType::DOUBLE},
	                               LogicalType::BOOLEAN, DistanceWithinFunction, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_DWithin", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
