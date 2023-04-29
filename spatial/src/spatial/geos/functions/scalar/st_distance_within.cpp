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

static void ExecutePreparedDistanceWithin(GEOSFunctionLocalState &lstate, Vector &left, Vector &right, Vector &distance_vec, idx_t count, Vector &result) {
	auto &ctx = lstate.ctx.GetCtx();

	// Optimize: if one of the arguments is a constant, we can prepare it once and reuse it
	if(left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &left_blob = FlatVector::GetData<string_t>(left)[0];
		auto left_geom = lstate.factory.Deserialize(left_blob);
		auto left_geos = lstate.ctx.FromGeometry(left_geom);
		auto left_prepared_geos = left_geos.Prepare();

		BinaryExecutor::Execute<string_t, double, bool>(right, distance_vec, result, count, [&](string_t &right_blob, double distance) {
			auto right_geometry = lstate.factory.Deserialize(right_blob);
			auto geos_right = lstate.ctx.FromGeometry(right_geometry);
			auto ok = GEOSPreparedDistanceWithin_r(ctx, left_prepared_geos.get(), geos_right.get(), distance);
			return ok == 1;
		});
	} else if(right.GetVectorType() == VectorType::CONSTANT_VECTOR && left.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &right_blob = FlatVector::GetData<string_t>(right)[0];
		auto right_geom = lstate.factory.Deserialize(right_blob);
		auto right_geos = lstate.ctx.FromGeometry(right_geom);
		auto right_prepared_geos = right_geos.Prepare();

		BinaryExecutor::Execute<string_t, double, bool>(left, distance_vec, result, count, [&](string_t &left_blob, double distance) {
			auto left_geometry = lstate.factory.Deserialize(left_blob);
			auto geos_left = lstate.ctx.FromGeometry(left_geometry);
			auto ok = GEOSPreparedDistanceWithin_r(ctx, right_prepared_geos.get(), geos_left.get(), distance);
			return ok == 1;
		});
	} else {
		TernaryExecutor::Execute<string_t, string_t, double, bool>(left, right, distance_vec, result, count, [&](string_t &left_blob, string_t &right_blob, double distance) {
			auto left_geometry = lstate.factory.Deserialize(left_blob);
			auto right_geometry = lstate.factory.Deserialize(right_blob);
			auto geos_left = lstate.ctx.FromGeometry(left_geometry);
			auto geos_right = lstate.ctx.FromGeometry(right_geometry);
			auto ok = GEOSDistanceWithin_r(ctx, geos_left.get(), geos_right.get(), distance);
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

void GEOSScalarFunctions::RegisterStDistanceWithin(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_DWithin");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, LogicalType::BOOLEAN, DistanceWithinFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace spatials

} // namespace spatial
