#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"
#include "spatial/geos/geos_executor.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void ExecuteContainsProperlyPrepared(GEOSFunctionLocalState &lstate, Vector &left, Vector &right, idx_t count, Vector &result) {
    auto &ctx = lstate.ctx.GetCtx();

    if(left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
        auto &left_blob = FlatVector::GetData<string_t>(left)[0];
        auto left_geom = lstate.factory.Deserialize(left_blob);
        auto left_geos = lstate.ctx.FromGeometry(left_geom);
        auto left_prepared_geos = left_geos.Prepare();

        UnaryExecutor::Execute<string_t, bool>(right, result, count, [&](string_t &right_blob) {
            auto right_geometry = lstate.factory.Deserialize(right_blob);
            auto geos_right = lstate.ctx.FromGeometry(right_geometry);
            auto ok = GEOSPreparedContainsProperly_r(ctx, left_prepared_geos.get(), geos_right.get());
            return ok == 1;
        });
    } else {
        // ContainsProperly only has a prepared version, so we just prepare the left one always
        BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_blob, string_t &right_blob) {
            auto left_geometry = lstate.factory.Deserialize(left_blob);
            auto right_geometry = lstate.factory.Deserialize(right_blob);
            auto geos_left = lstate.ctx.FromGeometry(left_geometry);
            auto geos_right = lstate.ctx.FromGeometry(right_geometry);
            auto left_prepared = geos_left.Prepare();

            auto ok = GEOSPreparedContainsProperly_r(ctx, left_prepared.get(), geos_right.get());
            return ok == 1;
        });
    }
}

static void ContainsProperlyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto count = args.size();
    ExecuteContainsProperlyPrepared(lstate, left, right, count, result);
}

void GEOSScalarFunctions::RegisterStContainsProperly(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_ContainsProperly");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN, ContainsProperlyFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace spatials

} // namespace spatial

/*

ST_Within(
ST_GeomFromText('POLYGON((339 346, 459 346, 399 311, 340 277, 399 173, 280 242, 339 415, 280 381, 460 207, 339 346))'),
ST_GeomFromText('POLYGON((339 207, 280 311, 460 138, 399 242, 459 277, 459 415, 399 381, 519 311, 520 242, 519 173, 399 450, 339 207))'));
*/