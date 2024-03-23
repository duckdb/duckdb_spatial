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

static void ExecuteContainsProperlyPrepared(GEOSFunctionLocalState &lstate, Vector &left, Vector &right, idx_t count,
                                            Vector &result) {
	auto &ctx = lstate.ctx.GetCtx();

	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		auto &left_blob = FlatVector::GetData<geometry_t>(left)[0];
		auto left_geom = lstate.ctx.Deserialize(left_blob);
		auto left_prepared = make_uniq_geos(ctx, GEOSPrepare_r(ctx, left_geom.get()));

		UnaryExecutor::Execute<geometry_t, bool>(right, result, count, [&](geometry_t &right_blob) {
			auto right_geometry = lstate.ctx.Deserialize(right_blob);
			auto ok = GEOSPreparedContainsProperly_r(ctx, left_prepared.get(), right_geometry.get());
			return ok == 1;
		});
	} else {
		// ContainsProperly only has a prepared version, so we just prepare the left one always
		BinaryExecutor::Execute<geometry_t, geometry_t, bool>(
		    left, right, result, count, [&](geometry_t &left_blob, geometry_t &right_blob) {
			    auto left_geometry = lstate.ctx.Deserialize(left_blob);
			    auto right_geometry = lstate.ctx.Deserialize(right_blob);

			    auto left_prepared = make_uniq_geos(ctx, GEOSPrepare_r(ctx, left_geometry.get()));

			    auto ok = GEOSPreparedContainsProperly_r(ctx, left_prepared.get(), right_geometry.get());
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

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns true if geom1 "properly contains" geom2
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "relation"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStContainsProperly(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_ContainsProperly");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN,
	                               ContainsProperlyFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_ContainsProperly", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial

/*

ST_Within(
ST_GeomFromText('POLYGON((339 346, 459 346, 399 311, 340 277, 399 173, 280 242, 339 415, 280 381, 460 207, 339 346))'),
ST_GeomFromText('POLYGON((339 207, 280 311, 460 138, 399 242, 459 277, 459 415, 399 381, 519 311, 520 242, 519 173, 399
450, 339 207))'));
*/