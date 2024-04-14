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

static void BoundaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::ExecuteWithNulls<geometry_t, geometry_t>(
	    args.data[0], result, args.size(), [&](geometry_t &geometry_blob, ValidityMask &mask, idx_t i) {
		    auto geom = lstate.ctx.Deserialize(geometry_blob);
		    if (GEOSGeomTypeId_r(lstate.ctx.GetCtx(), geom.get()) == GEOS_GEOMETRYCOLLECTION) {
			    mask.SetInvalid(i);
			    return geometry_t {};
		    }

		    auto boundary = make_uniq_geos(lstate.ctx.GetCtx(), GEOSBoundary_r(lstate.ctx.GetCtx(), geom.get()));
		    return lstate.ctx.Serialize(result, boundary);
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
Returns the "boundary" of a geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStBoundary(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Boundary");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), BoundaryFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Boundary", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
