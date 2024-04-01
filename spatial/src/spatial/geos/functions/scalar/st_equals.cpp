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

static void EqualsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	BinaryExecutor::Execute<geometry_t, geometry_t, bool>(args.data[0], args.data[1], result, args.size(),
	                                                      [&](geometry_t &left_blob, geometry_t &right_blob) {
		                                                      auto left = lstate.ctx.Deserialize(left_blob);
		                                                      auto right = lstate.ctx.Deserialize(right_blob);
		                                                      return GEOSEquals_r(ctx, left.get(), right.get());
	                                                      });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Compares two geometries for equality
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------

void GEOSScalarFunctions::RegisterStEquals(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Equals");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN, EqualsFunction,
	                               nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Equals", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
