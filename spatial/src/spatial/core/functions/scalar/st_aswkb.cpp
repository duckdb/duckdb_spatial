#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
void GeometryAsWBKFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, string_t>(input, result, count,
	                                             [&](geometry_t input) { return WKBWriter::Write(input, result); });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the geometry as a WKB blob
)";

static constexpr const char *DOC_EXAMPLE = R"(
SELECT ST_AsWKB('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsWKB(DatabaseInstance &db) {
	ScalarFunctionSet as_wkb_function_set("ST_AsWKB");

	as_wkb_function_set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::WKB_BLOB(), GeometryAsWBKFunction));

	ExtensionUtil::RegisterFunction(db, as_wkb_function_set);
	DocUtil::AddDocumentation(db, "ST_AsWKB", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial