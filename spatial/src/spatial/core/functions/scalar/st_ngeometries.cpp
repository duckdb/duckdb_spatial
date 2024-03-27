#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryNGeometriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &ctx = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, int32_t>(input, result, count, [&](geometry_t input) {
		struct op {
			static int32_t Apply(const CollectionGeometry &collection) {
				return static_cast<int32_t>(collection.Count());
			}
			static int32_t Apply(const Polygon &geom) {
				return geom.IsEmpty() ? 0 : 1;
			}
			static int32_t Apply(const SinglePartGeometry &geom) {
				return geom.IsEmpty() ? 0 : 1;
			}
		};
		return Geometry::Deserialize(ctx.arena, input).Visit<op>();
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the number of component geometries in a collection geometry
    If the input geometry is not a collection, returns 1 if the geometry is not empty, otherwise 0
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStNGeometries(DatabaseInstance &db) {

	const char *aliases[] = {"ST_NGeometries", "ST_NumGeometries"};
	for (auto alias : aliases) {
		ScalarFunctionSet set(alias);
		set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::INTEGER, GeometryNGeometriesFunction,
		                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

		ExtensionUtil::RegisterFunction(db, set);
		DocUtil::AddDocumentation(db, alias, DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
	}
}

} // namespace core

} // namespace spatial