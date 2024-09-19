#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------

static void GeometryMultiFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto &input = args.data[0];

	UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, args.size(), [&](const geometry_t &geom_blob) {

		const bool has_z = geom_blob.GetProperties().HasZ();
		const bool has_m = geom_blob.GetProperties().HasM();

		switch(geom_blob.GetType()) {
		case GeometryType::POINT: {
			auto mpoint = MultiPoint::Create(arena, 1, has_z, has_m);
			MultiPoint::Part(mpoint, 0) = Geometry::Deserialize(arena, geom_blob);
			return Geometry::Serialize(mpoint, result);
		}
		case GeometryType::LINESTRING: {
			auto mline = MultiLineString::Create(arena, 1, has_z, has_m);
			MultiLineString::Part(mline, 0) = Geometry::Deserialize(arena, geom_blob);
			return Geometry::Serialize(mline, result);
		}
		case GeometryType::POLYGON: {
			auto mpoly = MultiPolygon::Create(arena, 1, has_z, has_m);
			MultiPolygon::Part(mpoly, 0) = Geometry::Deserialize(arena, geom_blob);
			return Geometry::Serialize(mpoly, result);
		}
		default:
			return geom_blob;
		}
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
	Turns a single geometry into a multi geometry.

	If the geometry is already a multi geometry, it is returned as is.
)";

static constexpr const char *DOC_EXAMPLE = R"(
SELECT ST_Multi(ST_GeomFromText('POINT(1 2)'));
-- MULTIPOINT (1 2)

SELECT ST_Multi(ST_GeomFromText('LINESTRING(1 1, 2 2)'));
-- MULTILINESTRING ((1 1, 2 2))

SELECT ST_Multi(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'));
-- MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)))
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStMulti(DatabaseInstance &db) {
	ScalarFunction function("ST_Multi",{GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), GeometryMultiFunction);
	function.init_local_state = GeometryFunctionLocalState::Init;
	ExtensionUtil::RegisterFunction(db, function);
	DocUtil::AddDocumentation(db, "ST_Multi", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
