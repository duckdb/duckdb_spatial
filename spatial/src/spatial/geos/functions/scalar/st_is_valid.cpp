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

static bool IsValidForGeos(Geometry &geometry) {
	switch (geometry.GetType()) {
	case GeometryType::LINESTRING:
		// Every linestring needs 0 or at least 2 points
		return LineString::VertexCount(geometry) != 1;

	case GeometryType::POLYGON: {
		// Every ring needs 0 or at least 4 points
		for (uint32_t i = 0; i < Polygon::PartCount(geometry); i++) {
			auto &ring = Polygon::Part(geometry, i);
			if (LineString::VertexCount(ring) < 4) {
				return false;
			}
		}
		return true;
	}
	case GeometryType::MULTILINESTRING: {
		for (uint32_t i = 0; i < MultiLineString::PartCount(geometry); i++) {
			auto &linestring = MultiLineString::Part(geometry, i);
			if (LineString::VertexCount(linestring) == 1) {
				return false;
			}
		}
		return true;
	}
	case GeometryType::MULTIPOLYGON: {
		for (uint32_t i = 0; i < MultiPolygon::PartCount(geometry); i++) {
			auto &polygon = MultiPolygon::Part(geometry, i);
			for (uint32_t j = 0; j < Polygon::PartCount(polygon); j++) {
				auto &ring = Polygon::Part(polygon, j);
				if (LineString::VertexCount(ring) < 4) {
					return false;
				}
			}
		}
		return true;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		for (uint32_t i = 0; i < GeometryCollection::PartCount(geometry); i++) {
			auto &geom = GeometryCollection::Part(geometry, i);
			if (!IsValidForGeos(geom)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

static void IsValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	UnaryExecutor::Execute<geometry_t, bool>(args.data[0], result, args.size(), [&](geometry_t input) {
		auto geom = Geometry::Deserialize(arena, input);

		// double check before calling into geos
		if (!IsValidForGeos(geom)) {
			return false;
		}

		auto geos_geom = lstate.ctx.Deserialize(input);
		return (bool)GEOSisValid_r(lstate.ctx.GetCtx(), geos_geom.get());
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns true if the geometry is topologically "valid"
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStIsValid(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_IsValid");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN, IsValidFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_IsValid", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
