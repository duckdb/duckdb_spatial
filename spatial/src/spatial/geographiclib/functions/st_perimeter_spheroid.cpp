#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"
#include "GeographicLib/PolygonArea.hpp"

namespace spatial {

namespace geographiclib {

using namespace core;

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void GeodesicPolygon2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();

	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();
	auto polygon_area = GeographicLib::PolygonArea(geod, false);

	UnaryExecutor::Execute<list_entry_t, double>(input, result, count, [&](list_entry_t polygon) {
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;
		double perimeter = 0;
		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			polygon_area.Clear();
			// Note: the last point is the same as the first point, but geographiclib doesn't know that,
			// so skip it.
			for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
				polygon_area.AddPoint(x_data[coord_idx], y_data[coord_idx]);
			}
			double _ring_area;
			double ring_perimeter;
			polygon_area.Compute(false, true, ring_perimeter, _ring_area);
			perimeter += ring_perimeter;
		}
		return perimeter;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static double PolygonPerimeter(const Geometry &poly, GeographicLib::PolygonArea &comp) {

	double total_perimeter = 0;
	for (auto &ring : Polygon::Parts(poly)) {
		comp.Clear();
		// Note: the last point is the same as the first point, but geographiclib doesn't know that,
		// so skip it.
		for (uint32_t coord_idx = 0; coord_idx < ring.Count() - 1; coord_idx++) {
			auto coord = LineString::GetVertex(ring, coord_idx);
			comp.AddPoint(coord.x, coord.y);
		}
		double _ring_area;
		double perimeter;
		comp.Compute(false, true, perimeter, _ring_area);
		total_perimeter += perimeter;
	}
	return total_perimeter;
}

static void GeodesicGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto &input = args.data[0];
	auto count = args.size();

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();
	auto comp = GeographicLib::PolygonArea(geod, false);

	UnaryExecutor::Execute<geometry_t, double>(input, result, count, [&](geometry_t input) {
		auto geom = Geometry::Deserialize(arena, input);
		auto length = 0.0;
		Geometry::ExtractPolygons(geom, [&](const Geometry &poly) { length += PolygonPerimeter(poly, comp); });
		return length;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the length of the perimeter in meters using an ellipsoidal model of the earths surface

    The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the length is returned in meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library, calculating the perimeter using an ellipsoidal model of the earth. This is a highly accurate method for calculating the perimeter of a polygon taking the curvature of the earth into account, but is also the slowest.

    Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.
)";

static constexpr const char *DOC_EXAMPLE = R"()";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}, {"category", "spheroid"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void GeographicLibFunctions::RegisterPerimeter(DatabaseInstance &db) {

	// Perimiter
	ScalarFunctionSet set("ST_Perimeter_Spheroid");
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, GeodesicPolygon2DFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeodesicGeometryFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));
	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Perimeter_Spheroid", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geographiclib

} // namespace spatial