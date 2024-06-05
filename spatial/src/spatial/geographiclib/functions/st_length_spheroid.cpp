#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"
#include "GeographicLib/PolygonArea.hpp"

namespace spatial {

namespace geographiclib {

using namespace core;

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void GeodesicLineString2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &line_vec = args.data[0];
	auto count = args.size();

	auto &coord_vec = ListVector::GetEntry(line_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();
	auto polygon_area = GeographicLib::PolygonArea(geod, true);

	UnaryExecutor::Execute<list_entry_t, double>(line_vec, result, count, [&](list_entry_t line) {
		polygon_area.Clear();
		auto offset = line.offset;
		auto length = line.length;
		// Loop over the segments
		for (idx_t j = offset; j < offset + length; j++) {
			auto x = x_data[j];
			auto y = y_data[j];
			polygon_area.AddPoint(x, y);
		}
		double _area;
		double linestring_length;
		polygon_area.Compute(false, true, linestring_length, _area);
		return linestring_length;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static double LineLength(const Geometry &line, GeographicLib::PolygonArea &comp) {
	comp.Clear();
	for (uint32_t i = 0; i < LineString::VertexCount(line); i++) {
		auto vert = LineString::GetVertex(line, i);
		comp.AddPoint(vert.x, vert.y);
	}
	double _area;
	double linestring_length;
	comp.Compute(false, true, linestring_length, _area);
	return linestring_length;
}

static void GeodesicGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto &input = args.data[0];
	auto count = args.size();

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();
	auto comp = GeographicLib::PolygonArea(geod, true);

	UnaryExecutor::Execute<geometry_t, double>(input, result, count, [&](geometry_t input) {
		auto geom = Geometry::Deserialize(arena, input);
		double length = 0.0;
		Geometry::ExtractLines(geom, [&](const Geometry &line) { length += LineLength(line, comp); });
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
Returns the length of the input geometry in meters, using a ellipsoidal model of the earth

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the length is returned in square meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library, calculating the length using an ellipsoidal model of the earth. This is a highly accurate method for calculating the length of a line geometry taking the curvature of the earth into account, but is also the slowest.

Returns `0.0` for any geometry that is not a `LINESTRING`, `MULTILINESTRING` or `GEOMETRYCOLLECTION` containing line geometries.
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}, {"category", "spheroid"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void GeographicLibFunctions::RegisterLength(DatabaseInstance &db) {

	// Length
	ScalarFunctionSet set("ST_Length_Spheroid");
	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, GeodesicLineString2DFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeodesicGeometryFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Length_Spheroid", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geographiclib

} // namespace spatial