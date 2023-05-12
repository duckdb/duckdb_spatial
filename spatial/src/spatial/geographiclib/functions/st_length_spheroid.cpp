#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"
#include "GeographicLib/PolygonArea.hpp"

namespace spatial {

namespace geographiclib {

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

    const GeographicLib::Geodesic& geod = GeographicLib::Geodesic::WGS84();
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
static double LineLength(const core::LineString &line, GeographicLib::PolygonArea &comp) {
		comp.Clear();
		for (auto &vert : line.Vertices()) {
			comp.AddPoint(vert.x, vert.y);
		}
		double _area;
		double linestring_length;
		comp.Compute(false, true, linestring_length, _area);
		return linestring_length;
}

static double GeometryLength(const core::Geometry &geom, GeographicLib::PolygonArea &comp) {
	switch(geom.Type()) {
		case core::GeometryType::LINESTRING: {
			return LineLength(geom.GetLineString(), comp);
		}
		case core::GeometryType::MULTILINESTRING: {
			auto &mline = geom.GetMultiLineString();
			double mline_length = 0;
			for (auto &line : mline) {
				mline_length += LineLength(line, comp);
			}
			return mline_length;
		}
		case core::GeometryType::GEOMETRYCOLLECTION: {
			auto &coll = geom.GetGeometryCollection();
			auto sum = 0;
			for (auto &item : coll) {
				sum += GeometryLength(item, comp);
			}
			return sum;
		}
		default: {
			return 0.0;
		}
	}
}

static void GeodesicGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = core::GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	const GeographicLib::Geodesic& geod = GeographicLib::Geodesic::WGS84();
	auto comp = GeographicLib::PolygonArea(geod, true);

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		return GeometryLength(geometry, comp);
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void GeographicLibFunctions::RegisterLength(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

    // Length
    ScalarFunctionSet set("st_length_spheroid");
    set.AddFunction(ScalarFunction({spatial::core::GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, GeodesicLineString2DFunction));
	set.AddFunction(ScalarFunction({spatial::core::GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeodesicGeometryFunction, nullptr, nullptr, nullptr, spatial::core::GeometryFunctionLocalState::Init));

    CreateScalarFunctionInfo info(std::move(set));
    info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(context, info);
}

} // namespace geographiclib

} // namespace spatial