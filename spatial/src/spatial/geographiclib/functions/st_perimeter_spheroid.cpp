#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"
#include "GeographicLib/PolygonArea.hpp"

namespace spatial {

namespace geographiclib {

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

    const GeographicLib::Geodesic& geod = GeographicLib::Geodesic::WGS84();
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
static double PolygonPerimeter(const core::Polygon &poly, GeographicLib::PolygonArea &comp) {
	
	double total_perimeter = 0;	
	for (auto &ring : poly.Rings()) {
		comp.Clear();
		// Note: the last point is the same as the first point, but geographiclib doesn't know that,
		// so skip it.
		for (uint32_t coord_idx = 0; coord_idx < ring.Count() - 1; coord_idx++) {
			auto &coord = ring[coord_idx];
			comp.AddPoint(coord.x, coord.y);
		}
		double _ring_area;
		double perimeter;
		comp.Compute(false, true, perimeter, _ring_area);
		total_perimeter += perimeter;
	}
	return total_perimeter;
}

static double GeometryPerimeter(const core::Geometry &geom, GeographicLib::PolygonArea &comp) {
	switch (geom.Type()) {
		case core::GeometryType::POLYGON: {
			auto &poly = geom.GetPolygon();
			return PolygonPerimeter(poly, comp);
		}
		case core::GeometryType::MULTIPOLYGON: {
			auto &mpoly = geom.GetMultiPolygon();
			double total_perimeter = 0;
			for (auto &poly : mpoly) {
				total_perimeter += PolygonPerimeter(poly, comp);
			}
			return total_perimeter;
		}
		case core::GeometryType::GEOMETRYCOLLECTION: {
			auto &coll = geom.GetGeometryCollection();
			double total_perimeter = 0;
			for (auto &item : coll) {
				total_perimeter += GeometryPerimeter(item, comp);
			}
			return total_perimeter;
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
	auto comp = GeographicLib::PolygonArea(geod, false);

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		return GeometryPerimeter(geometry, comp);
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}


void GeographicLibFunctions::RegisterPerimeter(ClientContext &context) {
    auto &catalog = Catalog::GetSystemCatalog(context);

    // Perimiter
	ScalarFunctionSet set("st_perimeter_spheroid");
	set.AddFunction(ScalarFunction({spatial::core::GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, GeodesicPolygon2DFunction));
	set.AddFunction(ScalarFunction({spatial::core::GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeodesicGeometryFunction, nullptr, nullptr, nullptr, spatial::core::GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geographiclib

} // namespace spatial