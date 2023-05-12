#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/functions/scalar.hpp"


namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void Polygon2DPerimeterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();

	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, double>(input, result, count, [&](list_entry_t polygon) {
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;
		double perimeter = 0;
		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

            for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
                auto x1 = x_data[coord_idx];
                auto y1 = y_data[coord_idx];
                auto x2 = x_data[coord_idx + 1];
                auto y2 = y_data[coord_idx + 1];
                perimeter += std::sqrt(std::pow(x1 - x2, 2) + std::pow(y1 - y2, 2));
            }
		}
		return perimeter;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void Box2DPerimeterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using PERIMETER_TYPE = PrimitiveType<double>;

	GenericExecutor::ExecuteUnary<BOX_TYPE, PERIMETER_TYPE>(args.data[0], result, args.size(), [&](BOX_TYPE &box) {
		auto minx = box.a_val;
		auto miny = box.b_val;
		auto maxx = box.c_val;
		auto maxy = box.d_val;
        return 2 * (maxx - minx + maxy - miny);
	});
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static double PolygonPerimeter(const Polygon &poly) {
	double perimeter = 0;
	for (auto &ring : poly.Rings()) {
		for (uint32_t i = 0; i < ring.Count() - 1; i++) {
			auto &v1 = ring[i];
			auto &v2 = ring[i + 1];
			perimeter += std::sqrt(std::pow(v1.x - v2.x, 2) + std::pow(v1.y - v2.y, 2));
		}
	}
	return perimeter;
}

static double GeometryPerimeter(const Geometry &geom) {
	switch (geom.Type()) {
		case core::GeometryType::POLYGON: {
			auto &poly = geom.GetPolygon();
			return PolygonPerimeter(poly);
		}
		case core::GeometryType::MULTIPOLYGON: {
			auto &mpoly = geom.GetMultiPolygon();
			double total_perimeter = 0;
			for (auto &poly : mpoly) {
				total_perimeter += PolygonPerimeter(poly);
			}
			return total_perimeter;
		}
		case core::GeometryType::GEOMETRYCOLLECTION: {
			auto &coll = geom.GetGeometryCollection();
			double total_perimeter = 0;
			for (auto &subgeom : coll) {
				total_perimeter += GeometryPerimeter(subgeom);
			}
			return total_perimeter;
		}
		default: {
			return 0.0;
		}
	}
}

static void GeometryPerimeterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = core::GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		return GeometryPerimeter(geometry);
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}


void CoreScalarFunctions::RegisterStPerimeter(ClientContext &context) {
    auto &catalog = Catalog::GetSystemCatalog(context);

    // Perimiter
	ScalarFunctionSet set("st_perimeter");
    set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DPerimeterFunction));
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DPerimeterFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryPerimeterFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geographiclib

} // namespace spatial