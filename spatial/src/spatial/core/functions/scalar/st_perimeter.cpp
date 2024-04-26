#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
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
static void GeometryPerimeterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = core::GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, double>(input, result, count, [&](geometry_t input) {
		auto geom = Geometry::Deserialize(arena, input);
		double perimeter = 0.0;
		Geometry::ExtractPolygons(geom, [&](const Geometry &poly) {
			for (auto &p : Polygon::Parts(poly)) {
				perimeter += LineString::Length(p);
			}
		});
		return perimeter;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the length of the perimeter of the geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStPerimeter(DatabaseInstance &db) {

	// Perimiter
	ScalarFunctionSet set("ST_Perimeter");
	set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DPerimeterFunction));
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DPerimeterFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryPerimeterFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Perimeter", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial