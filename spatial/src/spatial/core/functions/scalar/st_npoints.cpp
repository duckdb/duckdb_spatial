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
// POINT_2D
//------------------------------------------------------------------------------
static void PointNumPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using COUNT_TYPE = PrimitiveType<idx_t>;

	GenericExecutor::ExecuteUnary<POINT_TYPE, COUNT_TYPE>(args.data[0], result, args.size(),
	                                                      [](POINT_TYPE) { return 1; });
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineStringNumPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	UnaryExecutor::Execute<list_entry_t, idx_t>(input, result, args.size(),
	                                            [](list_entry_t input) { return input.length; });
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonNumPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();
	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);

	UnaryExecutor::Execute<list_entry_t, idx_t>(input, result, count, [&](list_entry_t polygon) {
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;
		idx_t npoints = 0;
		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			npoints += ring.length;
		}
		return npoints;
	});
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void BoxNumPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using COUNT_TYPE = PrimitiveType<idx_t>;

	GenericExecutor::ExecuteUnary<BOX_TYPE, COUNT_TYPE>(args.data[0], result, args.size(), [](BOX_TYPE) { return 4; });
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------

static void GeometryNumPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto &input = args.data[0];
	auto count = args.size();

	struct op {
		static uint32_t Case(Geometry::Tags::SinglePartGeometry, const Geometry &geom) {
			return geom.Count();
		}
		static uint32_t Case(Geometry::Tags::MultiPartGeometry, const Geometry &geom) {
			uint32_t count = 0;
			for (uint32_t i = 0; i < MultiPartGeometry::PartCount(geom); i++) {
				auto part = MultiPartGeometry::Part(geom, i);
				count += Geometry::Match<op>(part);
			}
			return count;
		}
	};

	UnaryExecutor::Execute<geometry_t, uint32_t>(input, result, count, [&](geometry_t input) {
		auto geom = Geometry::Deserialize(arena, input);
		return Geometry::Match<op>(geom);
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the number of vertices within a geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStNPoints(DatabaseInstance &db) {
	const char *aliases[] = {"ST_NPoints", "ST_NumPoints"};
	for (auto alias : aliases) {
		ScalarFunctionSet area_function_set(alias);
		area_function_set.AddFunction(
		    ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::UBIGINT, PointNumPointsFunction));
		area_function_set.AddFunction(
		    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::UBIGINT, LineStringNumPointsFunction));
		area_function_set.AddFunction(
		    ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::UBIGINT, PolygonNumPointsFunction));
		area_function_set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::UBIGINT, BoxNumPointsFunction));
		area_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::UINTEGER,
		                                             GeometryNumPointsFunction, nullptr, nullptr, nullptr,
		                                             GeometryFunctionLocalState::Init));

		ExtensionUtil::RegisterFunction(db, area_function_set);
		DocUtil::AddDocumentation(db, alias, DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
	}
}

} // namespace core

} // namespace spatial