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
static uint32_t GetVertexCount(const Geometry &geometry) {
	switch (geometry.Type()) {
	case GeometryType::POINT:
		return geometry.GetPoint().IsEmpty() ? 0U : 1U;
	case GeometryType::LINESTRING:
		return geometry.GetLineString().Count();
	case GeometryType::POLYGON: {
		auto &polygon = geometry.GetPolygon();
		uint32_t count = 0;
		for (auto &ring : polygon.Rings()) {
			count += ring.Count();
		}
		return count;
	}
	case GeometryType::MULTIPOINT: {
		uint32_t count = 0;
		for (auto &point : geometry.GetMultiPoint()) {
			if (!point.IsEmpty()) {
				count++;
			}
		}
		return count;
	}
	case GeometryType::MULTILINESTRING: {
		uint32_t count = 0;
		for (auto &linestring : geometry.GetMultiLineString()) {
			if (!linestring.IsEmpty()) {
				count += linestring.Count();
			}
		}
		return count;
	}
	case GeometryType::MULTIPOLYGON: {
		uint32_t count = 0;
		for (auto &polygon : geometry.GetMultiPolygon()) {
			for (auto &ring : polygon.Rings()) {
				count += ring.Count();
			}
		}
		return count;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		uint32_t count = 0;
		for (auto &geom : geometry.GetGeometryCollection()) {
			count += GetVertexCount(geom);
		}
		return count;
	}
	default:
		throw NotImplementedException("Geometry type not supported");
	}
}
static void GeometryNumPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &ctx = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, uint32_t>(input, result, count, [&](string_t input) {
		auto geometry = ctx.factory.Deserialize(input);
		return GetVertexCount(geometry);
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStNPoints(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

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
		CreateScalarFunctionInfo info(std::move(area_function_set));
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateFunction(context, info);
	}
}

} // namespace core

} // namespace spatial