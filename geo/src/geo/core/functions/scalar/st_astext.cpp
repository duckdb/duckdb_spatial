#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "geo/common.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/functions/common.hpp"
#include "geo/core/types.hpp"
namespace geo {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void Point2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	using POINT_TYPE = StructTypeBinary<double, double>;
	using TEXT_TYPE = PrimitiveType<string_t>;

	GenericExecutor::ExecuteUnary<POINT_TYPE, TEXT_TYPE>(input, result, count, [&](POINT_TYPE &point) {
		auto x = point.a_val;
		auto y = point.b_val;
		return StringVector::AddString(result, StringUtil::Format("POINT(%f %f)", x, y));
	});
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineString2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &inner = ListVector::GetEntry(input);
	auto &children = StructVector::GetEntries(inner);
	auto x_data = FlatVector::GetData<double>(*children[0]);
	auto y_data = FlatVector::GetData<double>(*children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(input, result, count, [&](list_entry_t &line) {
		auto offset = line.offset;
		auto length = line.length;

		string result_str = "LINESTRING(";
		for (idx_t i = offset; i < offset + length; i++) {
			result_str += StringUtil::Format("%f %f", x_data[i], y_data[i]);
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void Polygon2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &poly_vector = args.data[0];
	auto &ring_vector = ListVector::GetEntry(poly_vector);
	auto ring_entries = ListVector::GetData(ring_vector);
	auto &point_vector = ListVector::GetEntry(ring_vector);
	auto &point_children = StructVector::GetEntries(point_vector);
	auto x_data = FlatVector::GetData<double>(*point_children[0]);
	auto y_data = FlatVector::GetData<double>(*point_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(poly_vector, result, count, [&](list_entry_t polygon_entry) {
		auto offset = polygon_entry.offset;
		auto length = polygon_entry.length;

		string result_str = "POLYGON(";
		for (idx_t i = offset; i < offset + length; i++) {
			auto ring_entry = ring_entries[i];
			auto ring_offset = ring_entry.offset;
			auto ring_length = ring_entry.length;
			result_str += "(";
			for (idx_t j = ring_offset; j < ring_offset + ring_length; j++) {
				result_str += StringUtil::Format("%f %f", x_data[j], y_data[j]);
				if (j < ring_offset + ring_length - 1) {
					result_str += ", ";
				}
			}
			result_str += ")";
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
void GeometryAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::POINT:
			return StringVector::AddString(result, geometry.GetPoint().ToString());
		case GeometryType::LINESTRING:
			return StringVector::AddString(result, geometry.GetLineString().ToString());
		case GeometryType::POLYGON:
			return StringVector::AddString(result, geometry.GetPolygon().ToString());
		case GeometryType::MULTIPOINT:
			return StringVector::AddString(result, geometry.GetMultiPoint().ToString());
		case GeometryType::MULTILINESTRING:
			return StringVector::AddString(result, geometry.GetMultiLineString().ToString());
		case GeometryType::MULTIPOLYGON:
			return StringVector::AddString(result, geometry.GetMultiPolygon().ToString());
		case GeometryType::GEOMETRYCOLLECTION:
			return StringVector::AddString(result, geometry.GetGeometryCollection().ToString());
		default:
			throw NotImplementedException("Geometry type not implemented");
		}
	});
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsText(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet as_text_function_set("st_astext");

	as_text_function_set.AddFunction(ScalarFunction({GeoTypes::POINT_2D}, LogicalType::VARCHAR, Point2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D}, LogicalType::VARCHAR, LineString2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D}, LogicalType::VARCHAR, Polygon2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY}, LogicalType::VARCHAR, GeometryAsTextFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(as_text_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace core

} // namespace geo