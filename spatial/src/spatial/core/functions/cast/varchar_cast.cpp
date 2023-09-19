#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Point2DToVarchar(Vector &source, Vector &result, idx_t count) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;

	GenericExecutor::ExecuteUnary<POINT_TYPE, VARCHAR_TYPE>(source, result, count, [&](POINT_TYPE &point) {
		auto x = point.a_val;
		auto y = point.b_val;
		return StringVector::AddString(result, StringUtil::Format("POINT (%s)", Utils::format_coord(x, y)));
	});
}

//------------------------------------------------------------------------------
// LINESTRING_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::LineString2DToVarchar(Vector &source, Vector &result, idx_t count) {
	auto &inner = ListVector::GetEntry(source);
	auto &children = StructVector::GetEntries(inner);
	auto x_data = FlatVector::GetData<double>(*children[0]);
	auto y_data = FlatVector::GetData<double>(*children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &line) {
		auto offset = line.offset;
		auto length = line.length;

		string result_str = "LINESTRING (";
		for (idx_t i = offset; i < offset + length; i++) {
			result_str += Utils::format_coord(x_data[i], y_data[i]);
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// POLYGON_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Polygon2DToVarchar(Vector &source, Vector &result, idx_t count) {
	auto &poly_vector = source;
	auto &ring_vector = ListVector::GetEntry(poly_vector);
	auto ring_entries = ListVector::GetData(ring_vector);
	auto &point_vector = ListVector::GetEntry(ring_vector);
	auto &point_children = StructVector::GetEntries(point_vector);
	auto x_data = FlatVector::GetData<double>(*point_children[0]);
	auto y_data = FlatVector::GetData<double>(*point_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(poly_vector, result, count, [&](list_entry_t polygon_entry) {
		auto offset = polygon_entry.offset;
		auto length = polygon_entry.length;

		string result_str = "POLYGON (";
		for (idx_t i = offset; i < offset + length; i++) {
			auto ring_entry = ring_entries[i];
			auto ring_offset = ring_entry.offset;
			auto ring_length = ring_entry.length;
			result_str += "(";
			for (idx_t j = ring_offset; j < ring_offset + ring_length; j++) {
				result_str += Utils::format_coord(x_data[j], y_data[j]);
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
// BOX_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Box2DToVarchar(Vector &source, Vector &result, idx_t count) {
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;
	GenericExecutor::ExecuteUnary<BOX_TYPE, VARCHAR_TYPE>(source, result, count, [&](BOX_TYPE &box) {
		return StringVector::AddString(result,
		                               StringUtil::Format("BOX(%s, %s)", Utils::format_coord(box.a_val, box.b_val),
		                                                  Utils::format_coord(box.c_val, box.d_val)));
	});
}

//------------------------------------------------------------------------------
// GEOMETRY -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::GeometryToVarchar(Vector &source, Vector &result, idx_t count, GeometryFactory &factory) {
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t &input) {
		auto geometry = factory.Deserialize(input);
		return StringVector::AddString(result, geometry.ToString());
	});
}

//------------------------------------------------------------------------------
// CASTS
//------------------------------------------------------------------------------
static bool Point2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::Point2DToVarchar(source, result, count);
	return true;
}

static bool LineString2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::LineString2DToVarchar(source, result, count);
	return true;
}

static bool Polygon2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::Polygon2DToVarchar(source, result, count);
	return true;
}

static bool Box2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::Box2DToVarchar(source, result, count);
	return true;
}

static bool GeometryToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	CoreVectorOperations::GeometryToVarchar(source, result, count, lstate.factory);
	return true;
}

void CoreCastFunctions::RegisterVarcharCasts(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	casts.RegisterCastFunction(GeoTypes::POINT_2D(), LogicalType::VARCHAR, BoundCastInfo(Point2DToVarcharCast), 1);

	casts.RegisterCastFunction(GeoTypes::LINESTRING_2D(), LogicalType::VARCHAR,
	                           BoundCastInfo(LineString2DToVarcharCast), 1);

	casts.RegisterCastFunction(GeoTypes::POLYGON_2D(), LogicalType::VARCHAR, BoundCastInfo(Polygon2DToVarcharCast), 1);

	casts.RegisterCastFunction(GeoTypes::BOX_2D(), LogicalType::VARCHAR, BoundCastInfo(Box2DToVarcharCast), 1);

	casts.RegisterCastFunction(GeoTypes::GEOMETRY(), LogicalType::VARCHAR,
	                           BoundCastInfo(GeometryToVarcharCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
}

} // namespace core

} // namespace spatial
