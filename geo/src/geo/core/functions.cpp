#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace geo {

namespace core {

static void MakePoint2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc, 1024);
	GeometryContext ctx(allocator);

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<double, double, string_t>(x, y, result, count, [&](double x, double y) {
		auto point = ctx.CreatePoint(x, y);
		return ctx.Serialize(result, Geometry(std::move(point)));
	});
}

static void PointToStringOperation(Vector &input, Vector &output, idx_t count) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc, 1024);
	GeometryContext ctx(allocator);

	UnaryExecutor::Execute<string_t, string_t>(input, output, count, [&](string_t input) {
		/*
		auto type = ctx.PeekType(input);
		if (type != GeometryType::Point) {
			throw NotImplementedException("Geometry type not implemented");
		}
		auto point = (Point*)ctx.LoadGeometry(input);
		auto x = point->X();
		auto y = point->Y();
		return StringUtil::Format("%.2f %.2f", x, y);
		 */
		auto geometry = ctx.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::POINT:
			return StringVector::AddString(output, geometry.GetPoint().ToString());
		case GeometryType::LINESTRING:
			return StringVector::AddString(output, geometry.GetLineString().ToString());
		case GeometryType::POLYGON:
			return StringVector::AddString(output, geometry.GetPolygon().ToString());
		default:
			throw NotImplementedException("Geometry type not implemented");
		}

	});
}

static void GeometryFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryContext ctx(allocator);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = ctx.FromWKB(input.GetDataUnsafe(), input.GetSize());
		return ctx.Serialize(result, geometry);
	});
}

static void PointToStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	PointToStringOperation(args.data[0], result, args.size());
}

static bool PointToStringCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	PointToStringOperation(source, result, count);
	return true;
}

void GeometryFunctions::Register(ClientContext &context) {

	auto &catalog = Catalog::GetSystemCatalog(context);

	CreateScalarFunctionInfo info(ScalarFunction("st_point2d", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::GEOMETRY, MakePoint2DFunction));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);

	CreateScalarFunctionInfo info2(ScalarFunction("st_astext", {GeoTypes::GEOMETRY}, LogicalType::VARCHAR, PointToStringFunction));
	info2.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info2);

	CreateScalarFunctionInfo info3(ScalarFunction("st_fromwkb", {GeoTypes::WKB_BLOB}, GeoTypes::GEOMETRY, GeometryFromWKBFunction));
	info3.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info3);

	auto &casts = DBConfig::GetConfig(context).GetCastFunctions();
	casts.RegisterCastFunction(GeoTypes::GEOMETRY, LogicalType::VARCHAR, PointToStringCast);
}

} // namespace core

} // namespace geo
