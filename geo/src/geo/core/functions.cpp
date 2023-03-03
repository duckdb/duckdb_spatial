#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions.hpp"
#include "geo/core/geometry/geometry.hpp"

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
		auto point = (Geometry*)ctx.CreatePoint(x, y);
		return ctx.SaveGeometry(point, result);
	});

}

static void PointToStringOperation(Vector &input, Vector &output, idx_t count) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc, 1024);
	GeometryContext ctx(allocator);

	UnaryExecutor::Execute<string_t, string_t>(input, output, count, [&](string_t input) {
		auto type = ctx.PeekType(input);
		if (type != GeometryType::Point) {
			throw NotImplementedException("Geometry type not implemented");
		}
		auto point = (Point*)ctx.LoadGeometry(input);
		auto x = point->X();
		auto y = point->Y();
		return StringUtil::Format("%.2f %.2f", x, y);
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

	auto &casts = DBConfig::GetConfig(context).GetCastFunctions();
	casts.RegisterCastFunction(GeoTypes::GEOMETRY, LogicalType::VARCHAR, DefaultCasts::ReinterpretCast);
}

} // namespace core

} // namespace geo
