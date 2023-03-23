
#include "geo/core/types.hpp"

#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "geo/common.hpp"
#include "geo/core/geometry/geometry_factory.hpp"

namespace geo {

namespace core {

LogicalType GeoTypes::POINT_2D = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});

LogicalType GeoTypes::POINT_3D =
    LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"z", LogicalType::DOUBLE}});

LogicalType GeoTypes::POINT_4D = LogicalType::STRUCT(
    {{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"z", LogicalType::DOUBLE}, {"m", LogicalType::DOUBLE}});

LogicalType GeoTypes::BOX_2D = LogicalType::STRUCT({{"min_x", LogicalType::DOUBLE},
                                                    {"min_y", LogicalType::DOUBLE},
                                                    {"max_x", LogicalType::DOUBLE},
                                                    {"max_y", LogicalType::DOUBLE}});

LogicalType GeoTypes::LINESTRING_2D = LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}));

LogicalType GeoTypes::POLYGON_2D = LogicalType::LIST(GeoTypes::LINESTRING_2D);

LogicalType GeoTypes::GEOMETRY = LogicalType::BLOB;

LogicalType GeoTypes::WKB_BLOB = LogicalType::BLOB;

static void AddType(Catalog &catalog, ClientContext &context, LogicalType &type, const string &name) {
	type.SetAlias(name);
	auto type_info = CreateTypeInfo(name, type);
	type_info.temporary = true;
	type_info.internal = true;
	catalog.CreateType(context, &type_info);
}

// Casts
static bool WKBToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t input) {
		auto geometry = ctx.FromWKB(input.GetDataUnsafe(), input.GetSize());
		return ctx.Serialize(result, geometry);
	});
	return true;
}

void GeoTypes::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	// POINT_2D
	AddType(catalog, context, GeoTypes::POINT_2D, "POINT_2D");

	// POINT_3D
	AddType(catalog, context, GeoTypes::POINT_3D, "POINT_3D");

	// POINT_4D
	AddType(catalog, context, GeoTypes::POINT_4D, "POINT_4D");

	// LineString2D
	AddType(catalog, context, GeoTypes::LINESTRING_2D, "LINESTRING_2D");

	// Polygon2D
	AddType(catalog, context, GeoTypes::POLYGON_2D, "POLYGON_2D");

	// Box2D
	AddType(catalog, context, GeoTypes::BOX_2D, "BOX_2D");

	// GEOMETRY
	AddType(catalog, context, GeoTypes::GEOMETRY, "GEOMETRY");

	// WKB_BLOB
	AddType(catalog, context, GeoTypes::WKB_BLOB, "WKB_BLOB");

	casts.RegisterCastFunction(GeoTypes::WKB_BLOB, LogicalType::BLOB, DefaultCasts::ReinterpretCast);

	// TODO: remove this implicit cast once we have more functions for the geometry type itself
	casts.RegisterCastFunction(GeoTypes::WKB_BLOB, GeoTypes::GEOMETRY, WKBToGeometryCast, 1);
}

} // namespace core

} // namespace geo