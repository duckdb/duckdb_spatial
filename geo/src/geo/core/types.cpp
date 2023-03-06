
#include "geo/core/types.hpp"
#include "geo/common.hpp"

#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace geo {

namespace core {


LogicalType GeoTypes::POINT_2D = LogicalType::STRUCT({{"x", LogicalTypeId::DOUBLE}, {"y", LogicalTypeId::DOUBLE}});

LogicalType GeoTypes::BOX_2D = LogicalType::STRUCT({{"min_x", LogicalTypeId::DOUBLE},
                                                    {"min_y", LogicalTypeId::DOUBLE},
                                                    {"max_x", LogicalTypeId::DOUBLE},
                                                    {"max_y", LogicalTypeId::DOUBLE}});

LogicalType GeoTypes::LINESTRING_2D =
    LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalTypeId::DOUBLE}, {"y", LogicalTypeId::DOUBLE}}));

LogicalType GeoTypes::POLYGON_2D =
    LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalTypeId::DOUBLE}, {"y", LogicalTypeId::DOUBLE}})));


LogicalType GeoTypes::WKB_BLOB = LogicalTypeId::BLOB;


void GeoTypes::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	// Point2D
	auto point_2d = CreateTypeInfo("POINT_2D", GeoTypes::POINT_2D);
	point_2d.temporary = true;
	point_2d.internal = true;
	GeoTypes::POINT_2D.SetAlias("POINT_2D");
	auto entry = (TypeCatalogEntry *)catalog.CreateType(context, &point_2d);
	LogicalType::SetCatalog(GeoTypes::POINT_2D, entry);

	// LineString2D
	auto linestring_2d = CreateTypeInfo("LINESTRING_2D", GeoTypes::LINESTRING_2D);
	linestring_2d.temporary = true;
	linestring_2d.internal = true;
	GeoTypes::LINESTRING_2D.SetAlias("LINESTRING_2D");
	entry = (TypeCatalogEntry *)catalog.CreateType(context, &linestring_2d);
	LogicalType::SetCatalog(GeoTypes::LINESTRING_2D, entry);

	// Polygon2D
	auto polygon_2d = CreateTypeInfo("POLYGON_2D", GeoTypes::POLYGON_2D);
	polygon_2d.temporary = true;
	polygon_2d.internal = true;
	GeoTypes::POLYGON_2D.SetAlias("POLYGON_2D");
	entry = (TypeCatalogEntry *)catalog.CreateType(context, &polygon_2d);
	LogicalType::SetCatalog(GeoTypes::POLYGON_2D, entry);

	// Box2D
	auto box_2d = CreateTypeInfo("BOX_2D", GeoTypes::BOX_2D);
	box_2d.temporary = true;
	box_2d.internal = true;
	GeoTypes::BOX_2D.SetAlias("BOX_2D");
	entry = (TypeCatalogEntry *)catalog.CreateType(context, &box_2d);
	LogicalType::SetCatalog(GeoTypes::BOX_2D, entry);

	// WKB_BLOB
	GeoTypes::WKB_BLOB.SetAlias("WKB_BLOB");
	auto wkb = CreateTypeInfo("WKB_BLOB", GeoTypes::WKB_BLOB);
	wkb.internal = true;
	wkb.temporary = true;
	catalog.CreateType(context, &wkb);

	casts.RegisterCastFunction(GeoTypes::WKB_BLOB, LogicalType::BLOB, DefaultCasts::ReinterpretCast);
}

} // namespace core

} // namespace geo