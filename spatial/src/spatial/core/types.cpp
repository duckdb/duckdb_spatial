#include "spatial/core/types.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"

namespace spatial {

namespace core {

LogicalType GeoTypes::POINT_2D() {
	auto type = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});
	type.SetAlias("POINT_2D");
	return type;
}

LogicalType GeoTypes::POINT_3D() {
	auto type =
	    LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"z", LogicalType::DOUBLE}});
	type.SetAlias("POINT_3D");
	return type;
}

LogicalType GeoTypes::POINT_4D() {
	auto type = LogicalType::STRUCT({{"x", LogicalType::DOUBLE},
	                                 {"y", LogicalType::DOUBLE},
	                                 {"z", LogicalType::DOUBLE},
	                                 {"m", LogicalType::DOUBLE}});
	type.SetAlias("POINT_4D");
	return type;
}

LogicalType GeoTypes::BOX_2D() {
	auto type = LogicalType::STRUCT({{"min_x", LogicalType::DOUBLE},
	                                 {"min_y", LogicalType::DOUBLE},
	                                 {"max_x", LogicalType::DOUBLE},
	                                 {"max_y", LogicalType::DOUBLE}});
	type.SetAlias("BOX_2D");
	return type;
}

LogicalType GeoTypes::LINESTRING_2D() {
	auto type = LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}));
	type.SetAlias("LINESTRING_2D");
	return type;
}

LogicalType GeoTypes::POLYGON_2D() {
	auto type = LogicalType::LIST(
	    LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}})));
	type.SetAlias("POLYGON_2D");
	return type;
}

LogicalType GeoTypes::GEOMETRY() {
	auto blob_type = LogicalType(LogicalTypeId::BLOB);
	blob_type.SetAlias("GEOMETRY");
	return blob_type;
}

LogicalType GeoTypes::WKB_BLOB() {
	auto blob_type = LogicalType(LogicalTypeId::BLOB);
	blob_type.SetAlias("WKB_BLOB");
	return blob_type;
}

static void AddType(Catalog &catalog, ClientContext &context, LogicalType type, const char *name) {
	CreateTypeInfo type_info(name, std::move(type));
	type_info.temporary = true;
	type_info.internal = true;
	catalog.CreateType(context, type_info);
}

void GeoTypes::Register(DatabaseInstance &instance) {
	// POINT_2D
	ExtensionUtil::RegisterType(instance, "POINT_2D", GeoTypes::POINT_2D());

	// POINT_3D
	ExtensionUtil::RegisterType(instance, "POINT_3D", GeoTypes::POINT_3D());

	// POINT_4D
	ExtensionUtil::RegisterType(instance, "POINT_4D", GeoTypes::POINT_4D());

	// LineString2D
	ExtensionUtil::RegisterType(instance, "LINESTRING_2D", GeoTypes::LINESTRING_2D());

	// Polygon2D
	ExtensionUtil::RegisterType(instance, "POLYGON_2D", GeoTypes::POLYGON_2D());

	// Box2D
	ExtensionUtil::RegisterType(instance, "BOX_2D", GeoTypes::BOX_2D());

	// GEOMETRY
	ExtensionUtil::RegisterType(instance, "GEOMETRY", GeoTypes::GEOMETRY());

	// WKB_BLOB
	ExtensionUtil::RegisterType(instance, "WKB_BLOB", GeoTypes::WKB_BLOB());
}

} // namespace core

} // namespace spatial