
#include "geo/gdal/types.hpp"

#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace geo {

namespace gdal {

LogicalType GeoTypes::WKB_BLOB = LogicalType::BLOB;

void GeoTypes::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	// WKB_BLOB
	auto wkb_name = "WKB_BLOB";
	auto wkb_info = CreateTypeInfo(wkb_name, GeoTypes::WKB_BLOB);
	wkb_info.internal = true;
	wkb_info.temporary = true;
	GeoTypes::WKB_BLOB.SetAlias(wkb_name);
	auto entry = (TypeCatalogEntry *)catalog.CreateType(context, &wkb_info);
	LogicalType::SetCatalog(GeoTypes::WKB_BLOB, entry);

	// TODO: We should not allow this in the future as there is no guarantee that the WKB_BLOB is actually a valid WKB
	casts.RegisterCastFunction(GeoTypes::WKB_BLOB, LogicalType::BLOB, DefaultCasts::ReinterpretCast);
	casts.RegisterCastFunction(LogicalType::BLOB, GeoTypes::WKB_BLOB, DefaultCasts::ReinterpretCast);
}

} // namespace gdal

} // namespace geo