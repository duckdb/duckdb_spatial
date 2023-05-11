#define DUCKDB_EXTENSION_MAIN

#include "spatial_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include "spatial/core/module.hpp"
#include "spatial/gdal/module.hpp"
#include "spatial/geos/module.hpp"
#include "spatial/proj/module.hpp"
#include "spatial/geographiclib/module.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();

	auto &context = *con.context;

	spatial::core::CoreModule::Register(context);
	spatial::proj::ProjModule::Register(context);
	spatial::gdal::GdalModule::Register(context);
	spatial::geos::GeosModule::Register(context);
	spatial::geographiclib::GeographicLibModule::Register(context);

	con.Commit();
}

void SpatialExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string SpatialExtension::Name() {
	return "spatial";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void spatial_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *spatial_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
