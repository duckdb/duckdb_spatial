#define DUCKDB_EXTENSION_MAIN

#include "geo_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include "geo/core/module.hpp"
#include "geo/gdal/module.hpp"
#include "geo/proj/module.hpp"

namespace duckdb {
inline void GeoScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Geo " + name.GetString() + " 🐥");
		;
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();

	auto &context = *con.context;

	geo::core::CoreModule::Register(context);
	geo::proj::ProjModule::Register(context);
	geo::gdal::GdalModule::Register(context);

	auto &catalog = Catalog::GetSystemCatalog(context);

	CreateScalarFunctionInfo geo_fun_info(
	    ScalarFunction("geo", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GeoScalarFun));
	geo_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &geo_fun_info);
	con.Commit();
}

void GeoExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string GeoExtension::Name() {
	return "geo";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void geo_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *geo_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
