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

#include "spatial/doc_util.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

static string RemoveIndentAndTrailingWhitespace(const char *text) {
	string result;
	// Skip any empty first newlines if present
	while (*text == '\n') {
		text++;
	}

	// Track indent length
	auto indent_start = text;
	while (isspace(*text) && *text != '\n') {
		text++;
	}
	auto indent_len = text - indent_start;
	while (*text) {
		result += *text;
		if (*text++ == '\n') {
			// Remove all indentation, but only if it matches the first line's indentation
			bool matched_indent = true;
			for (auto i = 0; i < indent_len; i++) {
				if (*text != indent_start[i]) {
					matched_indent = false;
					break;
				}
			}
			if (matched_indent) {
				text += indent_len;
			}
		}
	}

	// Also remove any trailing whitespace
	result.erase(result.find_last_not_of(" \n\r\t") + 1);
	return result;
}

void spatial::DocUtil::AddDocumentation(duckdb::DatabaseInstance &db, const char *function_name,
                                        const char *description, const char *example,
                                        const duckdb::unordered_map<duckdb::string, duckdb::string> &tags) {

	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::SCALAR_FUNCTION_ENTRY, function_name);
	if (!catalog_entry) {
		// Try get a aggregate function
		catalog_entry = schema.GetEntry(data, CatalogType::AGGREGATE_FUNCTION_ENTRY, function_name);
		if (!catalog_entry) {
			// Try get a table function
			catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, function_name);
			if (!catalog_entry) {
				throw duckdb::InvalidInputException("Function with name \"%s\" not found in DocUtil::AddDocumentation",
				                                    function_name);
			}
		}
	}

	auto &func_entry = catalog_entry->Cast<FunctionEntry>();
	if (description != nullptr) {
		func_entry.description = RemoveIndentAndTrailingWhitespace(description);
	}
	if (example != nullptr) {
		func_entry.example = RemoveIndentAndTrailingWhitespace(example);
	}
	if (!tags.empty()) {
		func_entry.tags = tags;
	}
}

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	spatial::core::CoreModule::Register(instance);
	spatial::proj::ProjModule::Register(instance);
	spatial::gdal::GdalModule::Register(instance);
	spatial::geos::GeosModule::Register(instance);
	spatial::geographiclib::GeographicLibModule::Register(instance);
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
