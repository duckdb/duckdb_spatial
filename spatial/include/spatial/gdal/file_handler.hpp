#pragma once

#include "spatial/common.hpp"
#include "spatial/gdal/raster/raster_registry.hpp"
#include "duckdb/main/database.hpp"

namespace spatial {

namespace gdal {

class DuckDBFileSystemHandler;

class GDALClientContextState : public ClientContextState {
	string client_prefix;
	DuckDBFileSystemHandler *fs_handler;
	std::map<uintptr_t, std::unique_ptr<RasterRegistry>> registries;
	std::mutex lock;

public:
	explicit GDALClientContextState(ClientContext &context);
	~GDALClientContextState() override;
	void QueryEnd() override;
	void QueryEnd(ClientContext &context) override;
	string GetPrefix(const string &value) const;
	static GDALClientContextState &GetOrCreate(ClientContext &context);
	RasterRegistry &GetRasterRegistry(ClientContext &context);
};

} // namespace gdal

} // namespace spatial
