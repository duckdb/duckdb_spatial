#pragma once

#include "spatial/common.hpp"
#include "duckdb/main/database.hpp"

namespace spatial {

namespace gdal {

class DuckDBFileSystemHandler;

class GDALClientContextState : public ClientContextState {
	string client_prefix;
	DuckDBFileSystemHandler* fs_handler;
public:
	explicit GDALClientContextState(ClientContext& context);
	~GDALClientContextState() override;
	void QueryEnd() override;
	const string& GetPrefix() const;
	static GDALClientContextState& GetOrCreate(ClientContext& context);
};

} // namespace gdal

} // namespace spatial
