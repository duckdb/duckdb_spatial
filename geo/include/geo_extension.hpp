#pragma once

#include "duckdb.hpp"

namespace duckdb {

class GeoExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
