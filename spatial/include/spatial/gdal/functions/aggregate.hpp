#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalAggregateFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStRasterMosaicAgg(db);
		RegisterStRasterUnionAgg(db);
	}

private:
	static void RegisterStRasterMosaicAgg(DatabaseInstance &db);
	static void RegisterStRasterUnionAgg(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
