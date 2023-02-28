#include "geo/gdal/module.hpp"
#include "geo/gdal/functions.hpp"
#include "geo/gdal/types.hpp"

#include "gdal.h"
#include "geo/common.hpp"

namespace geo {

namespace gdal {

void GdalModule::Register(ClientContext &context) {

	// Load GDAL
	GDALAllRegister();

	// Register functions
	GeoTypes::Register(context);
	GdalTableFunction::Register(context);
}

} // namespace gdal

} // namespace geo
