#include "geo/gdal/module.hpp"
#include "geo/gdal/functions.hpp"

#include "geo/common.hpp"

#include "ogrsf_frmts.h"

namespace geo {

namespace gdal {

void GdalModule::Register(ClientContext &context) {

	// Load GDAL
	OGRRegisterAll();

	// Register functions
	GdalTableFunction::Register(context);
	GdalDriversTableFunction::Register(context);
	GdalCopyFunction::Register(context);
}

} // namespace gdal

} // namespace geo
