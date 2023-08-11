#include "spatial/gdal/module.hpp"
#include "spatial/gdal/functions.hpp"

#include "spatial/common.hpp"

#include "ogrsf_frmts.h"

namespace spatial {

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

} // namespace spatial
