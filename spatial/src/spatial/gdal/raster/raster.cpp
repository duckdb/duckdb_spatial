#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

string Raster::GetLastErrorMsg() {
	return string(CPLGetLastErrorMsg());
}

} // namespace gdal

} // namespace spatial
