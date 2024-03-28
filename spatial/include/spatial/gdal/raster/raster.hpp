#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

//! A simple wrapper of GDALDataset with useful methods to manage raster data.
//! Does not take ownership of the pointer.
class Raster {
public:

	//! Get the last error message.
	static string GetLastErrorMsg();
};

} // namespace gdal

} // namespace spatial
