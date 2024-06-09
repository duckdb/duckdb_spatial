#pragma once
#include "spatial/common.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

//! A registry of Rasters (GDALDataset) where items are released.
//! This takes ownership of items registered.
class RasterRegistry {
public:
	//! Constructor
	RasterRegistry();
	//! Destructor
	~RasterRegistry();

	//! Register a raster
	void RegisterRaster(GDALDataset *dataset);

private:
	std::vector<GDALDatasetUniquePtr> datasets_;
};

} // namespace gdal

} // namespace spatial
