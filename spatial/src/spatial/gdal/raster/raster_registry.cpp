#include "spatial/core/types.hpp"
#include "spatial/gdal/raster/raster.hpp"
#include "spatial/gdal/raster/raster_registry.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

RasterRegistry::RasterRegistry() {
}

RasterRegistry::~RasterRegistry() {

	// Release items in reverse order, first children, then parent ones
	for (auto it = datasets_.rbegin(); it != datasets_.rend(); ++it) {
		auto datasetUniquePtr = std::move(*it);
		datasetUniquePtr.reset();
	}
}

void RasterRegistry::RegisterRaster(GDALDataset *dataset) {
	datasets_.emplace_back(GDALDatasetUniquePtr(dataset));
}

} // namespace gdal

} // namespace spatial
