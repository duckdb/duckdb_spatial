#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

class GDALDataset;

using namespace spatial::core;

namespace spatial {

namespace gdal {

//! A simple wrapper of GDALDataset with useful methods to manage raster data.
//! Does not take ownership of the pointer.
class Raster {
public:
	//! Constructor
	Raster(GDALDataset *dataset);

	//! Returns the pointer to the dataset managed
	GDALDataset *operator->() const noexcept {
		return dataset_;
	}
	//! Returns the pointer to the dataset managed
	GDALDataset *get() const noexcept {
		return dataset_;
	}

	//! Returns the raster width in pixels
	int GetRasterXSize() const;

	//! Returns the raster height in pixels
	int GetRasterYSize() const;

	//! Returns the number of raster bands
	int GetRasterCount() const;

	//! Returns the spatial reference identifier of the raster
	int32_t GetSrid() const;

	//! Gets the geometric transform matrix (double[6]) of the raster
	bool GetGeoTransform(double *matrix) const;

	//! Returns the polygon representation of the extent of the raster
	Polygon GetGeometry(GeometryFactory &factory) const;

public:

	//! Get the last error message.
	static string GetLastErrorMsg();

private:
	GDALDataset *dataset_;
};

} // namespace gdal

} // namespace spatial
