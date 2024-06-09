#pragma once
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/gdal/types.hpp"

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

	//! Gets the inverse geometric transform matrix (double[6]) of the raster
	bool GetInvGeoTransform(double *inv_matrix) const;

	//! Returns the polygon representation of the extent of the raster
	Geometry GetGeometry(ArenaAllocator &allocator) const;

	//! Returns the geometric X and Y (longitude and latitude) given a column and row
	bool RasterToWorldCoord(PointXY &point, int32_t col, int32_t row) const;

	//! Returns the upper left corner as column and row given geometric X and Y
	bool WorldToRasterCoord(RasterCoord &coord, double x, double y) const;

	//! Returns the value of a given band in a given col and row pixel
	bool GetValue(double &value, int32_t band_num, int32_t col, int32_t row) const;

public:
	//! Returns the geometric X and Y (longitude and latitude) given a column and row
	static bool RasterToWorldCoord(PointXY &point, double matrix[], int32_t col, int32_t row);

	//! Returns the upper left corner as column and row given geometric X and Y
	static bool WorldToRasterCoord(RasterCoord &coord, double inv_matrix[], double x, double y);

	//! Builds a VRT from a list of Rasters
	static GDALDataset *BuildVRT(const std::vector<GDALDataset *> &datasets,
	                             const std::vector<std::string> &options = std::vector<std::string>());

	//! Performs mosaicing, reprojection and/or warping on a raster
	static GDALDataset *Warp(GDALDataset *dataset,
	                         const std::vector<std::string> &options = std::vector<std::string>());

	//! Returns a raster that is clipped by the input geometry
	static GDALDataset *Clip(GDALDataset *dataset, const geometry_t &geometry,
	                         const std::vector<std::string> &options = std::vector<std::string>());

	//! Get the last error message.
	static string GetLastErrorMsg();

private:
	GDALDataset *dataset_;
};

} // namespace gdal

} // namespace spatial
