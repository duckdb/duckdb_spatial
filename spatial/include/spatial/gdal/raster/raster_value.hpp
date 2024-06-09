#pragma once
#include "spatial/common.hpp"

class GDALDataset;

namespace spatial {

namespace gdal {

//! This Value object holds a Raster instance
class RasterValue : public Value {
public:
	//! Returns the pointer to the dataset
	GDALDataset *operator->() const;
	//! Returns the pointer to the dataset
	GDALDataset *get() const;

	//! Create a RASTER value
	static Value CreateValue(GDALDataset *dataset);
};

} // namespace gdal

} // namespace spatial
