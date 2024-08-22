#include "spatial/core/types.hpp"
#include "spatial/gdal/raster/raster.hpp"
#include "spatial/gdal/raster/raster_value.hpp"

using namespace spatial::core;

namespace spatial {

namespace gdal {

Value RasterValue::CreateValue(GDALDataset *dataset) {
	Value value = Value::POINTER(CastPointerToValue(dataset));
	value.Reinterpret(GeoTypes::RASTER());
	return value;
}

GDALDataset *RasterValue::operator->() const {
	GDALDataset *dataset = reinterpret_cast<GDALDataset *>(GetValueUnsafe<uint64_t>());
	return dataset;
}

GDALDataset *RasterValue::get() const {
	GDALDataset *dataset = reinterpret_cast<GDALDataset *>(GetValueUnsafe<uint64_t>());
	return dataset;
}

} // namespace gdal

} // namespace spatial
