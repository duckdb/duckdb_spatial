#include "spatial/core/types.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

Raster::Raster(GDALDataset *dataset) : dataset_(dataset) {
}

int Raster::GetRasterXSize() const {
	return dataset_->GetRasterXSize();
}

int Raster::GetRasterYSize() const {
	return dataset_->GetRasterYSize();
}

int Raster::GetRasterCount() const {
	return dataset_->GetRasterCount();
}

int32_t Raster::GetSrid() const {

	int32_t srid = 0; // SRID_UNKNOWN

	const char *proj_def = dataset_->GetProjectionRef();
	if (proj_def) {
		OGRSpatialReference spatial_ref;
		spatial_ref.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);

		if (spatial_ref.importFromWkt(proj_def) == OGRERR_NONE &&
		    spatial_ref.AutoIdentifyEPSG() == OGRERR_NONE) {

			const char *code = spatial_ref.GetAuthorityCode(nullptr);
			if (code) {
				srid = atoi(code);
			}
		}
	}
	return srid;
}

bool Raster::GetGeoTransform(double *matrix) const {

	if (dataset_->GetGeoTransform(matrix) != CE_None) {
		// Using default geotransform matrix (0, 1, 0, 0, 0, -1)
		matrix[0] = 0;
		matrix[1] = 1;
		matrix[2] = 0;
		matrix[3] = 0;
		matrix[4] = 0;
		matrix[5] = -1;
		return false;
	}
	return true;
}

static VertexXY rasterToWorldVertex(double matrix[], int32_t col, int32_t row) {
	double xgeo = matrix[0] + matrix[1] * col + matrix[2] * row;
	double ygeo = matrix[3] + matrix[4] * col + matrix[5] * row;
	return VertexXY {xgeo, ygeo};
}

Polygon Raster::GetGeometry(GeometryFactory &factory) const {
	auto cols = dataset_->GetRasterXSize();
	auto rows = dataset_->GetRasterYSize();

	double gt[6] = {0};
	GetGeoTransform(gt);

	Polygon polygon(factory.allocator, 1, false, false);
	auto &ring = polygon[0];
	ring.Resize(factory.allocator, 5); // 4 vertices + 1 for closing the polygon
	ring.Set(0, rasterToWorldVertex(gt, 0, 0));
	ring.Set(1, rasterToWorldVertex(gt, cols, 0));
	ring.Set(2, rasterToWorldVertex(gt, cols, rows));
	ring.Set(3, rasterToWorldVertex(gt, 0, rows));
	ring.Set(4, rasterToWorldVertex(gt, 0, 0));

	return polygon;
}

string Raster::GetLastErrorMsg() {
	return string(CPLGetLastErrorMsg());
}

} // namespace gdal

} // namespace spatial
