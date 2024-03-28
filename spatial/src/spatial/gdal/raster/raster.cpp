#include "spatial/core/types.hpp"
#include "spatial/gdal/types.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"
#include <float.h> /* for FLT_EPSILON */

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

bool Raster::GetInvGeoTransform(double *inv_matrix) const {
	double gt[6] = {0};
	GetGeoTransform(gt);

	if (!GDALInvGeoTransform(gt, inv_matrix)) {
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

bool Raster::RasterToWorldCoord(PointXY &point, int32_t col, int32_t row) const {
	double gt[6] = {0};
	GetGeoTransform(gt);
	return Raster::RasterToWorldCoord(point, gt, col, row);
}

bool Raster::RasterToWorldCoord(PointXY &point, double matrix[], int32_t col, int32_t row) {
	point.x = matrix[0] + matrix[1] * col + matrix[2] * row;
	point.y = matrix[3] + matrix[4] * col + matrix[5] * row;
	return true;
}

bool Raster::WorldToRasterCoord(RasterCoord &coord, double x, double y) const {
	double inv_gt[6] = {0};

	if (GetInvGeoTransform(inv_gt)) {
		return Raster::WorldToRasterCoord(coord, inv_gt, x, y);
	}
	return false;
}

bool Raster::WorldToRasterCoord(RasterCoord &coord, double inv_matrix[], double x, double y) {
	double xr = 0, yr = 0;
	GDALApplyGeoTransform(inv_matrix, x, y, &xr, &yr);

	// Source:
	// https://github.com/postgis/postgis/blob/stable-3.4/raster/rt_core/rt_raster.c#L808
	double rnd = 0;

	// Helper macro for symmetrical rounding
	#define ROUND(x, y) (((x > 0.0) ? floor((x * pow(10, y) + 0.5)) : ceil((x * pow(10, y) - 0.5))) / pow(10, y))
	// Helper macro for consistent floating point equality checks
	#define FLT_EQ(x, y) ((x == y) || (isnan(x) && isnan(y)) || (fabs(x - y) <= FLT_EPSILON))

	rnd = ROUND(xr, 0);
	if (FLT_EQ(rnd, xr))
		xr = rnd;
	else
		xr = floor(xr);

	rnd = ROUND(yr, 0);
	if (FLT_EQ(rnd, yr))
		yr = rnd;
	else
		yr = floor(yr);

	coord.col = (int32_t)xr;
	coord.row = (int32_t)yr;
	return true;
}

bool Raster::GetValue(double &value, int32_t band_num, int32_t col, int32_t row) const {

	GDALRasterBand *raster_band = dataset_->GetRasterBand(band_num);
	double pixel_value = raster_band->GetNoDataValue();

	if (raster_band->RasterIO(GF_Read, col, row, 1, 1, &pixel_value, 1, 1, GDT_Float64, 0, 0) == CE_None) {
		value = pixel_value;
		return true;
	}
	return false;
}

string Raster::GetLastErrorMsg() {
	return string(CPLGetLastErrorMsg());
}

} // namespace gdal

} // namespace spatial
