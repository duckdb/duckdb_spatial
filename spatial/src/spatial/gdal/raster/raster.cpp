#include "duckdb/common/types/uuid.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"
#include "spatial/core/util/math.hpp"
#include "spatial/gdal/types.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"
#include "gdal_utils.h"
#include "gdalwarper.h"
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

		if (spatial_ref.importFromWkt(proj_def) == OGRERR_NONE && spatial_ref.AutoIdentifyEPSG() == OGRERR_NONE) {

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

Geometry Raster::GetGeometry(ArenaAllocator &allocator) const {
	auto cols = dataset_->GetRasterXSize();
	auto rows = dataset_->GetRasterYSize();

	double gt[6] = {0};
	GetGeoTransform(gt);

	VertexXY vertex1 = rasterToWorldVertex(gt, 0, 0);
	VertexXY vertex2 = rasterToWorldVertex(gt, cols, rows);
	double minx = std::min(vertex1.x, vertex2.x);
	double miny = std::min(vertex1.y, vertex2.y);
	double maxx = std::max(vertex1.x, vertex2.x);
	double maxy = std::max(vertex1.y, vertex2.y);

	return Polygon::CreateFromBox(allocator, minx, miny, maxx, maxy);
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

GDALDataset *Raster::BuildVRT(const std::vector<GDALDataset *> &datasets, const std::vector<std::string> &options) {

	char **papszArgv = nullptr;

	for (auto it = options.begin(); it != options.end(); ++it) {
		papszArgv = CSLAddString(papszArgv, (*it).c_str());
	}

	CPLErrorReset();

	GDALBuildVRTOptions *psOptions = GDALBuildVRTOptionsNew(papszArgv, nullptr);
	CSLDestroy(papszArgv);

	auto result = GDALDatasetUniquePtr(GDALDataset::FromHandle(
	    GDALBuildVRT(nullptr, datasets.size(), (GDALDatasetH *)&datasets[0], nullptr, psOptions, nullptr)));

	GDALBuildVRTOptionsFree(psOptions);

	if (result.get() != nullptr) {
		result->FlushCache();
	}
	return result.release();
}

GDALDataset *Raster::Warp(GDALDataset *dataset, const std::vector<std::string> &options) {

	GDALDatasetH hDataset = GDALDataset::ToHandle(dataset);

	auto driver = GetGDALDriverManager()->GetDriverByName("MEM");
	if (!driver) {
		throw InvalidInputException("Unknown driver 'MEM'");
	}

	char **papszArgv = nullptr;
	papszArgv = CSLAddString(papszArgv, "-of");
	papszArgv = CSLAddString(papszArgv, "MEM");

	for (auto it = options.begin(); it != options.end(); ++it) {
		papszArgv = CSLAddString(papszArgv, (*it).c_str());
	}

	CPLErrorReset();

	GDALWarpAppOptions *psOptions = GDALWarpAppOptionsNew(papszArgv, nullptr);
	CSLDestroy(papszArgv);

	auto ds_name = UUID::ToString(UUID::GenerateRandomUUID());

	auto result = GDALDatasetUniquePtr(
	    GDALDataset::FromHandle(GDALWarp(ds_name.c_str(), nullptr, 1, &hDataset, psOptions, nullptr)));

	GDALWarpAppOptionsFree(psOptions);

	if (result.get() != nullptr) {
		result->FlushCache();
	}
	return result.release();
}

//! Transformer of Geometries to pixel/line coordinates
class CutlineTransformer : public OGRCoordinateTransformation {
public:
	void *hTransformArg = nullptr;

	explicit CutlineTransformer(void *hTransformArg) : hTransformArg(hTransformArg) {
	}
	virtual ~CutlineTransformer() {
		GDALDestroyTransformer(hTransformArg);
	}

	virtual const OGRSpatialReference *GetSourceCS() const override {
		return nullptr;
	}
	virtual const OGRSpatialReference *GetTargetCS() const override {
		return nullptr;
	}
	virtual OGRCoordinateTransformation *Clone() const override {
		return nullptr;
	}
	virtual OGRCoordinateTransformation *GetInverse() const override {
		return nullptr;
	}

	virtual int Transform(int nCount, double *x, double *y, double *z, double * /* t */, int *pabSuccess) override {
		return GDALGenImgProjTransform(hTransformArg, TRUE, nCount, x, y, z, pabSuccess);
	}
};

GDALDataset *Raster::Clip(GDALDataset *dataset, const geometry_t &geometry, const std::vector<std::string> &options) {

	GDALDatasetH hDataset = GDALDataset::ToHandle(dataset);

	auto driver = GetGDALDriverManager()->GetDriverByName("MEM");
	if (!driver) {
		throw InvalidInputException("Unknown driver 'MEM'");
	}

	char **papszArgv = nullptr;
	papszArgv = CSLAddString(papszArgv, "-of");
	papszArgv = CSLAddString(papszArgv, "MEM");

	for (auto it = options.begin(); it != options.end(); ++it) {
		papszArgv = CSLAddString(papszArgv, (*it).c_str());
	}

	// Add Bounds & Geometry in pixel/line coordinates to the options.
	if (geometry.GetType() == GeometryType::POLYGON || geometry.GetType() == GeometryType::MULTIPOLYGON) {

		OGRGeometryUniquePtr ogr_geom;

		OGRSpatialReference srs;
		srs.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
		const char *proj_ref = dataset->GetProjectionRef();
		if (proj_ref) {
			srs.importFromWkt(&proj_ref, nullptr);
		}

		vector<data_t> buffer;
		WKBWriter::Write(geometry, buffer);

		OGRGeometry *ptr_geom = nullptr;
		if (OGRGeometryFactory::createFromWkb(buffer.data(), &srs, &ptr_geom, buffer.size(), wkbVariantIso) !=
		    OGRERR_NONE) {

			CSLDestroy(papszArgv);
			throw InvalidInputException("Input Geometry could not imported");
		} else {
			ogr_geom = OGRGeometryUniquePtr(ptr_geom);
		}

		OGREnvelope envelope;
		ogr_geom->getEnvelope(&envelope);

		CutlineTransformer transformer(GDALCreateGenImgProjTransformer2(hDataset, nullptr, nullptr));

		if (ogr_geom->transform(&transformer) != OGRERR_NONE) {
			CSLDestroy(papszArgv);
			throw InvalidInputException("Transform of geometry to pixel/line coordinates failed");
		}

		char *pszWkt = nullptr;
		if (ogr_geom->exportToWkt(&pszWkt) != OGRERR_NONE) {
			CSLDestroy(papszArgv);
			CPLFree(pszWkt);
			throw InvalidInputException("Input Geometry could not loaded");
		}
		std::string wkt_geom = pszWkt;
		CPLFree(pszWkt);

		std::string wkt_option = "CUTLINE=" + wkt_geom;
		papszArgv = CSLAddString(papszArgv, "-wo");
		papszArgv = CSLAddString(papszArgv, wkt_option.c_str());
		papszArgv = CSLAddString(papszArgv, "-te");
		papszArgv = CSLAddString(papszArgv, MathUtil::format_coord(envelope.MinX).c_str());
		papszArgv = CSLAddString(papszArgv, MathUtil::format_coord(envelope.MinY).c_str());
		papszArgv = CSLAddString(papszArgv, MathUtil::format_coord(envelope.MaxX).c_str());
		papszArgv = CSLAddString(papszArgv, MathUtil::format_coord(envelope.MaxY).c_str());
	}

	CPLErrorReset();

	GDALWarpAppOptions *psOptions = GDALWarpAppOptionsNew(papszArgv, nullptr);
	CSLDestroy(papszArgv);

	auto ds_name = UUID::ToString(UUID::GenerateRandomUUID());

	auto result = GDALDatasetUniquePtr(
	    GDALDataset::FromHandle(GDALWarp(ds_name.c_str(), nullptr, 1, &hDataset, psOptions, nullptr)));

	GDALWarpAppOptionsFree(psOptions);

	if (result.get() != nullptr) {
		result->FlushCache();
	}
	return result.release();
}

string Raster::GetLastErrorMsg() {
	return string(CPLGetLastErrorMsg());
}

} // namespace gdal

} // namespace spatial
