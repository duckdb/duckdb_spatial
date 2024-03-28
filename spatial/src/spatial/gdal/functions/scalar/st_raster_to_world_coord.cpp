#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_RasterToWorldCoord[XY]
//------------------------------------------------------------------------------

static void RasterRasterToWorldCoordFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using INT_TYPE = PrimitiveType<int32_t>;
	using POINT_TYPE = StructTypeBinary<double, double>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p3 = args.data[2];

	GenericExecutor::ExecuteTernary<POINTER_TYPE, INT_TYPE, INT_TYPE, POINT_TYPE>(p1, p2, p3, result, args.size(),
		[&](POINTER_TYPE p1, INT_TYPE p2, INT_TYPE p3) {
			auto input = p1.val;
			auto col = p2.val;
			auto row = p3.val;
			Raster raster(reinterpret_cast<GDALDataset *>(input));

			PointXY coord(0, 0);
			if (!raster.RasterToWorldCoord(coord, col, row)) {
				throw InternalException("Could not compute geotransform matrix");
			}
			return POINT_TYPE {coord.x, coord.y};
		}
	);
}

static void RasterRasterToWorldCoordXFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	TernaryExecutor::Execute<uintptr_t, int32_t, int32_t, double>(args.data[0], args.data[1], args.data[2], result, args.size(),
		[&](uintptr_t input, int32_t col, int32_t row) {
			Raster raster(reinterpret_cast<GDALDataset *>(input));

			PointXY coord(0, 0);
			if (!raster.RasterToWorldCoord(coord, col, row)) {
				throw InternalException("Could not compute geotransform matrix");
			}
			return coord.x;
		}
	);
}

static void RasterRasterToWorldCoordYFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	TernaryExecutor::Execute<uintptr_t, int32_t, int32_t, double>(args.data[0], args.data[1], args.data[2], result, args.size(),
		[&](uintptr_t input, int32_t col, int32_t row) {
			Raster raster(reinterpret_cast<GDALDataset *>(input));

			PointXY coord(0, 0);
			if (!raster.RasterToWorldCoord(coord, col, row)) {
				throw InternalException("Could not compute geotransform matrix");
			}
			return coord.y;
		}
	);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStRasterToWorldCoord(DatabaseInstance &db) {

	ScalarFunctionSet set01("ST_RasterToWorldCoord");
	set01.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER, LogicalType::INTEGER}, GeoTypes::POINT_2D(), RasterRasterToWorldCoordFunction));
	ExtensionUtil::RegisterFunction(db, set01);

	ScalarFunctionSet set02("ST_RasterToWorldCoordX");
	set02.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::DOUBLE, RasterRasterToWorldCoordXFunction));
	ExtensionUtil::RegisterFunction(db, set02);

	ScalarFunctionSet set03("ST_RasterToWorldCoordY");
	set03.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::DOUBLE, RasterRasterToWorldCoordYFunction));
	ExtensionUtil::RegisterFunction(db, set03);
}

} // namespace gdal

} // namespace spatial
