#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_WorldToRasterCoord[XY]
//------------------------------------------------------------------------------

static void RasterWorldToRasterCoordFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using DOUBLE_TYPE = PrimitiveType<double_t>;
	using COORD_TYPE = StructTypeBinary<int32_t, int32_t>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p3 = args.data[2];

	GenericExecutor::ExecuteTernary<POINTER_TYPE, DOUBLE_TYPE, DOUBLE_TYPE, COORD_TYPE>(
	    p1, p2, p3, result, args.size(), [&](POINTER_TYPE p1, DOUBLE_TYPE p2, DOUBLE_TYPE p3) {
		    auto input = p1.val;
		    auto x = p2.val;
		    auto y = p3.val;
		    Raster raster(reinterpret_cast<GDALDataset *>(input));

		    RasterCoord coord(0, 0);
		    if (!raster.WorldToRasterCoord(coord, x, y)) {
			    throw InternalException("Could not compute inverse geotransform matrix");
		    }
		    return COORD_TYPE {coord.col, coord.row};
	    });
}

static void RasterWorldToRasterCoordXFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	TernaryExecutor::Execute<uintptr_t, double_t, double_t, int32_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [&](uintptr_t input, double_t x, double_t y) {
		    Raster raster(reinterpret_cast<GDALDataset *>(input));

		    RasterCoord coord(0, 0);
		    if (!raster.WorldToRasterCoord(coord, x, y)) {
			    throw InternalException("Could not compute inverse geotransform matrix");
		    }
		    return coord.col;
	    });
}

static void RasterWorldToRasterCoordYFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);

	TernaryExecutor::Execute<uintptr_t, double_t, double_t, int32_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [&](uintptr_t input, double_t x, double_t y) {
		    Raster raster(reinterpret_cast<GDALDataset *>(input));

		    RasterCoord coord(0, 0);
		    if (!raster.WorldToRasterCoord(coord, x, y)) {
			    throw InternalException("Could not compute inverse geotransform matrix");
		    }
		    return coord.row;
	    });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION_1 = R"(
	Returns the upper left corner as column and row given geometric X and Y (longitude and latitude).
	Geometric X and Y must be expressed in the spatial reference coordinate system of the raster.
)";

static constexpr const char *DOC_EXAMPLE_1 = R"(
	SELECT ST_WorldToRasterCoord(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_1[] = {{"ext", "spatial"}, {"category", "position"}};

static constexpr const char *DOC_DESCRIPTION_2 = R"(
	Returns the column in the raster given geometric X and Y (longitude and latitude).
	Geometric X and Y must be expressed in the spatial reference coordinate system of the raster.
)";

static constexpr const char *DOC_EXAMPLE_2 = R"(
	SELECT ST_WorldToRasterCoordX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_2[] = {{"ext", "spatial"}, {"category", "position"}};

static constexpr const char *DOC_DESCRIPTION_3 = R"(
	Returns the row in the raster given geometric X and Y (longitude and latitude).
	Geometric X and Y must be expressed in the spatial reference coordinate system of the raster.
)";

static constexpr const char *DOC_EXAMPLE_3 = R"(
	SELECT ST_WorldToRasterCoordY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_3[] = {{"ext", "spatial"}, {"category", "position"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStWorldToRasterCoord(DatabaseInstance &db) {

	ScalarFunctionSet set01("ST_WorldToRasterCoord");
	set01.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::DOUBLE, LogicalType::DOUBLE},
	                                 GeoTypes::RASTER_COORD(), RasterWorldToRasterCoordFunction));
	ExtensionUtil::RegisterFunction(db, set01);

	DocUtil::AddDocumentation(db, "ST_WorldToRasterCoord", DOC_DESCRIPTION_1, DOC_EXAMPLE_1, DOC_TAGS_1);

	ScalarFunctionSet set02("ST_WorldToRasterCoordX");
	set02.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::DOUBLE, LogicalType::DOUBLE},
	                                 LogicalType::INTEGER, RasterWorldToRasterCoordXFunction));
	ExtensionUtil::RegisterFunction(db, set02);

	DocUtil::AddDocumentation(db, "ST_WorldToRasterCoordX", DOC_DESCRIPTION_2, DOC_EXAMPLE_2, DOC_TAGS_2);

	ScalarFunctionSet set03("ST_WorldToRasterCoordY");
	set03.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::DOUBLE, LogicalType::DOUBLE},
	                                 LogicalType::INTEGER, RasterWorldToRasterCoordYFunction));
	ExtensionUtil::RegisterFunction(db, set03);

	DocUtil::AddDocumentation(db, "ST_WorldToRasterCoordY", DOC_DESCRIPTION_3, DOC_EXAMPLE_3, DOC_TAGS_3);
}

} // namespace gdal

} // namespace spatial
