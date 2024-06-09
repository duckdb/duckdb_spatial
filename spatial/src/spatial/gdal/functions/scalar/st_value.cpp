#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_Value
//------------------------------------------------------------------------------

static void RasterGetValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 4);

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using INT_TYPE = PrimitiveType<int32_t>;
	using DOUBLE_TYPE = PrimitiveType<double>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p3 = args.data[2];
	auto &p4 = args.data[3];

	GenericExecutor::ExecuteQuaternary<POINTER_TYPE, INT_TYPE, INT_TYPE, INT_TYPE, DOUBLE_TYPE>(
	    p1, p2, p3, p4, result, args.size(), [&](POINTER_TYPE p1, INT_TYPE p2, INT_TYPE p3, INT_TYPE p4) {
		    auto input = p1.val;
		    auto band_num = p2.val;
		    auto col = p3.val;
		    auto row = p4.val;

		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		    auto cols = dataset->GetRasterXSize();
		    auto rows = dataset->GetRasterYSize();

		    if (band_num < 1) {
			    throw InvalidInputException("BandNum must be greater than 0");
		    }
		    if (dataset->GetRasterCount() < band_num) {
			    throw InvalidInputException("Dataset only has %d RasterBands", dataset->GetRasterCount());
		    }
		    if (col < 0 || col >= cols || row < 0 || row >= rows) {
			    throw InvalidInputException(
			        "Attempting to get pixel value with out of range raster coordinates: (%d, %d)", col, row);
		    }

		    Raster raster(dataset);
		    double value;
		    if (raster.GetValue(value, band_num, col, row)) {
			    return value;
		    }
		    throw InternalException("Failed attempting to get pixel value with raster coordinates: (%d, %d)", col, row);
	    });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns the value of a given band in a given column, row pixel.
	Band numbers start at 1 and band is assumed to be 1 if not specified.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT ST_Value(raster, 1, 0, 0) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetValue(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Value");
	set.AddFunction(
	    ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER},
	                   LogicalType::DOUBLE, RasterGetValueFunction));

	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_Value", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
