#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_GetBandPixelType
//------------------------------------------------------------------------------

static void RasterGetPixelTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	BinaryExecutor::Execute<uintptr_t, int32_t, int32_t>(
	    args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		    if (band_num < 1) {
			    throw InvalidInputException("BandNum must be greater than 0");
		    }
		    if (dataset->GetRasterCount() < band_num) {
			    throw InvalidInputException("Dataset only has %d RasterBands", dataset->GetRasterCount());
		    }
		    GDALRasterBand *raster_band = dataset->GetRasterBand(band_num);
		    return (int32_t)raster_band->GetRasterDataType();
	    });
}

//------------------------------------------------------------------------------
// ST_GetBandPixelTypeName
//------------------------------------------------------------------------------

static void RasterGetPixelTypeNameFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	BinaryExecutor::Execute<uintptr_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		    if (band_num < 1) {
			    throw InvalidInputException("BandNum must be greater than 0");
		    }
		    if (dataset->GetRasterCount() < band_num) {
			    throw InvalidInputException("Dataset only has %d RasterBands", dataset->GetRasterCount());
		    }
		    GDALRasterBand *raster_band = dataset->GetRasterBand(band_num);
		    return GetPixelTypeName((PixelType)raster_band->GetRasterDataType());
	    });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION_1 = R"(
	Returns the pixel type of a band in the raster.

	This is a code in the enumeration:

	+ Unknown = 0: Unknown or unspecified type
	+ Byte = 1: Eight bit unsigned integer
	+ Int8 = 14: 8-bit signed integer
	+ UInt16 = 2: Sixteen bit unsigned integer
	+ Int16 = 3: Sixteen bit signed integer
	+ UInt32 = 4: Thirty two bit unsigned integer
	+ Int32 = 5: Thirty two bit signed integer
	+ UInt64 = 12: 64 bit unsigned integer
	+ Int64 = 13: 64 bit signed integer
	+ Float32 = 6: Thirty two bit floating point
	+ Float64 = 7: Sixty four bit floating point
	+ CInt16 = 8: Complex Int16
	+ CInt32 = 9: Complex Int32
	+ CFloat32 = 10: Complex Float32
	+ CFloat64 = 11: Complex Float64
)";

static constexpr const char *DOC_EXAMPLE_1 = R"(
	SELECT ST_GetBandPixelType(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_1[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_2 = R"(
	Returns the pixel type name of a band in the raster.

	This is a string in the enumeration:

	+ Unknown: Unknown or unspecified type
	+ Byte: Eight bit unsigned integer
	+ Int8: 8-bit signed integer
	+ UInt16: Sixteen bit unsigned integer
	+ Int16: Sixteen bit signed integer
	+ UInt32: Thirty two bit unsigned integer
	+ Int32: Thirty two bit signed integer
	+ UInt64: 64 bit unsigned integer
	+ Int64: 64 bit signed integer
	+ Float32: Thirty two bit floating point
	+ Float64: Sixty four bit floating point
	+ CInt16: Complex Int16
	+ CInt32: Complex Int32
	+ CFloat32: Complex Float32
	+ CFloat64: Complex Float64
)";

static constexpr const char *DOC_EXAMPLE_2 = R"(
	SELECT ST_GetBandPixelTypeName(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_2[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStBandPixelType(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandPixelType");
	set.AddFunction(
	    ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::INTEGER, RasterGetPixelTypeFunction));
	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_GetBandPixelType", DOC_DESCRIPTION_1, DOC_EXAMPLE_1, DOC_TAGS_1);
}

void GdalScalarFunctions::RegisterStBandPixelTypeName(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandPixelTypeName");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::VARCHAR,
	                               RasterGetPixelTypeNameFunction));
	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_GetBandPixelTypeName", DOC_DESCRIPTION_2, DOC_EXAMPLE_2, DOC_TAGS_2);
}

} // namespace gdal

} // namespace spatial
