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
// ST_GetBandColorInterp
//------------------------------------------------------------------------------

static void RasterGetColorInterpFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		    return (int32_t)raster_band->GetColorInterpretation();
	    });
}

//------------------------------------------------------------------------------
// ST_GetBandColorInterpName
//------------------------------------------------------------------------------

static void RasterGetColorInterpNameFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		    return GetColorInterpName((ColorInterp)raster_band->GetColorInterpretation());
	    });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION_1 = R"(
	Returns the color interpretation of a band in the raster.

	This is a code in the enumeration:

	+ Undefined = 0: Undefined
	+ GrayIndex = 1: Greyscale
	+ PaletteIndex = 2: Paletted (see associated color table)
	+ RedBand = 3: Red band of RGBA image
	+ GreenBand = 4: Green band of RGBA image
	+ BlueBand = 5: Blue band of RGBA image
	+ AlphaBand = 6: Alpha (0=transparent, 255=opaque)
	+ HueBand = 7: Hue band of HLS image
	+ SaturationBand = 8: Saturation band of HLS image
	+ LightnessBand = 9: Lightness band of HLS image
	+ CyanBand = 10: Cyan band of CMYK image
	+ MagentaBand = 11: Magenta band of CMYK image
	+ YellowBand = 12: Yellow band of CMYK image
	+ BlackBand = 13: Black band of CMYK image
	+ YCbCr_YBand = 14: Y Luminance
	+ YCbCr_CbBand = 15: Cb Chroma
	+ YCbCr_CrBand = 16: Cr Chroma
)";

static constexpr const char *DOC_EXAMPLE_1 = R"(
	SELECT ST_GetBandColorInterp(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_1[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_2 = R"(
	Returns the color interpretation name of a band in the raster.

	This is a string in the enumeration:

	+ Undefined: Undefined
	+ Greyscale: Greyscale
	+ Paletted: Paletted (see associated color table)
	+ Red: Red band of RGBA image
	+ Green: Green band of RGBA image
	+ Blue: Blue band of RGBA image
	+ Alpha: Alpha (0=transparent, 255=opaque)
	+ Hue: Hue band of HLS image
	+ Saturation: Saturation band of HLS image
	+ Lightness: Lightness band of HLS image
	+ Cyan: Cyan band of CMYK image
	+ Magenta: Magenta band of CMYK image
	+ Yellow: Yellow band of CMYK image
	+ Black: Black band of CMYK image
	+ YLuminance: Y Luminance
	+ CbChroma: Cb Chroma
	+ CrChroma: Cr Chroma
)";

static constexpr const char *DOC_EXAMPLE_2 = R"(
	SELECT ST_GetBandColorInterpName(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_2[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStBandColorInterp(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandColorInterp");
	set.AddFunction(
	    ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::INTEGER, RasterGetColorInterpFunction));
	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_GetBandColorInterp", DOC_DESCRIPTION_1, DOC_EXAMPLE_1, DOC_TAGS_1);
}

void GdalScalarFunctions::RegisterStBandColorInterpName(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandColorInterpName");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::VARCHAR,
	                               RasterGetColorInterpNameFunction));
	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_GetBandColorInterpName", DOC_DESCRIPTION_2, DOC_EXAMPLE_2, DOC_TAGS_2);
}

} // namespace gdal

} // namespace spatial
