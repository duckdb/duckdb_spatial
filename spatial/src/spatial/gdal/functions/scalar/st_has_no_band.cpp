#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_HasNoBand
//------------------------------------------------------------------------------

static void RasterHasNoBandFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	BinaryExecutor::Execute<uintptr_t, int32_t, bool>(args.data[0], args.data[1], result, args.size(),
	                                                  [&](uintptr_t input, int32_t band_num) {
		                                                  GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		                                                  return dataset->GetRasterCount() >= band_num;
	                                                  });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns true if there is no band with given band number.
	Band numbers start at 1 and band is assumed to be 1 if not specified.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT ST_HasNoBand(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetHasNoBand(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_HasNoBand");
	set.AddFunction(
	    ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::BOOLEAN, RasterHasNoBandFunction));
	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_HasNoBand", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
