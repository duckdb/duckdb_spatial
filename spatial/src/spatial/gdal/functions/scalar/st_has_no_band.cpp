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

	BinaryExecutor::Execute<uintptr_t, int32_t, bool>(args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		return dataset->GetRasterCount() >= band_num;
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetHasNoBand(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_HasNoBand");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::BOOLEAN, RasterHasNoBandFunction));
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace gdal

} // namespace spatial
