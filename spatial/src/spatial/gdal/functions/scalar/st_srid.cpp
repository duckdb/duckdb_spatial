#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_SRID
//------------------------------------------------------------------------------

static void RasterGetSridFunction(DataChunk &args, ExpressionState &state, Vector &result) {

    UnaryExecutor::Execute<uintptr_t, int32_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
        Raster raster(reinterpret_cast<GDALDataset *>(input));
        return raster.GetSrid();
    });
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetSRID(DatabaseInstance &db) {

    ScalarFunctionSet set("ST_SRID");
    set.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetSridFunction));

    ExtensionUtil::RegisterFunction(db, set);
}

} // namespace gdal

} // namespace spatial
