#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_GetGeometry
//------------------------------------------------------------------------------

static void RasterGetGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

    UnaryExecutor::Execute<uintptr_t, geometry_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
        Raster raster(reinterpret_cast<GDALDataset *>(input));
        return lstate.factory.Serialize(result, raster.GetGeometry(lstate.factory), false, false);
    });
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetGeometry(DatabaseInstance &db) {

    ScalarFunctionSet set("ST_GetGeometry");
    set.AddFunction(ScalarFunction({GeoTypes::RASTER()}, GeoTypes::GEOMETRY(), RasterGetGeometryFunction,
                                   nullptr, nullptr, nullptr,
                                   GeometryFunctionLocalState::Init));

    ExtensionUtil::RegisterFunction(db, set);
}

} // namespace gdal

} // namespace spatial
