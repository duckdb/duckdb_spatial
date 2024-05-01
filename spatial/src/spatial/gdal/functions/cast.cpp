#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/gdal/functions/cast.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// RASTER -> VARCHAR
//------------------------------------------------------------------------------

static bool RasterToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::Execute<uintptr_t, string_t>(source, result, count,
	                                            [&](uintptr_t &input) { return string_t("RASTER"); });
	return true;
}

//------------------------------------------------------------------------------
// RASTER -> GEOMETRY
//------------------------------------------------------------------------------

static bool RasterToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	UnaryExecutor::Execute<uintptr_t, geometry_t>(source, result, count, [&](uintptr_t &input) {
		Raster raster(reinterpret_cast<GDALDataset *>(input));
		return Geometry::Serialize(raster.GetGeometry(lstate.arena), result);
	});
	return true;
}

//------------------------------------------------------------------------------
// RASTER_COORD -> VARCHAR
//------------------------------------------------------------------------------

static bool RasterCoordToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	using COORD_TYPE = StructTypeBinary<int32_t, int32_t>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;

	GenericExecutor::ExecuteUnary<COORD_TYPE, VARCHAR_TYPE>(source, result, count, [&](COORD_TYPE &input) {
		auto col = input.a_val;
		auto row = input.b_val;
		return StringVector::AddString(result, StringUtil::Format("COORD (%d, %d)", col, row));
	});
	return true;
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void GdalCastFunctions::Register(DatabaseInstance &db) {

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::RASTER(), LogicalType::VARCHAR, RasterToVarcharCast, 1);

	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::RASTER(), GeoTypes::GEOMETRY(),
	    BoundCastInfo(RasterToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	// POINTER -> RASTER is implicitly castable
	ExtensionUtil::RegisterCastFunction(db, LogicalType::POINTER, GeoTypes::RASTER(), DefaultCasts::ReinterpretCast, 1);

	// RASTER -> POINTER is implicitly castable
	ExtensionUtil::RegisterCastFunction(db, GeoTypes::RASTER(), LogicalType::POINTER, DefaultCasts::ReinterpretCast, 1);

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::RASTER_COORD(), LogicalType::VARCHAR, RasterCoordToVarcharCast,
	                                    1);
};

} // namespace gdal

} // namespace spatial
