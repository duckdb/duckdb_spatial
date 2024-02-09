#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/functions/cast.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/error_data.hpp"

namespace spatial {

namespace geos {

static bool WKBToWKTCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKTWriter();
	writer.SetTrim(true);

	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t input) {
		auto geom = reader.Read(input);
		return writer.Write(geom, result);
	});

	return true;
}

static bool GeometryToTextCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(parameters);
	auto writer = lstate.ctx.CreateWKTWriter();
	writer.SetTrim(true);

	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t &wkt) {
		auto geom = lstate.ctx.Deserialize(wkt);
		return writer.Write(geom, result);
	});

	return true;
}

static bool TextToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(parameters);
	auto reader = lstate.ctx.CreateWKTReader();

	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t &wkt, ValidityMask &mask, idx_t idx) {
		    try {
			    auto geos_geom = reader.Read(wkt);
			    auto multidimensional = (GEOSHasZ_r(lstate.ctx.GetCtx(), geos_geom.get()) == 1);
			    if (multidimensional) {
				    throw InvalidInputException("3D/4D geometries are not supported");
			    }
			    return lstate.ctx.Serialize(result, geos_geom);
		    } catch (InvalidInputException &e) {
			    if (success) {
				    success = false;
				    ErrorData error(e);
				    HandleCastError::AssignError(error.RawMessage(), parameters.error_message);
			    }
			    mask.SetInvalid(idx);
			    return string_t();
		    }
	    });
	return success;
}

void GeosCastFunctions::Register(DatabaseInstance &db) {

	ExtensionUtil::RegisterCastFunction(db, core::GeoTypes::WKB_BLOB(), LogicalType::VARCHAR, WKBToWKTCast);
	ExtensionUtil::RegisterCastFunction(db, core::GeoTypes::GEOMETRY(), LogicalType::VARCHAR,
	                                    BoundCastInfo(GeometryToTextCast, nullptr, GEOSFunctionLocalState::InitCast));

	ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, core::GeoTypes::GEOMETRY(),
	                                    BoundCastInfo(TextToGeometryCast, nullptr, GEOSFunctionLocalState::InitCast));
};

} // namespace geos

} // namespace spatial