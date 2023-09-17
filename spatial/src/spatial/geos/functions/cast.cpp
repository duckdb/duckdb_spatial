#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/functions/cast.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

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

void GeosCastFunctions::Register(DatabaseInstance &instance) {
	ExtensionUtil::RegisterCastFunction(instance, core::GeoTypes::WKB_BLOB(), LogicalType::VARCHAR, WKBToWKTCast);
	ExtensionUtil::RegisterCastFunction(instance, core::GeoTypes::GEOMETRY(), LogicalType::VARCHAR,
	                           BoundCastInfo(GeometryToTextCast, nullptr, GEOSFunctionLocalState::InitCast));
};

} // namespace geos

} // namespace spatial