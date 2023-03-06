#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/geos/functions/cast.hpp"
#include "geo/geos/geos_wrappers.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"

namespace geo {

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

void GeosCastFunctions::Register(ClientContext &context) {

	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	casts.RegisterCastFunction(core::GeoTypes::WKB_BLOB, LogicalType::VARCHAR, WKBToWKTCast);

};

} // namespace geos

} // namespace geo