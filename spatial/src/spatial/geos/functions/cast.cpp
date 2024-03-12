#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/cast.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

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

void GeosCastFunctions::Register(DatabaseInstance &db) {
	ExtensionUtil::RegisterCastFunction(db, core::GeoTypes::WKB_BLOB(), LogicalType::VARCHAR, WKBToWKTCast);
}

} // namespace geos

} // namespace spatial