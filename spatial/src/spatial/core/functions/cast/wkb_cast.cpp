#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// WKB -> GEOMETRY
//------------------------------------------------------------------------------
static bool WKBToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	UnaryExecutor::Execute<string_t, geometry_t>(source, result, count, [&](string_t input) {
		auto geometry = lstate.factory.FromWKB(input.GetDataUnsafe(), input.GetSize());
		return lstate.factory.Serialize(result, geometry);
	});
	return true;
}

//------------------------------------------------------------------------------
// GEOMETRY -> WKB
//------------------------------------------------------------------------------
static bool GeometryToWKBCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	UnaryExecutor::Execute<geometry_t, string_t>(source, result, count, [&](geometry_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		auto size = WKBWriter::GetRequiredSize(geometry);
		auto str = StringVector::EmptyString(result, size);
		auto ptr = (data_ptr_t)(str.GetDataUnsafe());
		WKBWriter::Write(geometry, ptr);
		return str;
	});

	return true;
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreCastFunctions::RegisterWKBCasts(DatabaseInstance &db) {
	// Geometry <-> WKB is explicitly castable
	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::GEOMETRY(), GeoTypes::WKB_BLOB(),
	    BoundCastInfo(GeometryToWKBCast, nullptr, GeometryFunctionLocalState::InitCast));

	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::WKB_BLOB(), GeoTypes::GEOMETRY(),
	    BoundCastInfo(WKBToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast));

	// WKB -> BLOB is implicitly castable
	ExtensionUtil::RegisterCastFunction(db, GeoTypes::WKB_BLOB(), LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);

	// Geometry -> BLOB is implicitly castable
	ExtensionUtil::RegisterCastFunction(db, GeoTypes::GEOMETRY(), LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
}

} // namespace core

} // namespace spatial