#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"


namespace spatial {

namespace geos {

using namespace spatial::core;

static void BufferFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];

	BinaryExecutor::Execute<string_t, double, string_t>(left, right, result, args.size(),
	                                                    [&](string_t &geometry_blob, double radius) {
		auto geometry = lstate.factory.Deserialize(geometry_blob);

		auto geos_geom = lstate.ctx.FromGeometry(geometry);
		auto boundary = geos_geom.Buffer(radius, 8);
		auto boundary_geometry = lstate.ctx.ToGeometry(lstate.factory, boundary.get());

		return lstate.factory.Serialize(result, boundary_geometry);
	});
}

static void BufferFunctionWithSegments(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto &segments = args.data[2];

	TernaryExecutor::Execute<string_t, double, int32_t, string_t>(left, right, segments, result, args.size(),
	                                                            [&](string_t &geometry_blob, double radius, int32_t segments) {
		auto geometry = lstate.factory.Deserialize(geometry_blob);

		auto geos_geom = lstate.ctx.FromGeometry(geometry);
		auto boundary = geos_geom.Buffer(radius, segments);
		auto boundary_geometry = lstate.ctx.ToGeometry(lstate.factory, boundary.get());

		return lstate.factory.Serialize(result, boundary_geometry);
	});
}

void GEOSScalarFunctions::RegisterStBuffer(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Buffer");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), BufferFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::INTEGER}, GeoTypes::GEOMETRY(), BufferFunctionWithSegments, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace spatials

} // namespace spatial
