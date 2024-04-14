#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/senary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void BufferFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];

	BinaryExecutor::Execute<geometry_t, double, geometry_t>(
	    left, right, result, args.size(), [&](geometry_t &geometry_blob, double radius) {
		    auto geos_geom = lstate.ctx.Deserialize(geometry_blob);
		    auto boundary =
		        make_uniq_geos(lstate.ctx.GetCtx(), GEOSBuffer_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, 8));
		    return lstate.ctx.Serialize(result, boundary);
	    });
}

static void BufferFunctionWithSegments(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto &segments = args.data[2];

	TernaryExecutor::Execute<geometry_t, double, int32_t, geometry_t>(
	    left, right, segments, result, args.size(), [&](geometry_t &geometry_blob, double radius, int32_t segments) {
		    auto geos_geom = lstate.ctx.Deserialize(geometry_blob);
		    auto boundary = make_uniq_geos(lstate.ctx.GetCtx(),
		                                   GEOSBuffer_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, segments));
		    return lstate.ctx.Serialize(result, boundary);
	    });
}

template <class T>
static T TryParseStringArgument(const char *name, const vector<string> &keys, const vector<T> &values,
                                const string_t &arg) {
	D_ASSERT(keys.size() == values.size());
	for (idx_t i = 0; i < keys.size(); i++) {
		if (StringUtil::CIEquals(keys[i], arg.GetString())) {
			return values[i];
		}
	}

	auto candidates = StringUtil::Join(keys, ", ");
	throw InvalidInputException("Unknown %s: '%s', accepted inputs: %s", name, arg.GetString().c_str(),
	                            candidates.c_str());
}

static void BufferFunctionWithArgs(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	SenaryExecutor::Execute<geometry_t, double, int32_t, string_t, string_t, double, geometry_t>(
	    args, result,
	    [&](const geometry_t &geometry_blob, double radius, int32_t segments, const string_t &cap_style_str,
	        const string_t &join_style_str, double mitre_limit) {
		    auto geos_geom = lstate.ctx.Deserialize(geometry_blob);

		    auto cap_style = TryParseStringArgument<GEOSBufCapStyles>(
		        "cap style", {"CAP_ROUND", "CAP_FLAT", "CAP_SQUARE"},
		        {GEOSBUF_CAP_ROUND, GEOSBUF_CAP_FLAT, GEOSBUF_CAP_SQUARE}, cap_style_str);

		    auto join_style = TryParseStringArgument<GEOSBufJoinStyles>(
		        "join style", {"JOIN_ROUND", "JOIN_MITRE", "JOIN_BEVEL"},
		        {GEOSBUF_JOIN_ROUND, GEOSBUF_JOIN_MITRE, GEOSBUF_JOIN_BEVEL}, join_style_str);

		    auto buffer = GEOSBufferWithStyle_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, segments, cap_style,
		                                        join_style, mitre_limit);
		    auto buffer_ptr = make_uniq_geos(lstate.ctx.GetCtx(), buffer);
		    return lstate.ctx.Serialize(result, buffer_ptr);
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns a buffer around the input geometry at the target distance

    `geom` is the input geometry.

    `distance` is the target distance for the buffer, using the same units as the input geometry.

    `num_triangles` represents how many triangles that will be produced to approximate a quarter circle. The larger the number, the smoother the resulting geometry. The default value is 8.

    `join_style` must be one of "JOIN_ROUND", "JOIN_MITRE", "JOIN_BEVEL". This parameter is case-insensitive.

    `cap_style` must be one of "CAP_ROUND", "CAP_FLAT", "CAP_SQUARE". This parameter is case-insensitive.

    `mite_limit` only applies when `join_style` is "JOIN_MITRE". It is the ratio of the distance from the corner to the miter point to the corner radius. The default value is 1.0.

    This is a planar operation and will not take into account the curvature of the earth.
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStBuffer(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Buffer");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), BufferFunction,
	                               nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::INTEGER},
	                               GeoTypes::GEOMETRY(), BufferFunctionWithSegments, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::INTEGER,
	                                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE},
	                               GeoTypes::GEOMETRY(), BufferFunctionWithArgs, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Buffer", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
