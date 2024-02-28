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

	BinaryExecutor::Execute<string_t, double, string_t>(
	    left, right, result, args.size(), [&](string_t &geometry_blob, double radius) {
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

	TernaryExecutor::Execute<string_t, double, int32_t, string_t>(
	    left, right, segments, result, args.size(), [&](string_t &geometry_blob, double radius, int32_t segments) {
		    auto geos_geom = lstate.ctx.Deserialize(geometry_blob);
		    auto boundary = make_uniq_geos(lstate.ctx.GetCtx(),
		                                   GEOSBuffer_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, segments));
		    return lstate.ctx.Serialize(result, boundary);
	    });
}

template<class T>
static T TryParseStringArgument(const char* name, const vector<string> &keys, const vector<T> &values, const string_t &arg) {
    D_ASSERT(keys.size() == values.size());
    for(idx_t i = 0; i < keys.size(); i++) {
        if(StringUtil::CIEquals(keys[i], arg.GetString())) {
            return values[i];
        }
    }

    auto candidates = StringUtil::Join(keys, ", ");
    throw InvalidInputException("Unknown %s: '%s', accepted inputs: %s", name, arg.GetString().c_str(), candidates.c_str());
}

static void BufferFunctionWithArgs(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

    SenaryExecutor::Execute<string_t, double, int32_t, string_t, string_t, double, string_t>(
            args, result,
            [&](const string_t &geometry_blob, double radius, int32_t segments, const string_t &cap_style_str, const string_t &join_style_str, double mitre_limit) {
                auto geos_geom = lstate.ctx.Deserialize(geometry_blob);

                auto cap_style = TryParseStringArgument<GEOSBufCapStyles>("cap style",
                                                                         {"CAP_ROUND", "CAP_FLAT", "CAP_SQUARE"},
                                                                         {GEOSBUF_CAP_ROUND, GEOSBUF_CAP_FLAT, GEOSBUF_CAP_SQUARE},
                                                                         cap_style_str);

                auto join_style = TryParseStringArgument<GEOSBufJoinStyles>("join style",
                                                                           {"JOIN_ROUND", "JOIN_MITRE", "JOIN_BEVEL"},
                                                                           {GEOSBUF_JOIN_ROUND, GEOSBUF_JOIN_MITRE, GEOSBUF_JOIN_BEVEL},
                                                                           join_style_str);

                auto buffer = GEOSBufferWithStyle_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, segments, cap_style, join_style, mitre_limit);
                auto buffer_ptr = make_uniq_geos(lstate.ctx.GetCtx(), buffer);
                return lstate.ctx.Serialize(result, buffer_ptr);
            });
}



void GEOSScalarFunctions::RegisterStBuffer(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Buffer");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), BufferFunction,
	                               nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::INTEGER},
	                               GeoTypes::GEOMETRY(), BufferFunctionWithSegments, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

    set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE},
                                   GeoTypes::GEOMETRY(), BufferFunctionWithArgs, nullptr, nullptr, nullptr,
                                   GEOSFunctionLocalState::Init));


	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
