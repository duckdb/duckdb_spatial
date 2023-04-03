#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

#include "ryu/ryu.h"

namespace spatial {

namespace core {

// This is not finished
// We need to hack into ryu to adapt it to our needs (e.g. we don't want to print trailing zeros)
static string PrintPoint(double x, double y) {
    char x_res[25];
    char y_res[25];

    // Doubles can safely round-trip through 15 decimal digits
    auto nx = d2fixed_buffered_n(x, 15, x_res);
    auto ny = d2fixed_buffered_n(y, 15, y_res);

    // Trim trailing zeros
    while (nx > 0 && x_res[nx - 1] == '0') {
        nx--;
    }
    while (ny > 0 && y_res[ny - 1] == '0') {
        ny--;
    }
    // Trim trailing decimal point
    if (nx > 0 && x_res[nx - 1] == '.') {
        nx--;
    }
    if (ny > 0 && y_res[ny - 1] == '.') {
        ny--;
    }
    x_res[nx] = '\0';
    y_res[ny] = '\0';
    return StringUtil::Format("%s %s", x_res, y_res);
}

//------------------------------------------------------------------------------
// POINT_2D -> VARCHAR
//------------------------------------------------------------------------------
static bool Point2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    using POINT_TYPE = StructTypeBinary<double, double>;
    using VARCHAR_TYPE = PrimitiveType<string_t>;

    GenericExecutor::ExecuteUnary<POINT_TYPE, VARCHAR_TYPE>(source, result, count, [&](POINT_TYPE &point) {
        return StringVector::AddString(result, StringUtil::Format("POINT (%s)", PrintPoint(point.a_val, point.b_val)));
    });
    return true;
}

//------------------------------------------------------------------------------
// BOX_2D -> VARCHAR
//------------------------------------------------------------------------------
static bool Box2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
    using VARCHAR_TYPE = PrimitiveType<string_t>;
    GenericExecutor::ExecuteUnary<BOX_TYPE, VARCHAR_TYPE>(source, result, count, [&](BOX_TYPE &point) {
        return StringVector::AddString(result, StringUtil::Format("BOX(%s, %s)", PrintPoint(point.a_val, point.b_val), PrintPoint(point.c_val, point.d_val)));
    });
    return true;
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreCastFunctions::RegisterVarcharCasts(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

    
    casts.RegisterCastFunction(GeoTypes::POINT_2D(), LogicalType::VARCHAR,
                                BoundCastInfo(Point2DToVarcharCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

    
    casts.RegisterCastFunction(GeoTypes::BOX_2D(), LogicalType::VARCHAR,
                                 BoundCastInfo(Box2DToVarcharCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
    
    // The others are implicitly cast to GEOMETRY and then to VARCHAR for now.
    // we should port all to use ryu/trimming later.
}

} // namespace core

} // namespace geo
