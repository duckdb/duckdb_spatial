#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/constants.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

#include <cmath>

namespace spatial {

namespace core {

static void GetQuadKey(double lon, double lat, int32_t level, char *buffer) {

	lat = std::max(-85.05112878, std::min(85.05112878, lat));
	lon = std::max(-180.0, std::min(180.0, lon));

	double lat_rad = lat * PI / 180.0;
	auto x = static_cast<int32_t>((lon + 180.0) / 360.0 * (1 << level));
	auto y =
	    static_cast<int32_t>((1.0 - std::log(std::tan(lat_rad) + 1.0 / std::cos(lat_rad)) / PI) / 2.0 * (1 << level));

	for (int i = level; i > 0; --i) {
		char digit = '0';
		int32_t mask = 1 << (i - 1);
		if ((x & mask) != 0) {
			digit += 1;
		}
		if ((y & mask) != 0) {
			digit += 2;
		}

		buffer[level - i] = digit;
	}
}
//------------------------------------------------------------------------------
// Coordinates
//------------------------------------------------------------------------------
static void CoordinateQuadKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lon_in = args.data[0];
	auto &lat_in = args.data[1];
	auto &level = args.data[2];
	auto count = args.size();

	TernaryExecutor::Execute<double, double, int32_t, string_t>(
	    lon_in, lat_in, level, result, count, [&](double lon, double lat, int32_t level) {
		    if (level < 1 || level > 23) {
			    throw InvalidInputException("ST_QuadKey: Level must be between 1 and 23");
		    }
		    char buffer[64];
		    GetQuadKey(lon, lat, level, buffer);
		    return StringVector::AddString(result, buffer, level);
	    });
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryQuadKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto &geom = args.data[0];
	auto &level = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<geometry_t, int32_t, string_t>(
	    geom, level, result, count, [&](geometry_t input, int32_t level) {
		    if (level < 1 || level > 23) {
			    throw InvalidInputException("ST_QuadKey: Level must be between 1 and 23");
		    }
		    if (input.GetType() != GeometryType::POINT) {
			    throw InvalidInputException("ST_QuadKey: Only POINT geometries are supported");
		    }
		    auto point = Geometry::Deserialize(arena, input);
		    if (Point::IsEmpty(point)) {
			    throw InvalidInputException("ST_QuadKey: Empty geometries are not supported");
		    }
		    auto vertex = Point::GetVertex(point);
		    char buffer[64];
		    GetQuadKey(vertex.x, vertex.y, level, buffer);
		    return StringVector::AddString(result, buffer, level);
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
Computes a quadkey from a given lon/lat point.

Compute the [quadkey](https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system) for a given lon/lat point at a given level.
Note that the the parameter order is __longitude__, __latitude__.

`level` has to be between 1 and 23, inclusive.

The input coordinates will be clamped to the lon/lat bounds of the earth (longitude between -180 and 180, latitude between -85.05112878 and 85.05112878).

Throws for any geometry that is not a `POINT`
)";

static constexpr const char *DOC_EXAMPLE = R"(
SELECT ST_QuadKey(st_point(11.08, 49.45), 10);
----
1333203202
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStQuadKey(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_QuadKey");

	set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER},
	                               LogicalType::VARCHAR, CoordinateQuadKeyFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::INTEGER}, LogicalType::VARCHAR,
	                               GeometryQuadKeyFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_QuadKey", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
