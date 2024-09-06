#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/util/math.hpp"
#include "spatial/core/types.hpp"

#include <cmath>

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Hilbert Curve Encoding
// From (Public Domain): https://github.com/rawrunprotected/hilbert_curves
//------------------------------------------------------------------------------
inline uint32_t Interleave(uint32_t x) {
	x = (x | (x << 8)) & 0x00FF00FF;
	x = (x | (x << 4)) & 0x0F0F0F0F;
	x = (x | (x << 2)) & 0x33333333;
	x = (x | (x << 1)) & 0x55555555;
	return x;
}

inline uint32_t HilbertEncode(uint32_t n, uint32_t x, uint32_t y) {
	x = x << (16 - n);
	y = y << (16 - n);

	// Initial prefix scan round, prime with x and y
	uint32_t a = x ^ y;
	uint32_t b = 0xFFFF ^ a;
	uint32_t c = 0xFFFF ^ (x | y);
	uint32_t d = x & (y ^ 0xFFFF);
	uint32_t A = a | (b >> 1);
	uint32_t B = (a >> 1) ^ a;
	uint32_t C = ((c >> 1) ^ (b & (d >> 1))) ^ c;
	uint32_t D = ((a & (c >> 1)) ^ (d >> 1)) ^ d;

	a = A;
	b = B;
	c = C;
	d = D;
	A = ((a & (a >> 2)) ^ (b & (b >> 2)));
	B = ((a & (b >> 2)) ^ (b & ((a ^ b) >> 2)));
	C ^= ((a & (c >> 2)) ^ (b & (d >> 2)));
	D ^= ((b & (c >> 2)) ^ ((a ^ b) & (d >> 2)));

	a = A;
	b = B;
	c = C;
	d = D;
	A = ((a & (a >> 4)) ^ (b & (b >> 4)));
	B = ((a & (b >> 4)) ^ (b & ((a ^ b) >> 4)));
	C ^= ((a & (c >> 4)) ^ (b & (d >> 4)));
	D ^= ((b & (c >> 4)) ^ ((a ^ b) & (d >> 4)));

	// Final round and projection
	a = A;
	b = B;
	c = C;
	d = D;
	C ^= ((a & (c >> 8)) ^ (b & (d >> 8)));
	D ^= ((b & (c >> 8)) ^ ((a ^ b) & (d >> 8)));

	// Undo transformation prefix scan
	a = C ^ (C >> 1);
	b = D ^ (D >> 1);

	// Recover index bits
	uint32_t i0 = x ^ y;
	uint32_t i1 = b | (0xFFFF ^ (i0 | a));

	return ((Interleave(i1) << 1) | Interleave(i0)) >> (32 - 2 * n);
}

static uint32_t FloatToUint32(float f)
{
	if (std::isnan(f)) {
		return 0xFFFFFFFF;
	}
	uint32_t res;
	memcpy(&res, &f, sizeof(res));
	if((res & 0x80000000) != 0) {
		res ^= 0xFFFFFFFF;
	} else {
		res |= 0x80000000;
	}
	return res;
}

//------------------------------------------------------------------------------
// Coordinates
//------------------------------------------------------------------------------
static void HilbertEncodeCoordsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &x_in = args.data[0];
	auto &y_in = args.data[1];
	auto &box_in = args.data[2];

	const auto count = args.size();

	using DOUBLE_TYPE = PrimitiveType<double>;
	using UINT32_TYPE = PrimitiveType<uint32_t>;
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;

	auto constexpr max_hilbert = std::numeric_limits<uint16_t>::max();

	GenericExecutor::ExecuteTernary<DOUBLE_TYPE, DOUBLE_TYPE, BOX_TYPE, UINT32_TYPE>(
	    x_in, y_in, box_in, result, count, [&](DOUBLE_TYPE x, DOUBLE_TYPE y, BOX_TYPE &box) {
		    const auto hilbert_width = max_hilbert / (box.c_val - box.a_val);
		    const auto hilbert_height = max_hilbert / (box.d_val - box.b_val);

		    // TODO: Check for overflow
		    const auto hilbert_x = static_cast<uint32_t>((x.val - box.a_val) * hilbert_width);
		    const auto hilbert_y = static_cast<uint32_t>((y.val - box.b_val) * hilbert_height);
		    const auto h = HilbertEncode(16, hilbert_x, hilbert_y);
		    return UINT32_TYPE {h};
	    });
}

//------------------------------------------------------------------------------
// GEOMETRY (points)
//------------------------------------------------------------------------------
static void HilbertEncodeBoundsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vec = args.data[0];
	auto &bounds_vec = args.data[1];

	const auto count = args.size();

	auto constexpr max_hilbert = std::numeric_limits<uint16_t>::max();

	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using GEOM_TYPE = PrimitiveType<geometry_t>;
	using UINT32_TYPE = PrimitiveType<uint32_t>;

	GenericExecutor::ExecuteBinary<GEOM_TYPE, BOX_TYPE, UINT32_TYPE>(
	    input_vec, bounds_vec, result, count, [&](const GEOM_TYPE &geom_type, const BOX_TYPE &bounds) {
		    const auto geom = geom_type.val;

	    	Box2D<double> geom_bounds;
			if(!geom.TryGetCachedBounds(geom_bounds)) {
				throw InvalidInputException("ST_Hilbert(geom, bounds) requires that all geometries have a bounding box");
			}

			const auto dx = geom_bounds.min.x + (geom_bounds.max.x - geom_bounds.min.x) / 2;
			const auto dy = geom_bounds.min.y + (geom_bounds.max.y - geom_bounds.min.y) / 2;

		    const auto hilbert_width = max_hilbert / (bounds.c_val - bounds.a_val);
		    const auto hilbert_height = max_hilbert / (bounds.d_val - bounds.b_val);
		    // TODO: Check for overflow
		    const auto hilbert_x = static_cast<uint32_t>((dx - bounds.a_val) * hilbert_width);
		    const auto hilbert_y = static_cast<uint32_t>((dy - bounds.b_val) * hilbert_height);

		    const auto h = HilbertEncode(16, hilbert_x, hilbert_y);
		    return UINT32_TYPE {h};
	    });
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void HilbertEncodeGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input_vec = args.data[0];

	UnaryExecutor::ExecuteWithNulls<geometry_t, uint32_t>(
		input_vec, result, count, [&](const geometry_t &geom, ValidityMask &mask, idx_t out_idx) -> uint32_t {
			Box2D<double> bounds;
			if(!geom.TryGetCachedBounds(bounds)) {
				mask.SetInvalid(out_idx);
				return 0;
			}

			Box2D<float> bounds_f;
			bounds_f.min.x = MathUtil::DoubleToFloatDown(bounds.min.x);
			bounds_f.min.y = MathUtil::DoubleToFloatDown(bounds.min.y);
			bounds_f.max.x = MathUtil::DoubleToFloatUp(bounds.max.x);
			bounds_f.max.y = MathUtil::DoubleToFloatUp(bounds.max.y);

			const auto dx = bounds_f.min.x + (bounds_f.max.x - bounds_f.min.x) / 2;
			const auto dy = bounds_f.min.y + (bounds_f.max.y - bounds_f.min.y) / 2;

			const auto hx = FloatToUint32(dx);
			const auto hy = FloatToUint32(dy);

			return HilbertEncode(16, hx, hy);
		});
}


//------------------------------------------------------------------------------
// BOX_2D/BOX_2DF
//------------------------------------------------------------------------------
template <class T>
static void HilbertEncodeBoxFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vec = args.data[0];
	auto &bounds_vec = args.data[1];
	auto count = args.size();

	constexpr auto max_hilbert = std::numeric_limits<uint16_t>::max();

	using BOX_TYPE = StructTypeQuaternary<T, T, T, T>;
	using UINT32_TYPE = PrimitiveType<uint32_t>;

	GenericExecutor::ExecuteBinary<BOX_TYPE, BOX_TYPE, UINT32_TYPE>(
	    input_vec, bounds_vec, result, count, [&](BOX_TYPE &box, BOX_TYPE &bounds) {
		    const auto x = box.a_val + (box.c_val - box.a_val) / static_cast<T>(2);
		    const auto y = box.b_val + (box.d_val - box.b_val) / static_cast<T>(2);

		    const auto hilbert_width = max_hilbert / (bounds.c_val - bounds.a_val);
		    const auto hilbert_height = max_hilbert / (bounds.d_val - bounds.b_val);

		    // TODO: Check for overflow
		    const auto hilbert_x = static_cast<uint32_t>((x - bounds.a_val) * hilbert_width);
		    const auto hilbert_y = static_cast<uint32_t>((y - bounds.b_val) * hilbert_height);
		    const auto h = HilbertEncode(16, hilbert_x, hilbert_y);
		    return UINT32_TYPE {h};
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
static constexpr const char *DOC_DESCRIPTION = R"(
    Encodes the X and Y values as the hilbert curve index for a curve covering the given bounding box.

	Only POINT geometries are supported for the GEOMETRY variant.
	For the BOX_2D and BOX_2DF variants, the center of the box is used as the point to encode.
)";
static constexpr const char *DOC_EXAMPLE = R"(

)";
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStHilbert(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Hilbert");
	set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, GeoTypes::BOX_2D()},
	                               LogicalType::UINTEGER, HilbertEncodeCoordsFunction));
	set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::BOX_2D()}, LogicalType::UINTEGER, HilbertEncodeBoundsFunction));
	set.AddFunction(ScalarFunction({GeoTypes::BOX_2D(), GeoTypes::BOX_2D()}, LogicalType::UINTEGER,
	                               HilbertEncodeBoxFunction<double>));
	set.AddFunction(ScalarFunction({GeoTypes::BOX_2DF(), GeoTypes::BOX_2DF()}, LogicalType::UINTEGER,
	                               HilbertEncodeBoxFunction<float>));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::UINTEGER, HilbertEncodeGeometryFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Hilbert", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
