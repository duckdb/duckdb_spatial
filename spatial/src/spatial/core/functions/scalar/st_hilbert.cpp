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

//------------------------------------------------------------------------------
// Coordinates
//------------------------------------------------------------------------------
static void HilbertEncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &x_in = args.data[0];
	auto &y_in = args.data[1];
	auto &box_in = args.data[2];
	auto count = args.size();

	using DOUBLE_TYPE = PrimitiveType<double>;
	using UINT32_TYPE = PrimitiveType<uint32_t>;
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;

	auto max_hilbert = std::numeric_limits<uint16_t>::max();

	GenericExecutor::ExecuteTernary<DOUBLE_TYPE, DOUBLE_TYPE, BOX_TYPE, UINT32_TYPE>(
	    x_in, y_in, box_in, result, count, [&](DOUBLE_TYPE x, DOUBLE_TYPE y, BOX_TYPE &box) {
		    auto hilbert_width = max_hilbert / (box.c_val - box.a_val);
		    auto hilbert_height = max_hilbert / (box.d_val - box.b_val);
		    auto hilbert_x = static_cast<uint32_t>((x.val - box.a_val) * hilbert_width);
		    auto hilbert_y = static_cast<uint32_t>((y.val - box.b_val) * hilbert_height);
		    auto h = HilbertEncode(16, hilbert_x, hilbert_y);
		    return UINT32_TYPE {h};
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
static constexpr const char *DOC_DESCRIPTION = R"(
    Encodes the X and Y values as the hilbert curve index for a curve covering the given bounding box
)";
static constexpr const char *DOC_EXAMPLE = R"(

)";
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStHilbert(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Hilbert");
	set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, GeoTypes::BOX_2D()},
	                               LogicalType::UINTEGER, HilbertEncodeFunction));
	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Hilbert", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
