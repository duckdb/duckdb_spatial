#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace spatial {

namespace core {

static void IntersectsBox2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using BOOL_TYPE = PrimitiveType<bool>;

	GenericExecutor::ExecuteBinary<BOX_TYPE, BOX_TYPE, BOOL_TYPE>(
	    args.data[0], args.data[1], result, args.size(), [&](BOX_TYPE &left, BOX_TYPE &right) {
		    return !(left.a_val > right.c_val || left.c_val < right.a_val || left.b_val > right.d_val ||
		             left.d_val < right.b_val);
	    });
}

void CoreScalarFunctions::RegisterStIntersects(DatabaseInstance &db) {
	ScalarFunction intersects_func("ST_Intersects", {GeoTypes::BOX_2D(), GeoTypes::BOX_2D()}, LogicalType::BOOLEAN,
	                               IntersectsBox2DFunction);

	ExtensionUtil::RegisterFunction(db, intersects_func);
}

} // namespace core

} // namespace spatial
