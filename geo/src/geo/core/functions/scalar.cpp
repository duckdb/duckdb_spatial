#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions/scalar.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace geo {

namespace core {

using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
using AREA_TYPE = PrimitiveType<double>;

static void Box2DAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &left = args.data[0];

	GenericExecutor::ExecuteUnary<BOX_TYPE, AREA_TYPE>(
	    left, result, count, [&](BOX_TYPE left) { return (left.c_val - left.a_val) * (left.d_val - left.b_val); });
}


void CoreScalarFunctions::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	// ST_AREA
	ScalarFunctionSet area_set("st_area");
	area_set.AddFunction(ScalarFunction({GeoTypes::BOX_2D}, LogicalType::DOUBLE, Box2DAreaFunction));
	CreateScalarFunctionInfo area_info(std::move(area_set));
	area_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &area_info);
}

} // namespace core

} // namespace geo