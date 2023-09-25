#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace core {

static void IntersectsExtentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t left, string_t right) {
		BoundingBox left_bbox;
		BoundingBox right_bbox;
		if (GeometryFactory::TryGetSerializedBoundingBox(left, left_bbox) &&
		    GeometryFactory::TryGetSerializedBoundingBox(right, right_bbox)) {
			return left_bbox.Intersects(right_bbox);
		}
		return false;
	});
}

void CoreScalarFunctions::RegisterStIntersectsExtent(DatabaseInstance &db) {
	ScalarFunction intersects_func("st_intersects_extent", {GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()},
	                               LogicalType::BOOLEAN, IntersectsExtentFunction);

	ExtensionUtil::RegisterFunction(db, intersects_func);

	// So because this is a macro, we cant add an alias to it. crap.
	// ScalarFunctionSet intersects_op("&&");
	// intersects_op.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN,
	// IntersectsExtentFunction)); CreateScalarFunctionInfo op_info(std::move(intersects_op)); op_info.on_conflict =
	// OnCreateConflict::IGNORE_ON_CONFLICT; catalog.CreateFunction(context, op_info);
}

} // namespace core

} // namespace spatial
