#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void Point2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &point = args.data[0];
	auto &point_children = StructVector::GetEntries(point);
	auto &y_child = point_children[1];
	result.Reference(*y_child);
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::ExecuteWithNulls<string_t, double>(input, result, count, [&](string_t input,  ValidityMask &mask, idx_t idx) {
		if(mask.RowIsValid(idx)) {
			auto geometry = lstate.factory.Deserialize(input);
			if (geometry.Type() != GeometryType::POINT) {
				throw InvalidInputException("ST_Y only implemented for POINT geometries");
			}
			auto &point = geometry.GetPoint();
			if(point.IsEmpty()) {
				mask.SetInvalid(idx);
			} else {
				return point.GetVertex().y;
			}
		}
		return 0.0;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStY(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_y("st_y");
	st_y.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction));
	st_y.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(st_y));
	catalog.AddFunction(context, &info);
}

} // namespace core

} // namespace spatial
