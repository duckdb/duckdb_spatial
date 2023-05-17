#include "spatial/common.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/types.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POINT(N) -> POINT_2D
//------------------------------------------------------------------------------
static bool ToPoint2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &children = StructVector::GetEntries(source);
	auto &x_child = children[0];
	auto &y_child = children[1];

	auto &result_children = StructVector::GetEntries(result);
	auto &result_x_child = result_children[0];
	auto &result_y_child = result_children[1];
	result_x_child->Reference(*x_child);
	result_y_child->Reference(*y_child);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreCastFunctions::RegisterDimensionalCasts(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	casts.RegisterCastFunction(GeoTypes::POINT_4D(), GeoTypes::POINT_2D(), ToPoint2DCast, 1);
	casts.RegisterCastFunction(GeoTypes::POINT_3D(), GeoTypes::POINT_2D(), ToPoint2DCast, 1);
}

} // namespace core

} // namespace spatial