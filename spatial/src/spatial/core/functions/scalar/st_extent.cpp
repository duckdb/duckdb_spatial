#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

#include <spatial/core/util/math.hpp>

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void ExtentFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto count = args.size();
	auto &input = args.data[0];
	auto &struct_vec = StructVector::GetEntries(result);
	auto min_x_data = FlatVector::GetData<double>(*struct_vec[0]);
	auto min_y_data = FlatVector::GetData<double>(*struct_vec[1]);
	auto max_x_data = FlatVector::GetData<double>(*struct_vec[2]);
	auto max_y_data = FlatVector::GetData<double>(*struct_vec[3]);

	UnifiedVectorFormat input_vdata;
	input.ToUnifiedFormat(count, input_vdata);
	auto input_data = reinterpret_cast<geometry_t *>(input_vdata.data);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = input_vdata.sel->get_index(i);
		if (input_vdata.validity.RowIsValid(row_idx)) {
			auto &blob = input_data[row_idx];

			// Try to get the cached bounding box from the blob
			Box2D<double> bbox;
			if (blob.TryGetCachedBounds(bbox)) {
				min_x_data[i] = bbox.min.x;
				min_y_data[i] = bbox.min.y;
				max_x_data[i] = bbox.max.x;
				max_y_data[i] = bbox.max.y;
			} else {
				// No bounding box, return null
				FlatVector::SetNull(result, i, true);
			}
		} else {
			// Null input, return null
			FlatVector::SetNull(result, i, true);
		}
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ExtentCachedFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto count = args.size();
	auto &input = args.data[0];
	auto &struct_vec = StructVector::GetEntries(result);
	auto min_x_data = FlatVector::GetData<float>(*struct_vec[0]);
	auto min_y_data = FlatVector::GetData<float>(*struct_vec[1]);
	auto max_x_data = FlatVector::GetData<float>(*struct_vec[2]);
	auto max_y_data = FlatVector::GetData<float>(*struct_vec[3]);

	UnifiedVectorFormat input_vdata;
	input.ToUnifiedFormat(count, input_vdata);
	const auto input_data = UnifiedVectorFormat::GetData<geometry_t>(input_vdata);

	for (idx_t i = 0; i < count; i++) {
		const auto row_idx = input_vdata.sel->get_index(i);
		if (input_vdata.validity.RowIsValid(row_idx)) {
			auto &blob = input_data[row_idx];

			// Try to get the cached bounding box from the blob
			Box2D<double> bbox;
			if (blob.TryGetCachedBounds(bbox)) {
				min_x_data[i] = MathUtil::DoubleToFloatDown(bbox.min.x);
				min_y_data[i] = MathUtil::DoubleToFloatDown(bbox.min.y);
				max_x_data[i] = MathUtil::DoubleToFloatUp(bbox.max.x);
				max_y_data[i] = MathUtil::DoubleToFloatUp(bbox.max.y);
			} else {
				// No bounding box, return null
				FlatVector::SetNull(result, i, true);
			}
		} else {
			// Null input, return null
			FlatVector::SetNull(result, i, true);
		}
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the minimal bounding box enclosing the input geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStExtent(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Extent");

	set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::BOX_2D(), ExtentFunction, nullptr, nullptr, nullptr));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Extent", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);

	ScalarFunctionSet approx_set("ST_Extent_Approx");
	approx_set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::BOX_2DF(), ExtentCachedFunction, nullptr, nullptr, nullptr));
	ExtensionUtil::RegisterFunction(db, approx_set);
}

} // namespace core

} // namespace spatial