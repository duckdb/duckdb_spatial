#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

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
	auto input_data = reinterpret_cast<string_t *>(input_vdata.data);

	BoundingBox bbox;

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = input_vdata.sel->get_index(i);
		if (input_vdata.validity.RowIsValid(row_idx)) {
			auto &blob = input_data[row_idx];

			// Try to get the cached bounding box from the blob
			if (GeometryFactory::TryGetSerializedBoundingBox(blob, bbox)) {
				min_x_data[i] = bbox.minx;
				min_y_data[i] = bbox.miny;
				max_x_data[i] = bbox.maxx;
				max_y_data[i] = bbox.maxy;
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

void CoreScalarFunctions::RegisterStExtent(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Extent");

	set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::BOX_2D(), ExtentFunction, nullptr, nullptr, nullptr));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial