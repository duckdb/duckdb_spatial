#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void DescribeFunction(DataChunk &args, ExpressionState &, Vector &result) {

	auto count = args.size();
	auto &input = args.data[0];
	auto &children = StructVector::GetEntries(result);
	auto type_data = FlatVector::GetData<uint8_t>(*children[0]);
	auto has_m_data = FlatVector::GetData<bool>(*children[1]);
	auto has_z_data = FlatVector::GetData<bool>(*children[2]);
	auto has_bbox_data = FlatVector::GetData<bool>(*children[3]);
	auto size_data = FlatVector::GetData<uint32_t>(*children[4]);

	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
	auto input_data = (string_t *)input.GetData();

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = format.sel->get_index(i);
		if (format.validity.RowIsValid(row_idx)) {
			auto &blob = input_data[row_idx];
			auto header = GeometryHeader::Get(blob);
			type_data[i] = (uint8_t)header.type;
			has_m_data[i] = header.properties.HasM();
			has_z_data[i] = header.properties.HasZ();
			has_bbox_data[i] = header.properties.HasBBox();
			size_data[i] = static_cast<uint32_t>(blob.GetSize());
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void CoreScalarFunctions::RegisterStDescribe(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Describe");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()},
	                               LogicalType::STRUCT({
	                                   {"type", LogicalType::UTINYINT},
	                                   {"has_m", LogicalType::BOOLEAN},
	                                   {"has_z", LogicalType::BOOLEAN},
	                                   {"has_bbox", LogicalType::BOOLEAN},
	                                   {"size", LogicalType::UINTEGER},
	                               }),
	                               DescribeFunction));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace core

} // namespace spatial