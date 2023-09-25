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
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineStringPointNFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto geom_vec = args.data[0];
	auto index_vec = args.data[1];
	auto count = args.size();
	UnifiedVectorFormat geom_format;
	geom_vec.ToUnifiedFormat(count, geom_format);
	UnifiedVectorFormat index_format;
	index_vec.ToUnifiedFormat(count, index_format);

	auto line_vertex_entries = ListVector::GetData(geom_vec);
	auto &line_vertex_vec = ListVector::GetEntry(geom_vec);
	auto &line_vertex_vec_children = StructVector::GetEntries(line_vertex_vec);
	auto line_x_data = FlatVector::GetData<double>(*line_vertex_vec_children[0]);
	auto line_y_data = FlatVector::GetData<double>(*line_vertex_vec_children[1]);

	auto &point_vertex_children = StructVector::GetEntries(result);
	auto point_x_data = FlatVector::GetData<double>(*point_vertex_children[0]);
	auto point_y_data = FlatVector::GetData<double>(*point_vertex_children[1]);

	auto index_data = FlatVector::GetData<int32_t>(index_vec);

	for (idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {

		auto in_row_idx = geom_format.sel->get_index(out_row_idx);
		auto in_idx_idx = index_format.sel->get_index(out_row_idx);
		if (geom_format.validity.RowIsValid(in_row_idx) && index_format.validity.RowIsValid(in_idx_idx)) {
			auto line = line_vertex_entries[in_row_idx];
			auto line_offset = line.offset;
			auto line_length = line.length;
			auto index = index_data[in_idx_idx];

			if (line_length == 0 || index == 0 || index < -static_cast<int64_t>(line_length) ||
			    index > static_cast<int64_t>(line_length)) {
				FlatVector::SetNull(result, out_row_idx, true);
				continue;
			}
			auto actual_index = index < 0 ? line_length + index : index - 1;
			point_x_data[out_row_idx] = line_x_data[line_offset + actual_index];
			point_y_data[out_row_idx] = line_y_data[line_offset + actual_index];
		} else {
			FlatVector::SetNull(result, out_row_idx, true);
		}
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}
//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryPointNFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &geom_vec = args.data[0];
	auto &index_vec = args.data[1];

	auto count = args.size();

	BinaryExecutor::ExecuteWithNulls<string_t, int32_t, string_t>(
	    geom_vec, index_vec, result, count, [&](string_t input, int32_t index, ValidityMask &mask, idx_t row_idx) {
		    auto header = GeometryHeader::Get(input);
		    if (header.type != GeometryType::LINESTRING) {
			    mask.SetInvalid(row_idx);
			    return string_t();
		    }

		    auto line = lstate.factory.Deserialize(input).GetLineString();
		    auto point_count = line.Count();

		    if (point_count == 0 || index == 0 || index < -static_cast<int64_t>(point_count) ||
		        index > static_cast<int64_t>(point_count)) {
			    mask.SetInvalid(row_idx);
			    return string_t();
		    }

		    auto actual_index = index < 0 ? point_count + index : index - 1;
		    auto &point = line.Vertices()[actual_index];
		    return lstate.factory.Serialize(result, Geometry(lstate.factory.CreatePoint(point.x, point.y)));
	    });
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStPointN(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_PointN");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::INTEGER}, GeoTypes::GEOMETRY(),
	                               GeometryPointNFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D(), LogicalType::INTEGER}, GeoTypes::POINT_2D(),
	                               LineStringPointNFunction));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
