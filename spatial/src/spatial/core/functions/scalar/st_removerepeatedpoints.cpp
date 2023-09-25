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
static void LineStringRemoveRepeatedPointsFunctions(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);

	auto in_line_entries = ListVector::GetData(input);
	auto &in_line_vertex_vec = StructVector::GetEntries(ListVector::GetEntry(input));
	auto in_x_data = FlatVector::GetData<double>(*in_line_vertex_vec[0]);
	auto in_y_data = FlatVector::GetData<double>(*in_line_vertex_vec[1]);

	auto out_line_entries = ListVector::GetData(result);
	auto &out_line_vertex_vec = StructVector::GetEntries(ListVector::GetEntry(result));

	idx_t out_offset = 0;
	for (idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {

		auto in_row_idx = format.sel->get_index(out_row_idx);
		if (!format.validity.RowIsValid(in_row_idx)) {
			FlatVector::SetNull(result, out_row_idx, true);
			continue;
		}
		auto in = in_line_entries[in_row_idx];
		auto in_offset = in.offset;
		auto in_length = in.length;

		// Special case: if the line has less than 3 points, we can't remove any points
		if (in_length < 3) {

			ListVector::Reserve(result, out_offset + in_length);
			auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
			auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);

			// If the line has less than 3 points, we can't remove any points
			// so we just copy the line
			out_line_entries[out_row_idx] = list_entry_t {out_offset, in_length};
			for (idx_t coord_idx = 0; coord_idx < in_length; coord_idx++) {
				out_x_data[out_offset + coord_idx] = in_x_data[in_offset + coord_idx];
				out_y_data[out_offset + coord_idx] = in_y_data[in_offset + coord_idx];
			}
			out_offset += in_length;
			continue;
		}

		// First pass, calculate how many points we need to keep
		// We always keep the first and last point, so we start at 2
		uint32_t points_to_keep = 0;

		auto last_x = in_x_data[in_offset];
		auto last_y = in_y_data[in_offset];
		points_to_keep++;

		for (idx_t i = 1; i < in_length; i++) {
			auto curr_x = in_x_data[in_offset + i];
			auto curr_y = in_y_data[in_offset + i];

			if (curr_x != last_x || curr_y != last_y) {
				points_to_keep++;
				last_x = curr_x;
				last_y = curr_y;
			}
		}

		// Special case: there is only 1 unique point in the line, so just keep
		// the start and end points
		if (points_to_keep == 1) {
			out_line_entries[out_row_idx] = list_entry_t {out_offset, 2};
			ListVector::Reserve(result, out_offset + 2);
			auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
			auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);
			out_x_data[out_offset] = in_x_data[in_offset];
			out_y_data[out_offset] = in_y_data[in_offset];
			out_x_data[out_offset + 1] = in_x_data[in_offset + in_length - 1];
			out_y_data[out_offset + 1] = in_y_data[in_offset + in_length - 1];
			out_offset += 2;
			continue;
		}

		// Set the list entry
		out_line_entries[out_row_idx] = list_entry_t {out_offset, points_to_keep};

		// Second pass, copy the points we need to keep
		ListVector::Reserve(result, out_offset + points_to_keep);
		auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
		auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);

		// Copy the first point
		out_x_data[out_offset] = in_x_data[in_offset];
		out_y_data[out_offset] = in_y_data[in_offset];
		out_offset++;

		// Copy the middle points (skip the last one, we'll copy it at the end)
		last_x = in_x_data[in_offset];
		last_y = in_y_data[in_offset];

		for (idx_t i = 1; i < in_length; i++) {
			auto curr_x = in_x_data[in_offset + i];
			auto curr_y = in_y_data[in_offset + i];

			if (curr_x != last_x || curr_y != last_y) {
				out_x_data[out_offset] = curr_x;
				out_y_data[out_offset] = curr_y;
				last_x = curr_x;
				last_y = curr_y;
				out_offset++;
			}
		}
	}
	ListVector::SetListSize(result, out_offset);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void LineStringRemoveRepeatedPointsFunctionsWithTolerance(DataChunk &args, ExpressionState &state,
                                                                 Vector &result) {
	auto input = args.data[0];
	auto tolerance = args.data[1];
	auto count = args.size();
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);

	UnifiedVectorFormat tolerance_format;
	tolerance.ToUnifiedFormat(count, tolerance_format);

	auto in_line_entries = ListVector::GetData(input);
	auto &in_line_vertex_vec = StructVector::GetEntries(ListVector::GetEntry(input));
	auto in_x_data = FlatVector::GetData<double>(*in_line_vertex_vec[0]);
	auto in_y_data = FlatVector::GetData<double>(*in_line_vertex_vec[1]);

	auto out_line_entries = ListVector::GetData(result);
	auto &out_line_vertex_vec = StructVector::GetEntries(ListVector::GetEntry(result));

	idx_t out_offset = 0;

	for (idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {
		auto in_row_idx = format.sel->get_index(out_row_idx);
		auto in_tol_idx = tolerance_format.sel->get_index(out_row_idx);
		if (!format.validity.RowIsValid(in_row_idx) || !tolerance_format.validity.RowIsValid(in_tol_idx)) {
			FlatVector::SetNull(result, out_row_idx, true);
			continue;
		}

		auto in = in_line_entries[in_row_idx];
		auto in_offset = in.offset;
		auto in_length = in.length;

		auto tolerance = Load<double>(tolerance_format.data + in_tol_idx);
		auto tolerance_squared = tolerance * tolerance;

		if (in_length < 3) {

			ListVector::Reserve(result, out_offset + in_length);
			auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
			auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);

			// If the line has less than 3 points, we can't remove any points
			// so we just copy the line
			out_line_entries[out_row_idx] = list_entry_t {out_offset, in_length};
			for (idx_t coord_idx = 0; coord_idx < in_length; coord_idx++) {
				out_x_data[out_offset + coord_idx] = in_x_data[in_offset + coord_idx];
				out_y_data[out_offset + coord_idx] = in_y_data[in_offset + coord_idx];
			}
			out_offset += in_length;
			continue;
		}

		// First pass, calculate how many points we need to keep
		uint32_t points_to_keep = 0;

		auto last_x = in_x_data[in_offset];
		auto last_y = in_y_data[in_offset];
		points_to_keep++;

		for (idx_t i = 1; i < in_length; i++) {
			auto curr_x = in_x_data[in_offset + i];
			auto curr_y = in_y_data[in_offset + i];

			auto dist_squared = (curr_x - last_x) * (curr_x - last_x) + (curr_y - last_y) * (curr_y - last_y);

			if (dist_squared > tolerance_squared) {
				last_x = curr_x;
				last_y = curr_y;
				points_to_keep++;
			}
		}

		// Special case: there is only 1 unique point in the line, so just keep
		// the start and end points
		if (points_to_keep == 1) {
			out_line_entries[out_row_idx] = list_entry_t {out_offset, 2};
			ListVector::Reserve(result, out_offset + 2);
			auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
			auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);
			out_x_data[out_offset] = in_x_data[in_offset];
			out_y_data[out_offset] = in_y_data[in_offset];
			out_x_data[out_offset + 1] = in_x_data[in_offset + in_length - 1];
			out_y_data[out_offset + 1] = in_y_data[in_offset + in_length - 1];
			out_offset += 2;
			continue;
		}

		// Set the list entry
		out_line_entries[out_row_idx] = list_entry_t {out_offset, points_to_keep};

		// Second pass, copy the points we need to keep
		ListVector::Reserve(result, out_offset + points_to_keep);
		auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
		auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);

		// Copy the first point
		out_x_data[out_offset] = in_x_data[in_offset];
		out_y_data[out_offset] = in_y_data[in_offset];
		out_offset++;

		// With tolerance its different, we always keep the first and last point
		// regardless of distance to the previous point
		// Copy the middle points
		last_x = in_x_data[in_offset];
		last_y = in_y_data[in_offset];

		for (idx_t i = 1; i < in_length - 1; i++) {

			auto curr_x = in_x_data[in_offset + i];
			auto curr_y = in_y_data[in_offset + i];

			auto dist_squared = (curr_x - last_x) * (curr_x - last_x) + (curr_y - last_y) * (curr_y - last_y);
			if (dist_squared > tolerance_squared) {
				out_x_data[out_offset] = curr_x;
				out_y_data[out_offset] = curr_y;
				last_x = curr_x;
				last_y = curr_y;
				out_offset++;
			}
		}

		// Copy the last point
		out_x_data[points_to_keep - 1] = in_x_data[in_offset + in_length - 1];
		out_y_data[points_to_keep - 1] = in_y_data[in_offset + in_length - 1];
		out_offset++;
	}
	ListVector::SetListSize(result, out_offset);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStRemoveRepeatedPoints(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_RemoveRepeatedPoints");

	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, GeoTypes::LINESTRING_2D(),
	                               LineStringRemoveRepeatedPointsFunctions));

	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D(), LogicalType::DOUBLE}, GeoTypes::LINESTRING_2D(),
	                               LineStringRemoveRepeatedPointsFunctionsWithTolerance));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
