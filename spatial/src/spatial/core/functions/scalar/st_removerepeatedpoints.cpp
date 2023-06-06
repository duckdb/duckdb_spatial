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

        if(in_length < 3) {
            
            ListVector::Reserve(result, out_offset + in_length);
            auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
            auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);

            // If the line has less than 3 points, we can't remove any points
            // so we just copy the line
            out_line_entries[out_row_idx] = list_entry_t{out_offset, in_length};
            for (idx_t coord_idx = 0; coord_idx < in_length; coord_idx++) {
                out_x_data[out_offset + coord_idx] = in_x_data[in_offset + coord_idx];
                out_y_data[out_offset + coord_idx] = in_y_data[in_offset + coord_idx];
            }
            out_offset += in_length;
            continue;
        }

        // First pass, calculate how many points we need to keep
        // We always keep the first and last point, so we start at 2
        uint32_t points_to_keep = 2; 
    
        for (idx_t i = 1; i < in_length - 2; i++) {
            auto x1 = in_x_data[in_offset + i];
            auto y1 = in_y_data[in_offset + i];
            auto x2 = in_x_data[in_offset + i + 1];
            auto y2 = in_y_data[in_offset + i + 1];

            if (x1 != x2 || y1 != y2) {
                points_to_keep++;
            }
        }
        
        // Set the list entry
        out_line_entries[out_row_idx] = list_entry_t{out_offset, points_to_keep};

        // Second pass, copy the points we need to keep
        ListVector::Reserve(result, out_offset + points_to_keep);
        auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
        auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);
        
        // Copy the first point
        out_x_data[out_offset] = in_x_data[in_offset];
        out_y_data[out_offset] = in_y_data[in_offset];
        out_offset++;

        // Copy the points in the middle
        for (idx_t i = 1; i < in_length - 2; i++) {
            auto x1 = in_x_data[in_offset + i];
            auto y1 = in_y_data[in_offset + i];
            auto x2 = in_x_data[in_offset + i + 1];
            auto y2 = in_y_data[in_offset + i + 1];

            if (x1 != x2 || y1 != y2) {
                out_x_data[out_offset] = x1;
                out_y_data[out_offset] = y1;
                out_offset++;
            }
        }

        // Copy the last point
        out_x_data[out_offset] = in_x_data[in_offset + in_length - 1];
        out_y_data[out_offset] = in_y_data[in_offset + in_length - 1];
        out_offset++;
	}
    ListVector::SetListSize(result, out_offset);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}


static void LineStringRemoveRepeatedPointsFunctionsWithTolerance(DataChunk &args, ExpressionState &state, Vector &result) {
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

        if(in_length < 3) {
            
            ListVector::Reserve(result, out_offset + in_length);
            auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
            auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);

            // If the line has less than 3 points, we can't remove any points
            // so we just copy the line
            out_line_entries[out_row_idx] = list_entry_t{out_offset, in_length};
            for (idx_t coord_idx = 0; coord_idx < in_length; coord_idx++) {
                out_x_data[out_offset + coord_idx] = in_x_data[in_offset + coord_idx];
                out_y_data[out_offset + coord_idx] = in_y_data[in_offset + coord_idx];
            }
            out_offset += in_length;
            continue;
        }

        // First pass, calculate how many points we need to keep
        // We always keep the first and last point, so we start at 2
        uint32_t points_to_keep = 2; 
    
        for (idx_t i = 1; i < in_length - 2; i++) {
            auto x1 = in_x_data[in_offset + i];
            auto y1 = in_y_data[in_offset + i];
            auto x2 = in_x_data[in_offset + i + 1];
            auto y2 = in_y_data[in_offset + i + 1];

            auto dist_squared = (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2);
            if (dist_squared > tolerance_squared) {
                points_to_keep++;
            }
        }
        
        // Set the list entry
        out_line_entries[out_row_idx] = list_entry_t{out_offset, points_to_keep};

        // Second pass, copy the points we need to keep
        ListVector::Reserve(result, out_offset + points_to_keep);
        auto out_x_data = FlatVector::GetData<double>(*out_line_vertex_vec[0]);
        auto out_y_data = FlatVector::GetData<double>(*out_line_vertex_vec[1]);
        
        // Copy the first point
        out_x_data[out_offset] = in_x_data[in_offset];
        out_y_data[out_offset] = in_y_data[in_offset];
        out_offset++;

        // Copy the points in the middle
        for (idx_t i = 1; i < in_length - 2; i++) {
            auto x1 = in_x_data[in_offset + i];
            auto y1 = in_y_data[in_offset + i];
            auto x2 = in_x_data[in_offset + i + 1];
            auto y2 = in_y_data[in_offset + i + 1];

            auto dist_squared = (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2);
            if(dist_squared > tolerance_squared) {
                out_x_data[out_offset] = x1;
                out_y_data[out_offset] = y1;
                out_offset++;
            }
        }
        
        // Copy the last point
        out_x_data[out_offset] = in_x_data[in_offset + in_length - 1];
        out_y_data[out_offset] = in_y_data[in_offset + in_length - 1];
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
void CoreScalarFunctions::RegisterStRemoveRepeatedPoints(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_RemoveRepeatedPoints");

    set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, GeoTypes::LINESTRING_2D(), 
        LineStringRemoveRepeatedPointsFunctions));

    set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D(), LogicalType::DOUBLE}, GeoTypes::LINESTRING_2D(), 
        LineStringRemoveRepeatedPointsFunctionsWithTolerance));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace core

} // namespace spatial
