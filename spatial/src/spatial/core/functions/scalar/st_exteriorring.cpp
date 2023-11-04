#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonExteriorRingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &poly_vec = args.data[0];
	auto poly_entries = ListVector::GetData(poly_vec);
	auto &ring_vec = ListVector::GetEntry(poly_vec);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &vertex_vec = ListVector::GetEntry(ring_vec);
	auto &vertex_vec_children = StructVector::GetEntries(vertex_vec);
	auto poly_x_data = FlatVector::GetData<double>(*vertex_vec_children[0]);
	auto poly_y_data = FlatVector::GetData<double>(*vertex_vec_children[1]);

	auto count = args.size();
	UnifiedVectorFormat poly_format;
	poly_vec.ToUnifiedFormat(count, poly_format);

	// First figure out how many vertices we need
	idx_t total_vertex_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto row_idx = poly_format.sel->get_index(i);
		if (poly_format.validity.RowIsValid(row_idx)) {
			auto poly = poly_entries[row_idx];
			if (poly.length != 0) {
				// We only care about the exterior ring (first entry)
				auto &ring = ring_entries[poly.offset];
				total_vertex_count += ring.length;
			}
		}
	}

	// Now we can allocate the result vector
	auto &line_vec = result;
	ListVector::Reserve(line_vec, total_vertex_count);
	ListVector::SetListSize(line_vec, total_vertex_count);

	auto line_entries = ListVector::GetData(line_vec);
	auto &line_coord_vec = StructVector::GetEntries(ListVector::GetEntry(line_vec));
	auto line_data_x = FlatVector::GetData<double>(*line_coord_vec[0]);
	auto line_data_y = FlatVector::GetData<double>(*line_coord_vec[1]);

	// Now we can fill the result vector
	idx_t line_data_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto row_idx = poly_format.sel->get_index(i);
		if (poly_format.validity.RowIsValid(row_idx)) {
			auto poly = poly_entries[row_idx];

			if (poly.length == 0) {
				line_entries[i].offset = 0;
				line_entries[i].length = 0;
				continue;
			}

			// We only care about the exterior ring (first entry)
			auto &ring = ring_entries[poly.offset];

			auto &line_entry = line_entries[i];
			line_entry.offset = line_data_offset;
			line_entry.length = ring.length;

			for (idx_t coord_idx = 0; coord_idx < ring.length; coord_idx++) {
				line_data_x[line_entry.offset + coord_idx] = poly_x_data[ring.offset + coord_idx];
				line_data_y[line_entry.offset + coord_idx] = poly_y_data[ring.offset + coord_idx];
			}

			line_data_offset += ring.length;
		} else {
			FlatVector::SetNull(line_vec, i, true);
		}
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryExteriorRingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    input, result, count, [&](string_t input, ValidityMask &validity, idx_t idx) {
		    auto header = GeometryHeader::Get(input);
		    if (header.type != GeometryType::POLYGON) {
			    validity.SetInvalid(idx);
			    return string_t();
		    }

		    auto polygon = lstate.factory.Deserialize(input);
		    auto &poly = polygon.GetPolygon();
		    if (poly.IsEmpty()) {
			    return lstate.factory.Serialize(result, Geometry(lstate.factory.CreateEmptyLineString()));
		    }

		    auto &shell = poly.Shell();
		    auto num_points = shell.Count();

		    auto line = lstate.factory.CreateLineString(num_points);
		    for (uint32_t i = 0; i < num_points; i++) {
			    line.Vertices().Add(shell[i]);
		    }
		    return lstate.factory.Serialize(result, Geometry(line));
	    });
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStExteriorRing(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_ExteriorRing");
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, GeoTypes::LINESTRING_2D(), PolygonExteriorRingFunction));

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), GeometryExteriorRingFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial