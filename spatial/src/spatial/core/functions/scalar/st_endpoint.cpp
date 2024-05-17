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
static void LineStringEndPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto geom_vec = args.data[0];
	auto count = args.size();

	UnifiedVectorFormat geom_format;
	geom_vec.ToUnifiedFormat(count, geom_format);

	auto line_vertex_entries = ListVector::GetData(geom_vec);
	auto &line_vertex_vec = ListVector::GetEntry(geom_vec);
	auto &line_vertex_vec_children = StructVector::GetEntries(line_vertex_vec);
	auto line_x_data = FlatVector::GetData<double>(*line_vertex_vec_children[0]);
	auto line_y_data = FlatVector::GetData<double>(*line_vertex_vec_children[1]);

	auto &point_vertex_children = StructVector::GetEntries(result);
	auto point_x_data = FlatVector::GetData<double>(*point_vertex_children[0]);
	auto point_y_data = FlatVector::GetData<double>(*point_vertex_children[1]);

	for (idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {
		auto in_row_idx = geom_format.sel->get_index(out_row_idx);

		if (!geom_format.validity.RowIsValid(in_row_idx)) {
			FlatVector::SetNull(result, out_row_idx, true);
			continue;
		}

		auto line = line_vertex_entries[in_row_idx];
		auto line_offset = line.offset;
		auto line_length = line.length;

		if (line_length == 0) {
			FlatVector::SetNull(result, out_row_idx, true);
			continue;
		}

		point_x_data[out_row_idx] = line_x_data[line_offset + line_length - 1];
		point_y_data[out_row_idx] = line_y_data[line_offset + line_length - 1];
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}
//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryEndPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &geom_vec = args.data[0];
	auto count = args.size();

	UnaryExecutor::ExecuteWithNulls<geometry_t, geometry_t>(
	    geom_vec, result, count, [&](geometry_t input, ValidityMask &mask, idx_t row_idx) {
		    if (input.GetType() != GeometryType::LINESTRING) {
			    mask.SetInvalid(row_idx);
			    return geometry_t {};
		    }

		    auto line = Geometry::Deserialize(lstate.arena, input);
		    auto point_count = LineString::VertexCount(line);

		    if (point_count == 0) {
			    mask.SetInvalid(row_idx);
			    return geometry_t {};
		    }

		    auto point = LineString::GetPointAsReference(line, point_count - 1);
		    return Geometry::Serialize(point, result);
	    });
}
//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
Returns the end point of a line.
)";

static constexpr const char *DOC_EXAMPLE = R"(
select st_endpoint('LINESTRING(0 0, 1 1)'::geometry);
-- POINT(1 1)
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStEndPoint(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_EndPoint");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), GeometryEndPointFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, GeoTypes::POINT_2D(), LineStringEndPointFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_EndPoint", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
