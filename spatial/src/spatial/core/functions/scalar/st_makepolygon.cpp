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

static void MakePolygonFromRingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();

	auto &child_vec = ListVector::GetEntry(args.data[1]);
	UnifiedVectorFormat format;
	child_vec.ToUnifiedFormat(count, format);

	BinaryExecutor::Execute<string_t, list_entry_t, string_t>(
	    args.data[0], args.data[1], result, count, [&](string_t line_blob, list_entry_t &rings_list) {
		    // First, setup the shell
		    auto shell_geom = lstate.factory.Deserialize(line_blob);
		    if (shell_geom.Type() != GeometryType::LINESTRING) {
			    throw InvalidInputException("ST_MakePolygon only accepts LINESTRING geometries");
		    }

		    auto &shell = shell_geom.GetLineString();
		    auto shell_vert_count = shell.Count();
		    if (shell_vert_count < 4) {
			    throw InvalidInputException("ST_MakePolygon shell requires at least 4 vertices");
		    }

		    auto &shell_verts = shell.Vertices();
		    if (!shell_verts.IsClosed()) {
			    throw InvalidInputException(
			        "ST_MakePolygon shell must be closed (first and last vertex must be equal)");
		    }

		    // Validate and count the hole ring sizes
		    auto holes_offset = rings_list.offset;
		    auto holes_length = rings_list.length;

		    vector<uint32_t> rings_counts;
		    vector<LineString> rings;
		    rings_counts.push_back(shell_vert_count);
		    rings.push_back(shell);

		    for (idx_t hole_idx = 0; hole_idx < holes_length; hole_idx++) {
			    auto mapped_idx = format.sel->get_index(holes_offset + hole_idx);
			    if (!format.validity.RowIsValid(mapped_idx)) {
				    continue;
			    }

			    auto geometry_blob = UnifiedVectorFormat::GetData<string_t>(format)[mapped_idx];
			    auto hole_geometry = lstate.factory.Deserialize(geometry_blob);

			    if (hole_geometry.Type() != GeometryType::LINESTRING) {
				    throw InvalidInputException(
				        StringUtil::Format("ST_MakePolygon hole #%lu is not a LINESTRING geometry", hole_idx + 1));
			    }
			    auto &hole = hole_geometry.GetLineString();
			    auto hole_count = hole.Count();
			    if (hole_count < 4) {
				    throw InvalidInputException(
				        StringUtil::Format("ST_MakePolygon hole #%lu requires at least 4 vertices", hole_idx + 1));
			    }

			    auto &ring_verts = hole.Vertices();
			    if (!ring_verts.IsClosed()) {
				    throw InvalidInputException(StringUtil::Format(
				        "ST_MakePolygon hole #%lu must be closed (first and last vertex must be equal)", hole_idx + 1));
			    }

			    rings_counts.push_back(hole_count);
			    rings.push_back(hole);
		    }

		    auto polygon = lstate.factory.CreatePolygon(rings_counts.size(), rings_counts.data());

		    for (idx_t ring_idx = 0; ring_idx < rings.size(); ring_idx++) {
			    auto &new_ring = rings[ring_idx];
			    auto &poly_ring = polygon.Ring(ring_idx);

			    for (auto i = 0; i < new_ring.Vertices().Count(); i++) {
				    poly_ring.Add(new_ring.Vertices().Get(i));
			    }
		    }

		    return lstate.factory.Serialize(result, Geometry(polygon));
	    });
}

static void MakePolygonFromShellFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();

	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, count, [&](string_t &line_blob) {
		auto line_geom = lstate.factory.Deserialize(line_blob);

		if (line_geom.Type() != GeometryType::LINESTRING) {
			throw InvalidInputException("ST_MakePolygon only accepts LINESTRING geometries");
		}

		auto &line = line_geom.GetLineString();
		auto line_count = line.Count();
		if (line_count < 4) {
			throw InvalidInputException("ST_MakePolygon shell requires at least 4 vertices");
		}

		auto &line_verts = line.Vertices();
		if (!line_verts.IsClosed()) {
			throw InvalidInputException("ST_MakePolygon shell must be closed (first and last vertex must be equal)");
		}

		auto polygon = lstate.factory.CreatePolygon(1, &line_count);
		for (uint32_t i = 0; i < line_count; i++) {
			polygon.Shell().Add(line_verts.Get(i));
		}

		return lstate.factory.Serialize(result, Geometry(polygon));
	});
}

void CoreScalarFunctions::RegisterStMakePolygon(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_MakePolygon");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::LIST(GeoTypes::GEOMETRY())},
	                               GeoTypes::GEOMETRY(), MakePolygonFromRingsFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), MakePolygonFromShellFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
