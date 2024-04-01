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
	auto &arena = lstate.arena;
	auto count = args.size();

	auto &child_vec = ListVector::GetEntry(args.data[1]);
	UnifiedVectorFormat format;
	child_vec.ToUnifiedFormat(count, format);

	BinaryExecutor::Execute<geometry_t, list_entry_t, geometry_t>(
	    args.data[0], args.data[1], result, count, [&](geometry_t line_blob, list_entry_t &rings_list) {
		    // First, setup the shell
		    if (line_blob.GetType() != GeometryType::LINESTRING) {
			    throw InvalidInputException("ST_MakePolygon only accepts LINESTRING geometries");
		    }

		    // TODO: Support Z and M
		    if (line_blob.GetProperties().HasM() || line_blob.GetProperties().HasZ()) {
			    throw InvalidInputException("ST_MakePolygon does not support Z or M geometries");
		    }

		    auto shell_geom = Geometry::Deserialize(arena, line_blob);
		    auto &shell = shell_geom.As<LineString>();
		    if (shell.Count() < 4) {
			    throw InvalidInputException("ST_MakePolygon shell requires at least 4 vertices");
		    }

		    if (!shell.IsClosed()) {
			    throw InvalidInputException(
			        "ST_MakePolygon shell must be closed (first and last vertex must be equal)");
		    }

		    // Validate and count the hole ring sizes
		    auto holes_offset = rings_list.offset;
		    auto holes_length = rings_list.length;

		    vector<LineString> rings;
		    rings.push_back(shell);

		    for (idx_t hole_idx = 0; hole_idx < holes_length; hole_idx++) {
			    auto mapped_idx = format.sel->get_index(holes_offset + hole_idx);
			    if (!format.validity.RowIsValid(mapped_idx)) {
				    continue;
			    }

			    auto geometry_blob = UnifiedVectorFormat::GetData<geometry_t>(format)[mapped_idx];

			    // TODO: Support Z and M
			    if (geometry_blob.GetProperties().HasZ() || geometry_blob.GetProperties().HasM()) {
				    throw InvalidInputException("ST_MakePolygon does not support Z or M geometries");
			    }

			    auto hole_geometry = Geometry::Deserialize(arena, geometry_blob);
			    if (hole_geometry.GetType() != GeometryType::LINESTRING) {
				    throw InvalidInputException(
				        StringUtil::Format("ST_MakePolygon hole #%lu is not a LINESTRING geometry", hole_idx + 1));
			    }
			    auto &hole = hole_geometry.As<LineString>();
			    if (hole.Count() < 4) {
				    throw InvalidInputException(
				        StringUtil::Format("ST_MakePolygon hole #%lu requires at least 4 vertices", hole_idx + 1));
			    }

			    if (!hole.IsClosed()) {
				    throw InvalidInputException(StringUtil::Format(
				        "ST_MakePolygon hole #%lu must be closed (first and last vertex must be equal)", hole_idx + 1));
			    }

			    rings.push_back(hole);
		    }

		    Polygon polygon(arena, rings.size(), false, false);
		    for (idx_t ring_idx = 0; ring_idx < rings.size(); ring_idx++) {
			    polygon[ring_idx] = std::move(rings[ring_idx]);
		    }
		    return Geometry(polygon).Serialize(result);
	    });
}

static void MakePolygonFromShellFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, geometry_t>(args.data[0], result, count, [&](geometry_t &line_blob) {
		if (line_blob.GetType() != GeometryType::LINESTRING) {
			throw InvalidInputException("ST_MakePolygon only accepts LINESTRING geometries");
		}

		auto line_geom = Geometry::Deserialize(arena, line_blob);
		auto &line = line_geom.As<LineString>();

		auto line_count = line.Count();
		if (line_count < 4) {
			throw InvalidInputException("ST_MakePolygon shell requires at least 4 vertices");
		}

		if (!line.IsClosed()) {
			throw InvalidInputException("ST_MakePolygon shell must be closed (first and last vertex must be equal)");
		}

		auto props = line_blob.GetProperties();

		Polygon polygon(arena, 1, props.HasZ(), props.HasM());
		polygon[0] = std::move(line);
		return Geometry(polygon).Serialize(result);
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Creates a polygon from a shell geometry and an optional set of holes
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStMakePolygon(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_MakePolygon");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::LIST(GeoTypes::GEOMETRY())},
	                               GeoTypes::GEOMETRY(), MakePolygonFromRingsFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), MakePolygonFromShellFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_MakePolygon", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
