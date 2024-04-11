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

static void MakeLineListFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto count = args.size();
	auto &child_vec = ListVector::GetEntry(args.data[0]);
	UnifiedVectorFormat format;
	child_vec.ToUnifiedFormat(count, format);

	UnaryExecutor::Execute<list_entry_t, geometry_t>(args.data[0], result, count, [&](list_entry_t &geometry_list) {
		auto offset = geometry_list.offset;
		auto length = geometry_list.length;

		auto line_geom = LineString::Create(arena, length, false, false);

		uint32_t vertex_idx = 0;
		for (idx_t i = offset; i < offset + length; i++) {

			auto mapped_idx = format.sel->get_index(i);
			if (!format.validity.RowIsValid(mapped_idx)) {
				continue;
			}
			auto geometry_blob = ((geometry_t *)format.data)[mapped_idx];

			if (geometry_blob.GetType() != GeometryType::POINT) {
				throw InvalidInputException("ST_MakeLine only accepts POINT geometries");
			}

			// TODO: Support Z and M
			if (geometry_blob.GetProperties().HasZ() || geometry_blob.GetProperties().HasM()) {
				throw InvalidInputException("ST_MakeLine from list does not support Z or M geometries");
			}

			auto point = Geometry::Deserialize(arena, geometry_blob).As<Point>();
			if (point.IsEmpty()) {
				continue;
			}
			auto vertex = point.Get(0);
			line_geom.Set(vertex_idx++, vertex.x, vertex.y);
		}

		// Shrink the vertex array to the actual size
		line_geom.Resize(arena, vertex_idx);

		if (line_geom.Count() == 1) {
			throw InvalidInputException("ST_MakeLine requires zero or two or more POINT geometries");
		}

		return Geometry(line_geom).Serialize(result);
	});
}

static void MakeLineBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto count = args.size();

	BinaryExecutor::Execute<geometry_t, geometry_t, geometry_t>(
	    args.data[0], args.data[1], result, count, [&](geometry_t &geom_blob_left, geometry_t &geom_blob_right) {
		    if (geom_blob_left.GetType() != GeometryType::POINT || geom_blob_right.GetType() != GeometryType::POINT) {
			    throw InvalidInputException("ST_MakeLine only accepts POINT geometries");
		    }

		    auto geometry_left = Geometry::Deserialize(arena, geom_blob_left);
		    auto geometry_right = Geometry::Deserialize(arena, geom_blob_right);

		    if (geometry_left.IsEmpty() && geometry_right.IsEmpty()) {
			    // Empty linestring
			    return Geometry(LineString::Empty(false, false)).Serialize(result);
		    }

		    if (geometry_left.IsEmpty() || geometry_right.IsEmpty()) {
			    throw InvalidInputException("ST_MakeLine requires zero or two or more POINT geometries");
		    }

		    auto has_z = geom_blob_left.GetProperties().HasZ() || geom_blob_right.GetProperties().HasZ();
		    auto has_m = geom_blob_left.GetProperties().HasM() || geom_blob_right.GetProperties().HasM();

		    auto &point_left = geometry_left.As<Point>();
		    auto &point_right = geometry_right.As<Point>();

		    // TODO: Dont upcast the child geometries, just append and let the append function handle upcasting of the
		    // target instead.
		    point_left.SetVertexType(arena, has_z, has_m);
		    point_right.SetVertexType(arena, has_z, has_m);

		    auto line_geom = LineString::Empty(has_z, has_m);
		    line_geom.Append(arena, point_left);
		    line_geom.Append(arena, point_right);
		    return Geometry(line_geom).Serialize(result);
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
Creates a LINESTRING geometry from a pair or list of input points
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStMakeLine(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_MakeLine");

	set.AddFunction(ScalarFunction({LogicalType::LIST(GeoTypes::GEOMETRY())}, GeoTypes::GEOMETRY(),
	                               MakeLineListFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                               MakeLineBinaryFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_MakeLine", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
