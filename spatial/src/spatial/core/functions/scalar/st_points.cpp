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


static void GeometryPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// Collect all vertex data into a buffer
	struct op {
		static void Case(Geometry::Tags::SinglePartGeometry, const Geometry &geom, vector<data_t> &buffer, uint32_t &result_count) {
			const auto buffer_size = buffer.size();
			const auto count = SinglePartGeometry::VertexCount(geom);
			const auto vertex_size = SinglePartGeometry::VertexSize(geom);

			// Resize the buffer to fit the new vertices
			buffer.resize(buffer.size() + vertex_size * count);

			const auto dst_ptr = buffer.data() + buffer_size;
			const auto src_ptr = geom.GetData();
			for(uint32_t i = 0; i < count; i++) {
				memcpy(dst_ptr + i * vertex_size, src_ptr + i * vertex_size, vertex_size);
			}

			result_count += count;
		}
		static void Case(Geometry::Tags::MultiPartGeometry, const Geometry &geom, vector<data_t> &buffer, uint32_t &result_count) {
			for (auto &part : MultiPartGeometry::Parts(geom)) {
				Geometry::Match<op>(part, buffer, result_count);
			}
		}
	};

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto &geom_vec = args.data[0];
	const auto count = args.size();

	vector<data_t> vertex_buffer;

	UnaryExecutor::Execute<geometry_t, geometry_t>(geom_vec, result, count, [&](geometry_t input) {
		// Reset the vertex buffer
	    vertex_buffer.clear();
	    uint32_t vertex_count = 0;

	    auto geom = Geometry::Deserialize(arena, input);
	    Geometry::Match<op>(geom, vertex_buffer, vertex_count);

	    const auto has_z = geom.GetProperties().HasZ();
	    const auto has_m = geom.GetProperties().HasM();
	    const auto vertex_size = geom.GetProperties().VertexSize();

		if(vertex_count == 0) {
			const auto mpoint = MultiPoint::CreateEmpty(has_z, has_m);
			return Geometry::Serialize(mpoint, result);
		}

		auto mpoint = MultiPoint::Create(arena, vertex_count, has_z, has_m);
		for(uint32_t i = 0; i < vertex_count; i++) {
			// Get the nth point
			auto &point = MultiPoint::Part(mpoint, i);
			// Set the point to reference the data in the vertex buffer
			Point::ReferenceData(point, vertex_buffer.data() + i * vertex_size, 1, has_z, has_m);
		}
		return Geometry::Serialize(mpoint, result);
    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Collects all the vertices in the geometry into a multipoint
)";

static constexpr const char *DOC_EXAMPLE = R"(
	select st_points('LINESTRING(1 1, 2 2)'::geometry);
	----
	MULTIPOINT (0 0, 1 1)

	select st_points('MULTIPOLYGON Z EMPTY'::geometry);
	----
	MULTIPOINT Z EMPTY
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStPoints(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Points");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                               GeometryPointsFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Points", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
