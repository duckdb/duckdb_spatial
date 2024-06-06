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
		static void Case(Geometry::Tags::SinglePartGeometry, const Geometry &geom, vector<const_data_ptr_t> &buffer) {
			const auto vertex_count = SinglePartGeometry::VertexCount(geom);
			const auto vertex_size = SinglePartGeometry::VertexSize(geom);

			// Reserve size for the pointers to the vertices
			buffer.reserve(buffer.size() + vertex_count);

			const auto vertex_ptr = geom.GetData();

			for (uint32_t i = 0; i < vertex_count; i++) {
				buffer.push_back(vertex_ptr + i * vertex_size);
			}
		}
		static void Case(Geometry::Tags::MultiPartGeometry, const Geometry &geom, vector<const_data_ptr_t> &buffer) {
			for (auto &part : MultiPartGeometry::Parts(geom)) {
				Geometry::Match<op>(part, buffer);
			}
		}
	};

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto &geom_vec = args.data[0];
	const auto count = args.size();

	vector<const_data_ptr_t> vertex_ptr_buffer;

	UnaryExecutor::Execute<geometry_t, geometry_t>(geom_vec, result, count, [&](geometry_t input) {
		const auto geom = Geometry::Deserialize(arena, input);
		const auto has_z = geom.GetProperties().HasZ();
		const auto has_m = geom.GetProperties().HasM();

		// Reset the vertex pointer buffer
		vertex_ptr_buffer.clear();

		// Collect the vertex pointers
		Geometry::Match<op>(geom, vertex_ptr_buffer);

		if (vertex_ptr_buffer.empty()) {
			const auto mpoint = MultiPoint::CreateEmpty(has_z, has_m);
			return Geometry::Serialize(mpoint, result);
		}

		auto mpoint = MultiPoint::Create(arena, vertex_ptr_buffer.size(), has_z, has_m);
		for (size_t i = 0; i < vertex_ptr_buffer.size(); i++) {
			// Get the nth point
			auto &point = MultiPoint::Part(mpoint, i);
			// Set the point to reference the data pointer to the current vertex
			Point::ReferenceData(point, vertex_ptr_buffer[i], 1, has_z, has_m);
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
	MULTIPOINT (1 1, 2 2)

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

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), GeometryPointsFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Points", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
