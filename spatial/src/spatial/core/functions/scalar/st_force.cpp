#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
template <bool HAS_Z, bool HAS_M>
static void GeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto count = args.size();
	auto &input = args.data[0];

	if (HAS_Z && HAS_M) {
		auto &z_values = args.data[1];
		auto &m_values = args.data[2];
		TernaryExecutor::Execute<geometry_t, double, double, geometry_t>(
		    input, z_values, m_values, result, count, [&](const geometry_t &blob, double default_z, double default_m) {
			    auto geom = Geometry::Deserialize(arena, blob);
			    geom.SetVertexType(arena, HAS_Z, HAS_M, default_z, default_m);
			    return Geometry::Serialize(geom, result);
		    });

	} else if (HAS_Z || HAS_M) {
		auto &z_values = args.data[1];
		BinaryExecutor::Execute<geometry_t, double, geometry_t>(
		    input, z_values, result, count, [&](const geometry_t &blob, double default_value) {
			    auto def_z = HAS_Z ? default_value : 0;
			    auto def_m = HAS_M ? default_value : 0;

			    auto geom = Geometry::Deserialize(arena, blob);
			    geom.SetVertexType(arena, HAS_Z, HAS_M, def_z, def_m);
			    return Geometry::Serialize(geom, result);
		    });
	} else {
		UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](const geometry_t &blob) {
			auto geom = Geometry::Deserialize(arena, blob);
			geom.SetVertexType(arena, HAS_Z, HAS_M);
			return Geometry::Serialize(geom, result);
		});
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

// FORCE_2D
static constexpr const char *FORCE2D_DOC_DESCRIPTION = R"(
Forces the vertices of a geometry to have X and Y components

This function will drop any Z and M values from the input geometry, if present. If the input geometry is already 2D, it will be returned as is.
)";

static constexpr const char *FORCE2D_DOC_EXAMPLE = R"(

)";

// FORCE_3DZ
static constexpr const char *FORCE3DZ_DOC_DESCRIPTION = R"(
Forces the vertices of a geometry to have X, Y and Z components

The following cases apply:
- If the input geometry has a M component but no Z component, the M component will be replaced with the new Z value.
- If the input geometry has a Z component but no M component, it will be returned as is.
- If the input geometry has both a Z component and a M component, the M component will be removed.
- Otherwise, if the input geometry has neither a Z or M component, the new Z value will be added to the vertices of the input geometry.
)";

static constexpr const char *FORCE3DZ_DOC_EXAMPLE = R"(

)";

// FORCE_3DM
static constexpr const char *FORCE3DM_DOC_DESCRIPTION = R"(
Forces the vertices of a geometry to have X, Y and M components

The following cases apply:
- If the input geometry has a Z component but no M component, the Z component will be replaced with the new M value.
- If the input geometry has a M component but no Z component, it will be returned as is.
- If the input geometry has both a Z component and a M component, the Z component will be removed.
- Otherwise, if the input geometry has neither a Z or M component, the new M value will be added to the vertices of the input geometry.
)";

static constexpr const char *FORCE3DM_DOC_EXAMPLE = R"(

)";

// FORCE_4D
static constexpr const char *FORCE4D_DOC_DESCRIPTION = R"(
Forces the vertices of a geometry to have X, Y, Z and M components

The following cases apply:
- If the input geometry has a Z component but no M component, the new M value will be added to the vertices of the input geometry.
- If the input geometry has a M component but no Z component, the new Z value will be added to the vertices of the input geometry.
- If the input geometry has both a Z component and a M component, the geometry will be returned as is.
- Otherwise, if the input geometry has neither a Z or M component, the new Z and M values will be added to the vertices of the input geometry.
)";

static constexpr const char *FORCE4D_DOC_EXAMPLE = R"(

)";

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStForce(DatabaseInstance &db) {
	ScalarFunction st_force2d("ST_Force2D", {GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                          GeometryFunction<false, false>, nullptr, nullptr, nullptr,
	                          GeometryFunctionLocalState::Init);
	ScalarFunction st_force3dz("ST_Force3DZ", {GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(),
	                           GeometryFunction<true, false>, nullptr, nullptr, nullptr,
	                           GeometryFunctionLocalState::Init);
	ScalarFunction st_force3dm("ST_Force3DM", {GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(),
	                           GeometryFunction<false, true>, nullptr, nullptr, nullptr,
	                           GeometryFunctionLocalState::Init);
	ScalarFunction st_force4d("ST_Force4D", {GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::DOUBLE},
	                          GeoTypes::GEOMETRY(), GeometryFunction<true, true>, nullptr, nullptr, nullptr,
	                          GeometryFunctionLocalState::Init);

	ExtensionUtil::RegisterFunction(db, st_force2d);
	ExtensionUtil::RegisterFunction(db, st_force3dz);
	ExtensionUtil::RegisterFunction(db, st_force3dm);
	ExtensionUtil::RegisterFunction(db, st_force4d);

	DocUtil::AddDocumentation(db, "ST_Force2D", FORCE2D_DOC_DESCRIPTION, FORCE2D_DOC_EXAMPLE, DOC_TAGS);
	DocUtil::AddDocumentation(db, "ST_Force3DM", FORCE3DM_DOC_DESCRIPTION, FORCE3DM_DOC_EXAMPLE, DOC_TAGS);
	DocUtil::AddDocumentation(db, "ST_Force3DZ", FORCE3DZ_DOC_DESCRIPTION, FORCE3DZ_DOC_EXAMPLE, DOC_TAGS);
	DocUtil::AddDocumentation(db, "ST_Force4D", FORCE4D_DOC_DESCRIPTION, FORCE4D_DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
