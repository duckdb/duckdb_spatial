#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

struct UpdateDimensionFunctor {
	static void Apply(Point &point, bool has_z, bool has_m) {
		point.Vertices().UpdateVertexType(has_z, has_m);
	}

	static void Apply(MultiPoint &mpoint, bool has_z, bool has_m) {
		for (auto &point : mpoint) {
			point.Vertices().UpdateVertexType(has_z, has_m);
		}
	}

	static void Apply(LineString &line, bool has_z, bool has_m) {
		line.Vertices().UpdateVertexType(has_z, has_m);
	}

	static void Apply(MultiLineString &mline, bool has_z, bool has_m) {
		for (auto &line : mline) {
			line.Vertices().UpdateVertexType(has_z, has_m);
		}
	}

	static void Apply(Polygon &polygon, bool has_z, bool has_m) {
		for (auto &ring : polygon) {
			ring.UpdateVertexType(has_z, has_m);
		}
	}

	static void Apply(MultiPolygon &mpolygon, bool has_z, bool has_m) {
		for (auto &polygon : mpolygon) {
			for (auto &ring : polygon) {
				ring.UpdateVertexType(has_z, has_m);
			}
		}
	}

	static void Apply(GeometryCollection &collection, bool has_z, bool has_m) {
		for (auto &child : collection) {
			child.Dispatch<UpdateDimensionFunctor>(has_z, has_m);
		}
	}
};

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
template <bool HAS_Z, bool HAS_M>
static void GeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto count = args.size();
	auto &input = args.data[0];

	if (HAS_Z || HAS_M) {
		auto &z_values = args.data[1];
		BinaryExecutor::Execute<geometry_t, double, geometry_t>(
		    input, z_values, result, count, [&](const geometry_t &blob, double z_values) {
			    auto geometry = lstate.factory.Deserialize(blob);
			    geometry.Dispatch<UpdateDimensionFunctor>(HAS_Z, HAS_M);
			    return lstate.factory.Serialize(result, geometry, HAS_Z, HAS_M);
		    });
	} else {
		UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](const geometry_t &blob) {
			auto geometry = lstate.factory.Deserialize(blob);
			geometry.Dispatch<UpdateDimensionFunctor>(HAS_Z, HAS_M);
			return lstate.factory.Serialize(result, geometry, HAS_Z, HAS_M);
		});
	}
}

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

	ExtensionUtil::RegisterFunction(db, st_force2d);
	ExtensionUtil::RegisterFunction(db, st_force3dz);
	ExtensionUtil::RegisterFunction(db, st_force3dm);
}

} // namespace core

} // namespace spatial
