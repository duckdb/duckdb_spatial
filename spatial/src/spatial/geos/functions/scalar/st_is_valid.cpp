#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static bool IsValidForGeos(Geometry &geometry) {
	switch(geometry.Type()) {
	case GeometryType::LINESTRING:
		// Every linestring needs 0 or at least 2 points
		return geometry.GetLineString().Count() != 1;

	case GeometryType::POLYGON: {
		// Every ring needs 0 or at least 4 points
		auto &polygon = geometry.GetPolygon();
		for (auto &ring : polygon.Rings()) {
			if (ring.Count() > 0 && ring.Count() < 4) {
				return false;
			}
		}
	}
	case GeometryType::MULTILINESTRING: {
		for (auto &linestring : geometry.GetMultiLineString()) {
			if (linestring.Count() == 1) {
				return false;
			}
		}
		return true;
	}
	case GeometryType::MULTIPOLYGON: {
		for (auto &polygon : geometry.GetMultiPolygon()) {
			for (auto &ring : polygon.Rings()) {
				if (ring.Count() > 0 && ring.Count() < 4) {
					return false;
				}
			}
		}
		return true;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		for (auto &geom : geometry.GetGeometryCollection()) {
			if (!IsValidForGeos(geom)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

static void IsValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t input) {
		auto geom = lstate.factory.Deserialize(input);

		// double check before calling into geos
		if (!IsValidForGeos(geom)) {
			return false;
		}

		auto geos_geom = lstate.ctx.Deserialize(input);
		return (bool)GEOSisValid_r(lstate.ctx.GetCtx(), geos_geom.get());
	});
}

void GEOSScalarFunctions::RegisterStIsValid(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_IsValid");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN, IsValidFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geos

} // namespace spatial
