#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct GeometryFactory;

struct CoreVectorOperations {
public:
	static void Point2DToVarchar(Vector &source, Vector &result, idx_t count);
	static void LineString2DToVarchar(Vector &source, Vector &result, idx_t count);
	static void Polygon2DToVarchar(Vector &source, Vector &result, idx_t count);
	static void Box2DToVarchar(Vector &source, Vector &result, idx_t count);
	static void GeometryToVarchar(Vector &source, Vector &result, idx_t count, GeometryFactory &factory);
};

struct CoreCastFunctions {
public:
	static void Register(DatabaseInstance &instance) {
		RegisterVarcharCasts(instance);
		RegisterDimensionalCasts(instance);
		RegisterGeometryCasts(instance);
		RegisterWKBCasts(instance);
	}

private:
	static void RegisterVarcharCasts(DatabaseInstance &instance);
	static void RegisterDimensionalCasts(DatabaseInstance &instance);
	static void RegisterGeometryCasts(DatabaseInstance &instance);
	static void RegisterWKBCasts(DatabaseInstance &instance);
};

} // namespace core

} // namespace spatial