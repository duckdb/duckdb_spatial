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
	static void GeometryToVarchar(Vector &source, Vector &result, idx_t count);
};

struct CoreCastFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterVarcharCasts(db);
		RegisterDimensionalCasts(db);
		RegisterGeometryCasts(db);
		RegisterWKBCasts(db);
	}

private:
	static void RegisterVarcharCasts(DatabaseInstance &db);
	static void RegisterDimensionalCasts(DatabaseInstance &db);
	static void RegisterGeometryCasts(DatabaseInstance &db);
	static void RegisterWKBCasts(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial