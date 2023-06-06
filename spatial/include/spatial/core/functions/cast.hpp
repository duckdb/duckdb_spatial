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
	static void Register(ClientContext &context) {
		RegisterVarcharCasts(context);
		RegisterDimensionalCasts(context);
		RegisterGeometryCasts(context);
		RegisterWKBCasts(context);
	}

private:
	static void RegisterVarcharCasts(ClientContext &context);
	static void RegisterDimensionalCasts(ClientContext &context);
	static void RegisterGeometryCasts(ClientContext &context);
	static void RegisterWKBCasts(ClientContext &context);
};

} // namespace core

} // namespace spatial