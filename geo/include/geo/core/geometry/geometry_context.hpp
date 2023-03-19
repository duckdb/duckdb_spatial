#pragma once
#include "geo/common.hpp"
#include "geo/core/geometry/vertex_vector.hpp"
#include "geo/core/geometry/geometry.hpp"

namespace geo {

namespace core {

struct GeometryContext {
public:
	ArenaAllocator &allocator;

	explicit GeometryContext(ArenaAllocator &allocator) : allocator(allocator) {}

	Geometry FromWKT(const char* wkt, uint32_t length);
	Geometry FromWKB(const char* wkb, uint32_t length);
	string ToWKT(const Geometry &geometry);
	string ToWKB(const Geometry &geometry);

	VertexVector AllocateVertexVector(uint32_t capacity);

	Point CreatePoint(double x, double y);
	LineString CreateLineString(uint32_t capacity);
	Polygon CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities);
	// Create a polygon, but leave the ring arrays uninitialized
	Polygon CreatePolygon(uint32_t num_rings);


	string_t Serialize(Vector &result, const Geometry &geometry);
	Geometry Deserialize(const string_t &data);
};

}

}