#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

class Cursor;
struct BoundingBox;

struct GeometryFactory {
public:
	ArenaAllocator allocator;

	explicit GeometryFactory(Allocator &allocator) : allocator(allocator) {
	}

	geometry_t Serialize(Vector &result, const Geometry &geometry, bool has_z, bool has_m);
	Geometry Deserialize(const geometry_t &data);

	static bool TryGetSerializedBoundingBox(const geometry_t &data, BoundingBox &bbox);

private:
	// Serialize
	void SerializeVertexArray(Cursor &cursor, const VertexArray &vector, bool update_bounds, BoundingBox &bbox);
	void SerializePoint(Cursor &cursor, const Point &point, BoundingBox &bbox);
	void SerializeLineString(Cursor &cursor, const LineString &linestring, BoundingBox &bbox);
	void SerializePolygon(Cursor &cursor, const Polygon &polygon, BoundingBox &bbox);
	void SerializeMultiPoint(Cursor &cursor, const MultiPoint &multipoint, BoundingBox &bbox);
	void SerializeMultiLineString(Cursor &cursor, const MultiLineString &multilinestring, BoundingBox &bbox);
	void SerializeMultiPolygon(Cursor &cursor, const MultiPolygon &multipolygon, BoundingBox &bbox);
	void SerializeGeometryCollection(Cursor &cursor, const GeometryCollection &collection, BoundingBox &bbox);

	// Get Serialize Size
	uint32_t GetSerializedSize(const Point &point);
	uint32_t GetSerializedSize(const LineString &linestring);
	uint32_t GetSerializedSize(const Polygon &polygon);
	uint32_t GetSerializedSize(const MultiPoint &multipoint);
	uint32_t GetSerializedSize(const MultiLineString &multilinestring);
	uint32_t GetSerializedSize(const MultiPolygon &multipolygon);
	uint32_t GetSerializedSize(const GeometryCollection &collection);
	uint32_t GetSerializedSize(const Geometry &geometry);
};

} // namespace core

} // namespace spatial