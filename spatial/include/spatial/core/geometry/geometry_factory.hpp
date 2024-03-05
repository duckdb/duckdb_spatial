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

	Geometry FromWKB(const char *wkb, uint32_t length);

    VertexArray AllocateVertexArray(uint32_t capacity, bool has_z, bool has_m);

	Point CreatePoint(double x, double y);
	LineString CreateLineString(uint32_t capacity, bool has_z, bool has_m);
	Polygon CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities, bool has_z, bool has_m);
	// Create a polygon, but leave the ring arrays empty
	Polygon CreatePolygon(uint32_t num_rings);

	MultiPoint CreateMultiPoint(uint32_t capacity);
	MultiLineString CreateMultiLineString(uint32_t capacity);
	MultiPolygon CreateMultiPolygon(uint32_t capacity);
	GeometryCollection CreateGeometryCollection(uint32_t capacity);

	Polygon CreateBox(double xmin, double ymin, double xmax, double ymax);

	// Empty
	Point CreateEmptyPoint();
	LineString CreateEmptyLineString();
	Polygon CreateEmptyPolygon();
	MultiPoint CreateEmptyMultiPoint();
	MultiLineString CreateEmptyMultiLineString();
	MultiPolygon CreateEmptyMultiPolygon();
	GeometryCollection CreateEmptyGeometryCollection();

	geometry_t Serialize(Vector &result, const Geometry &geometry);
	Geometry Deserialize(const geometry_t &data);

	static bool TryGetSerializedBoundingBox(const geometry_t &data, BoundingBox &bbox);

	// Deep Copy
    /*
    VertexArray CopyVertexArray(const VertexArray &vector);
	Point CopyPoint(const Point &point);
	LineString CopyLineString(const LineString &linestring);
	Polygon CopyPolygon(const Polygon &polygon);
	MultiPoint CopyMultiPoint(const MultiPoint &multipoint);
	MultiLineString CopyMultiLineString(const MultiLineString &multilinestring);
	MultiPolygon CopyMultiPolygon(const MultiPolygon &multipolygon);
	GeometryCollection CopyGeometryCollection(const GeometryCollection &collection);
	Geometry CopyGeometry(const Geometry &geometry);
     */

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

	// Deserialize
	Point DeserializePoint(Cursor &reader);
	LineString DeserializeLineString(Cursor &reader);
	Polygon DeserializePolygon(Cursor &reader);
	MultiPoint DeserializeMultiPoint(Cursor &reader);
	MultiLineString DeserializeMultiLineString(Cursor &reader);
	MultiPolygon DeserializeMultiPolygon(Cursor &reader);
	GeometryCollection DeserializeGeometryCollection(Cursor &reader);
};

} // namespace core

} // namespace spatial