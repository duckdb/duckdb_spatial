#pragma once
#include "geo/common.hpp"
#include "geo/core/geometry/vertex_vector.hpp"
#include "geo/core/geometry/geometry.hpp"

namespace geo {

namespace core {

struct GeometryFactory {
public:
	ArenaAllocator &allocator;

	explicit GeometryFactory(ArenaAllocator &allocator) : allocator(allocator) {
	}

	Geometry FromWKT(const char *wkt, uint32_t length);
	Geometry FromWKB(const char *wkb, uint32_t length);
	string ToWKT(const Geometry &geometry);
	string ToWKB(const Geometry &geometry);

	VertexVector AllocateVertexVector(uint32_t capacity);

	Point CreatePoint(double x, double y);
	LineString CreateLineString(uint32_t capacity);
	Polygon CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities);
	// Create a polygon, but leave the ring arrays uninitialized
	Polygon CreatePolygon(uint32_t num_rings);

	MultiPoint CreateMultiPoint(uint32_t capacity);
	MultiLineString CreateMultiLineString(uint32_t capacity);
	MultiPolygon CreateMultiPolygon(uint32_t capacity);
	GeometryCollection CreateGeometryCollection(uint32_t capacity);

	string_t Serialize(Vector &result, const Geometry &geometry);
	Geometry Deserialize(const string_t &data);

private:
	// Serialize
	void SerializePoint(data_ptr_t &ptr, const Point &point);
	void SerializeLineString(data_ptr_t &ptr, const LineString &linestring);
	void SerializePolygon(data_ptr_t &ptr, const Polygon &polygon);
	void SerializeMultiPoint(data_ptr_t &ptr, const MultiPoint &multipoint);
	void SerializeMultiLineString(data_ptr_t &ptr, const MultiLineString &multilinestring);
	void SerializeMultiPolygon(data_ptr_t &ptr, const MultiPolygon &multipolygon);
	void SerializeGeometryCollection(data_ptr_t &ptr, const GeometryCollection &collection);

	// Get Serialize Size
	uint32_t GetSerializedSize(const Point &point);
	uint32_t GetSerializedSize(const LineString &linestring);
	uint32_t GetSerializedSize(const Polygon &polygon);
	uint32_t GetSerializedSize(const MultiPoint &multipoint);
	uint32_t GetSerializedSize(const MultiLineString &multilinestring);
	uint32_t GetSerializedSize(const MultiPolygon &multipolygon);
	uint32_t GetSerializedSize(const GeometryCollection &collection);

	/*
	uint32_t SerializeMultiPoint(data_ptr_t &ptr, const MultiPoint &multipoint);
	uint32_t SerializeMultiLineString(data_ptr_t &ptr, const MultiLineString &multilinestring);
	uint32_t SerializeMultiPolygon(data_ptr_t &ptr, const MultiPolygon &multipolygon);
	uint32_t SerializeGeometryCollection(data_ptr_t &ptr, const GeometryCollection &collection);
	*/

	// Deserialize
	Point DeserializePoint(const_data_ptr_t &ptr);
	LineString DeserializeLineString(const_data_ptr_t &ptr);
	Polygon DeserializePolygon(const_data_ptr_t &ptr);
	MultiPoint DeserializeMultiPoint(const_data_ptr_t &ptr);
	MultiLineString DeserializeMultiLineString(const_data_ptr_t &ptr);
	MultiPolygon DeserializeMultiPolygon(const_data_ptr_t &ptr);
	GeometryCollection DeserializeGeometryCollection(const_data_ptr_t &ptr);

	/*
	Geometry DeserializeMultiPoint(const_data_ptr_t &ptr);
	Geometry DeserializeMultiLineString(const_data_ptr_t &ptr);
	Geometry DeserializeMultiPolygon(const_data_ptr_t &ptr);
	Geometry DeserializeGeometryCollection(const_data_ptr_t &ptr);
	*/
};

} // namespace core

} // namespace geo