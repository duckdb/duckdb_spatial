#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

struct WKBWriter {
	static uint32_t GetRequiredSize(const Geometry &geom);
	static uint32_t GetRequiredSize(const Point &point);
	static uint32_t GetRequiredSize(const LineString &line);
	static uint32_t GetRequiredSize(const Polygon &polygon);
	static uint32_t GetRequiredSize(const MultiPoint &multi_point);
	static uint32_t GetRequiredSize(const MultiLineString &multi_line);
	static uint32_t GetRequiredSize(const MultiPolygon &multi_polygon);
	static uint32_t GetRequiredSize(const GeometryCollection &collection);

	static void Write(const Geometry &geom, data_ptr_t &ptr);
	static void Write(const Point &point, data_ptr_t &ptr);
	static void Write(const LineString &line, data_ptr_t &ptr);
	static void Write(const Polygon &polygon, data_ptr_t &ptr);
	static void Write(const MultiPoint &multi_point, data_ptr_t &ptr);
	static void Write(const MultiLineString &multi_line, data_ptr_t &ptr);
	static void Write(const MultiPolygon &multi_polygon, data_ptr_t &ptr);
	static void Write(const GeometryCollection &collection, data_ptr_t &ptr);
};

} // namespace core

} // namespace spatial