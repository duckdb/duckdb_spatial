#pragma once

#include "geo/common.hpp"

namespace geo {

namespace core {

using DIMENSIONS = int;
using MEASURES = int;

template<DIMENSIONS ND, MEASURES NM>
struct TPoint { };

template<>
struct TPoint<3, 0> {
	double x;
	double y;
	double z;
};

template<>
struct TPoint<2, 0> {
	double x;
	double y;
};

template<>
struct TPoint<3, 1> {
	double x;
	double y;
	double z;
	double m;
};

template<>
struct TPoint<2, 1> {
	double x;
	double y;
	double m;
};

template<DIMENSIONS ND, MEASURES NM>
struct TPointArray {
	TPoint<ND, NM>* points;
	uint32_t num_points;

	double Length() const {
		double length = 0;
		for (uint32_t i = 0; i < num_points - 1; i++) {
			auto &p1 = points[i];
			auto &p2 = points[i + 1];
			length += std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
		}
		return length;
	}

	// Signed area of the linestring (as a polygon).
	// Positive if CCW, negative if CW.
	double SignedArea() const {
		if(num_points < 3) {
			return 0;
		}
		double area = 0;
		for (uint32_t i = 0; i < num_points - 1; i++) {
			auto &p1 = points[i];
			auto &p2 = points[i + 1];
			area += p1.x * p2.y - p2.x * p1.y;
		}
		return area * 0.5;
	}

	TPoint<ND, NM>& operator[](uint32_t index) const {
		D_ASSERT(index < num_points);
		return points[index];
	}

	uint32_t GetSerializedSize() const {
		return sizeof(TPoint<ND, NM>) * num_points;
	}

	data_ptr_t Write(data_ptr_t ptr) const {
		auto size = sizeof(TPoint<ND, NM>) * num_points;
		memcpy(ptr, points, size);
		return ptr + size;
	}

};


template<DIMENSIONS LD, MEASURES LM, DIMENSIONS RD, MEASURES RM>
static double Distance(TPoint<LD, LM> a, TPoint<RD, RM> b) {
	return std::sqrt((a.x - b.x) * (a.x - b.x) + (a.y - b.y) * (a.y - b.y));
}

template<DIMENSIONS LD, MEASURES LM, DIMENSIONS RD, MEASURES RM>
static double Distance(TPointArray<LD, LM> a, TPointArray<RD, RM> b) {
	double min_dist = std::numeric_limits<double>::max();
	for (uint32_t i = 0; i < a.num_points; i++) {
		for (uint32_t j = 0; j < b.num_points; j++) {
			min_dist = std::min(min_dist, Distance(a[i], b[j]));
		}
	}
	return min_dist;
}

static double Distance(TPointArray<2, 0> a, TPointArray<2, 0> b) {
	double min_dist = std::numeric_limits<double>::max();
	for (uint32_t i = 0; i < a.num_points; i++) {
		for (uint32_t j = 0; j < b.num_points; j++) {
			min_dist = std::min(min_dist, Distance(a[i], b[j]));
		}
	}
	return min_dist;
}



static void foobar() {
	TPoint<3, 1> a;
	TPoint<2, 0> b;

	TPointArray<3, 1> arr;
	TPointArray<2, 0> arr2;
	arr.Length();
	arr.SignedArea();

	arr[0].z;

	Distance(arr2, arr);
	Distance(a, b);
}
enum class GeometryType : uint8_t {
	Point = 0,
	LineString = 1,
	Polygon = 2,
};

static void Handle(GeometryType type, DIMENSIONS dims, MEASURES measures) {
	switch (type) {
	case GeometryType::LineString:
		switch (dims) {
		case 2:
			switch (measures) {
			case 0:
				auto p = TPointArray<2, 0>();
				break;
			case 1:
				auto p = TPointArray<2, 1>();
				break;
			}
			break;
		}
		break;
	case GeometryType::Point:
		switch(dims) {
		case 2:
			switch(measures) {
			case 0:
				auto p = TPoint<2, 0>();
				break;
			case 1:
				auto p = TPoint<2, 1>();
				break;
			}
			break;
		case 3:
			switch(measures) {
			case 0:
				auto p = TPoint<3, 0>();
				break;
			case 1:
				auto p = TPoint<3, 1>();
				break;
			}
			break;
		}
		break;
	case GeometryType::Polygon:
		break;
	}
}


template<DIMENSIONS ND, MEASURES NM>
struct GPoint {
	static_assert(ND > 0, "GPoint must have at least one dimension");
	static_assert(ND <= 3, "GPoint can have at most three dimensions");
	static_assert(NM <= 1, "GPoint can have at most one measure");
private:
	double _data[ND + NM];
public:
	bool HasMeasure() const {
		return NM > 0;
	}

	typename std::enable_if<(ND > 0), double&>::type X() const {
		return _data[0];
	}

	typename std::enable_if<(ND > 1), double&>::type Y() const {
		return _data[1];
	}

	typename std::enable_if<(ND > 2), double&>::type Z() const {
		return _data[2];
	}

	typename std::enable_if<(ND > 0), double&>::type M() const {
		return _data[ND];
	}
};


enum class GeometryType : uint8_t {
	Point = 1,
	LineString = 2,
	Polygon = 3,
	MultiPoint = 4,
	MultiLineString = 5,
	MultiPolygon = 6,
	GeometryCollection = 7,
};

struct Point2D {
	double x;
	double y;
};

// PointArray
struct alignas(double) PointArray  {
	uint32_t flags;
	uint32_t num_points;
	Point2D* points;
public:

	Point2D& operator[](uint32_t index) const {
		D_ASSERT(index < num_points);
		return points[index];
	}

	Point2D* begin() const {
		return points;
	}

	Point2D* end() const {
		return points + num_points;
	}

	Point2D* data() const {
		return points;
	}

	uint32_t size() const {
		return num_points;
	}

	double Length() const {
		double length = 0;
		for (uint32_t i = 0; i < num_points - 1; i++) {
			auto &p1 = points[i];
			auto &p2 = points[i + 1];
			length += std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
		}
		return length;
	}

	// Signed area of the linestring (as a polygon).
	// Positive if CCW, negative if CW.
	double SignedArea() const {
		if(num_points < 3) {
			return 0;
		}
		double area = 0;
		for (uint32_t i = 0; i < num_points - 1; i++) {
			auto &p1 = points[i];
			auto &p2 = points[i + 1];
			area += p1.x * p2.y - p2.x * p1.y;
		}
		return area * 0.5;
	}

	double IsClosed() const {
		// TODO: May want to check for a tolerance here?
		return points[0].x == points[num_points - 1].x && points[0].y == points[num_points - 1].y;
	}

	uint32_t GetSerializedSize() const {
		return sizeof(Point2D) * num_points;
	}

	data_ptr_t Write(data_ptr_t ptr) const {
		auto size = sizeof(Point2D) * num_points;
		memcpy(ptr, points, size);
		return ptr + size;
	}

};
static_assert(sizeof(PointArray) == 16, "PointArray should be 16 bytes");

// This is enough to always fit in the string_t prefix.
struct GeometryPrefix {
	uint8_t flags;
	GeometryType type;
	uint16_t _pad1;
	uint32_t _pad2;
public:
	// Write the geometry to the specified pointer, and return the pointer after the written data
	data_ptr_t Write(data_ptr_t ptr) const {
		auto size = sizeof(GeometryPrefix);
		memcpy(ptr, this, size);
		return ptr + size;
	}
};
static_assert(sizeof(GeometryPrefix) == 8, "GeometryPrefix should be 8 bytes");
static_assert(std::is_pod<GeometryPrefix>(), "geometry must be trivially copyable");


struct GeometryContext;
struct Geometry;
struct Point;
struct LineString;
struct Polygon;
struct MultiPoint;
struct MultiLineString;
struct MultiPolygon;

// Geometry Types
struct Geometry {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	data_ptr_t data;
public:
	GeometryType GetType() const {
		return type;
	}
};

static_assert(std::is_trivially_copyable<Geometry>::value, "geometry must be trivially copyable");

struct Point {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	PointArray* point;
public:

	double Distance(Point* other) const {
		auto &p1 = point->points[0];
		auto &p2 = other->point->points[0];
		return std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
	}

	inline double X() const {
		return point->points[0].x;
	}

	inline double Y() const {
		return point->points[0].y;
	}

	static Geometry* ToGeometry(Point* point) {
		return reinterpret_cast<Geometry*>(point);
	}
};


struct LineString {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	PointArray* points;
	uint32_t num_points;
public:
	static Geometry* ToGeometry(LineString* line_string) {
		return reinterpret_cast<Geometry*>(line_string);
	}
};

struct Polygon {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	PointArray** rings;
	uint32_t num_rings;
public:
	static Geometry* ToGeometry(Polygon* polygon) {
		return reinterpret_cast<Geometry*>(polygon);
	}
};

struct MultiPoint {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	PointArray* points;
	uint32_t num_points;
public:
	static Geometry* ToGeometry(MultiPoint* multi_point) {
		return reinterpret_cast<Geometry*>(multi_point);
	}
};

struct MultiLineString {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	PointArray** lines;
	uint32_t num_lines;
public:
	static Geometry* ToGeometry(MultiLineString* multi_line_string) {
		return reinterpret_cast<Geometry*>(multi_line_string);
	}
};

struct MultiPolygon {
	friend GeometryContext;
private:
	GeometryType type;
	uint8_t flags;
	PointArray* polygons;
	uint32_t num_polygons;
public:
	static Geometry* ToGeometry(MultiPolygon* multi_polygon) {
		return reinterpret_cast<Geometry*>(multi_polygon);
	}
};

// Geometry Context (manages allocations)
struct GeometryContext {
private:
	ArenaAllocator &allocator;
public:
	explicit GeometryContext(ArenaAllocator& allocator_p) : allocator(allocator_p) {

	}

	Point* CreatePoint(double x, double y) {
		auto point = (Point*)allocator.Allocate(sizeof(Point));
		auto point_data = (PointArray*)allocator.Allocate(sizeof(PointArray));

		point->type = GeometryType::Point;
		point->flags = 0;
		point->point = point_data;
		point->point->flags = 0;
		point->point->num_points = 1;
		point->point->points = (Point2D*)allocator.Allocate(sizeof(Point2D));
		point->point->points[0].x = x;
		point->point->points[0].y = y;
		return point;
	}

	string_t SaveGeometry(Geometry* geometry, Vector& vec) {
		switch (geometry->type) {
		case GeometryType::Point: {
			auto point = (Point*)geometry;
			auto size = sizeof(GeometryPrefix) + point->point->GetSerializedSize();
			auto start = allocator.Allocate(size);
			auto prefix = GeometryPrefix();
			prefix.type = GeometryType::Point;
			prefix.flags = point->flags;
			auto cursor = start;
			cursor = prefix.Write(cursor);
			cursor = point->point->Write(cursor);
			return StringVector::AddStringOrBlob(vec, (char*)start, size);
		}
		default:
			throw NotImplementedException("Geometry type not implemented");
		}
	}

	GeometryType PeekType(string_t data) {
		auto prefix = (GeometryPrefix*)data.GetPrefix();
		return prefix->type;
	}

	Geometry* LoadGeometry(string_t data) {
		// Geometry string_t's are never inlined
		// The prefix is always 4 bytes.
		auto prefix = (GeometryPrefix*)data.GetPrefix();
		switch (prefix->type) {
		case GeometryType::Point: {
			auto point = (Point*)allocator.Allocate(sizeof(Point));
			auto point_data = (PointArray*)allocator.Allocate(sizeof(PointArray));
			point->type = GeometryType::Point;
			point->flags = prefix->flags;
			point->point = point_data;
			point->point->flags = 0;
			point->point->num_points = 1;
			point->point->points = (Point2D*)(data.GetDataUnsafe() + sizeof(GeometryPrefix));
			return (Geometry*)point;
		}
		default:
			throw NotImplementedException("Geometry type not implemented");
		}
	}
};

} // core

} // geo