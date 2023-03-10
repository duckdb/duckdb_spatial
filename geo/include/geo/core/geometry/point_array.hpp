#pragma once

#include "geo/common.hpp"

namespace geo {

namespace core {

enum class Side {
	LEFT,
	RIGHT,
	ON
};

struct Flags {
private:
	static constexpr const uint8_t Z = 0x01;
	static constexpr const uint8_t M = 0x02;
	static constexpr const uint8_t BBOX = 0x04;
	static constexpr const uint8_t GEODETIC = 0x08;
	static constexpr const uint8_t READONLY = 0x10;
	static constexpr const uint8_t SOLID = 0x20;
	uint8_t flags;
public:
	inline bool HasZ() const { return (flags & Z) != 0; }
	inline bool HasM() const { return (flags & M) != 0; }
	inline bool HasBBox() const { return (flags & BBOX) != 0; }
	inline bool IsGeodetic() const { return (flags & GEODETIC) != 0; }
	inline bool IsReadOnly() const { return (flags & READONLY) != 0; }

	inline void SetZ(bool value) { flags = value ? (flags | Z) : (flags & ~Z); }
	inline void SetM(bool value) { flags = value ? (flags | M) : (flags & ~M); }
	inline void SetBBox(bool value) { flags = value ? (flags | BBOX) : (flags & ~BBOX); }
	inline void SetGeodetic(bool value) { flags = value ? (flags | GEODETIC) : (flags & ~GEODETIC); }
	inline void SetReadOnly(bool value) { flags = value ? (flags | READONLY) : (flags & ~READONLY); }
};

struct Point {
	double x;
	double y;
	explicit Point() : x(0), y(0) {}
	explicit Point(double x, double y) : x(x), y(y) {}

	// Distance to another point
	double Distance(const Point &other) const;

	// Squared distance to another point
	double DistanceSquared(const Point &other) const;

	// Distance to the line segment between p1 and p2
	double Distance(const Point &p1, const Point &p2) const;

	// Squared distance to the line segment between p1 and p2
	double DistanceSquared(const Point &p1, const Point &p2) const;

	bool operator==(const Point &other) const {
		return x == other.x && y == other.y;
	}

	bool operator!=(const Point &other) const {
		return x != other.x || y != other.y;
	}

	Side SideOfLine(const Point &p1, const Point &p2) const {
		double side = ( (x - p1.x) * (p2.y - p1.y) - (p2.x - p1.x) * (y - p1.y) );
		if (side == 0) {
			return Side::ON;
		} else if (side < 0) {
			return Side::LEFT;
		} else {
			return Side::RIGHT;
		}
	}

	// Returns true if the point is on the line between the two points
	bool IsOnSegment(const Point &p1, const Point &p2) const {
		return ((p1.x <= x && x < p2.x) || (p1.x >= x && x > p2.x)) ||
		       ((p1.y <= y && y < p2.y) || (p1.y >= y && y > p2.y));
	}
};

enum class WindingOrder {
	CLOCKWISE,
	COUNTER_CLOCKWISE
};



enum class Contains {
	INSIDE,
	OUTSIDE,
	ON_EDGE
};

class PointArray {
private:
	uint32_t count;
	uint32_t capacity;
	Point *data;
	bool is_owned;
private:
	PointArray(Point *data, uint32_t count, uint32_t capacity, bool is_owned) : data(data), count(count), capacity(capacity), is_owned(is_owned)
	{ }
public:

	// Copy constructor (deleted)
	PointArray(const PointArray&) = delete;

	// Copy assignment (deleted)
	PointArray& operator=(const PointArray&) = delete;

	// Move constructor
	PointArray(PointArray &&other) noexcept : data(nullptr) {
		std::swap(other.data, data);
		count = other.count;
		capacity = other.capacity;
		is_owned = other.is_owned;
	}

	// Move assignment
	PointArray &operator=(PointArray &&other) noexcept {
		std::swap(other.data, data);
		count = other.count;
		capacity = other.capacity;
		is_owned = other.is_owned;
		return *this;
	}

	// Destructor
	~PointArray() {
		if (is_owned) {
			free(data);
		}
	}

	// Create a PointArray from an already existing buffer
	static PointArray FromBuffer(Point *buffer, uint32_t count) {
		PointArray array(buffer, count, count, false);
		return array;
	}

	// Create a PointArray by allocating a new buffer on the heap
	static PointArray Create(uint32_t capacity) {
		PointArray array((Point *)malloc(sizeof(Point) * capacity), 0, capacity, true);
		return array;
	}

	inline Point& operator[](uint32_t index) {
		D_ASSERT(index < count);
		return data[index];
	}

	inline const Point& operator[](uint32_t index) const {
		D_ASSERT(index < count);
		return data[index];
	}

	inline Point* begin() {
		return data;
	}

	inline Point* end() {
		return data + count;
	}

	inline uint32_t Count() const {
		return count;
	}

	inline uint32_t Capacity() const {
		return capacity;
	}

	// Returns the number of bytes that this PointArray requires to be serialized
	inline uint32_t SerializedSize() const {
		return sizeof(Point) * count;
	}

	// Serializes the PointArray to a buffer
	void Serialize(const char* dst) {
		memcpy((void*)dst, (const char*)data, count * sizeof(Point));
	}

	inline Point* Data() {
		return data;
	}

	double Length() const;
	double SignedArea() const;
	double Area() const;
	Contains ContainsPoint(const Point &p, bool ensure_closed = true) const;
	WindingOrder GetWindingOrder() const;
	bool IsClockwise() const;
	bool IsCounterClockwise() const;

	// Returns true if the PointArray is closed (first and last point are the same)
	bool IsClosed() const;

	// Returns true if the PointArray is empty
	bool IsEmpty() const;

	// Returns true if the PointArray is simple (no self-intersections)
	bool IsSimple() const;

	// Returns the index and distance of the closest segment to the point
	std::tuple<uint32_t, double> ClosestSegment(const Point &p) const;

	// Returns the index and distance of the closest point in the pointarray to the point
	std::tuple<uint32_t, double> ClosetPoint(const Point &p) const;

	// Returns the closest point, how far along the pointarray it is (0-1), and the distance to that point
	std::tuple<Point, double, double> LocatePoint(const Point &p) const;
};

// Utils
Point ClosestPointOnSegment(const Point &p, const Point &p1, const Point &p2);

}

}