#pragma once

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"

#include <cmath>

namespace spatial {

namespace core {

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
	inline bool HasZ() const {
		return (flags & Z) != 0;
	}
	inline bool HasM() const {
		return (flags & M) != 0;
	}
	inline bool HasBBox() const {
		return (flags & BBOX) != 0;
	}
	inline bool IsGeodetic() const {
		return (flags & GEODETIC) != 0;
	}
	inline bool IsReadOnly() const {
		return (flags & READONLY) != 0;
	}

	inline void SetZ(bool value) {
		flags = value ? (flags | Z) : (flags & ~Z);
	}
	inline void SetM(bool value) {
		flags = value ? (flags | M) : (flags & ~M);
	}
	inline void SetBBox(bool value) {
		flags = value ? (flags | BBOX) : (flags & ~BBOX);
	}
	inline void SetGeodetic(bool value) {
		flags = value ? (flags | GEODETIC) : (flags & ~GEODETIC);
	}
	inline void SetReadOnly(bool value) {
		flags = value ? (flags | READONLY) : (flags & ~READONLY);
	}
};

struct Vertex {
	double x;
	double y;
	explicit Vertex() : x(0), y(0) {
	}
	explicit Vertex(double x, double y) : x(x), y(y) {
	}

	// Distance to another point
	double Distance(const Vertex &other) const;

	// Squared distance to another point
	double DistanceSquared(const Vertex &other) const;

	// Distance to the line segment between p1 and p2
	double Distance(const Vertex &p1, const Vertex &p2) const;

	// Squared distance to the line segment between p1 and p2
	double DistanceSquared(const Vertex &p1, const Vertex &p2) const;

	bool operator==(const Vertex &other) const {
		// approximate comparison
		return std::fabs(x - other.x) < 1e-10 && std::fabs(y - other.y) < 1e-10;
	}

	bool operator!=(const Vertex &other) const {
		return x != other.x || y != other.y;
	}

	Side SideOfLine(const Vertex &p1, const Vertex &p2) const {
		double side = ((x - p1.x) * (p2.y - p1.y) - (p2.x - p1.x) * (y - p1.y));
		if (side == 0) {
			return Side::ON;
		} else if (side < 0) {
			return Side::LEFT;
		} else {
			return Side::RIGHT;
		}
	}

	// Returns true if the point is on the line between the two points
	bool IsOnSegment(const Vertex &p1, const Vertex &p2) const {
		return ((p1.x <= x && x < p2.x) || (p1.x >= x && x > p2.x)) ||
		       ((p1.y <= y && y < p2.y) || (p1.y >= y && y > p2.y));
	}
};

enum class WindingOrder { CLOCKWISE, COUNTER_CLOCKWISE };

enum class Contains { INSIDE, OUTSIDE, ON_EDGE };

class VertexVector {
public:
	uint32_t count;
	uint32_t capacity;
	Vertex *data;

	explicit VertexVector(Vertex *data, uint32_t count, uint32_t capacity)
	    : count(count), capacity(capacity), data(data) {
	}

	// Create a VertexVector from an already existing buffer
	static VertexVector FromBuffer(Vertex *buffer, uint32_t count) {
		VertexVector array(buffer, count, count);
		return array;
	}

	inline Vertex &operator[](uint32_t index) const {
		D_ASSERT(index < count);
		return data[index];
	}

	inline Vertex *begin() {
		return data;
	}

	inline Vertex *end() {
		return data + count;
	}

	inline uint32_t Count() const {
		return count;
	}

	inline uint32_t Capacity() const {
		return capacity;
	}

	inline void Add(const Vertex &v) {
		D_ASSERT(count < capacity);
		data[count++] = v;
	}

	// Returns the number of bytes that this VertexVector requires to be serialized
	inline uint32_t SerializedSize() const {
		return sizeof(Vertex) * count;
	}

	inline void Serialize(data_ptr_t &ptr) const {
		memcpy((void *)ptr, (const char *)data, count * sizeof(Vertex));
		ptr += count * sizeof(Vertex);
	}

	inline Vertex *Data() {
		return data;
	}

	double Length() const;
	double SignedArea() const;
	double Area() const;
	Contains ContainsVertex(const Vertex &p, bool ensure_closed = true) const;
	WindingOrder GetWindingOrder() const;
	bool IsClockwise() const;
	bool IsCounterClockwise() const;

	// Returns true if the VertexVector is closed (first and last point are the same)
	bool IsClosed() const;

	// Returns true if the VertexVector is empty
	bool IsEmpty() const;

	// Returns true if the VertexVector is simple (no self-intersections)
	bool IsSimple() const;

	// Returns the index and distance of the closest segment to the point
	std::tuple<uint32_t, double> ClosestSegment(const Vertex &p) const;

	// Returns the index and distance of the closest point in the pointarray to the point
	std::tuple<uint32_t, double> ClosetVertex(const Vertex &p) const;

	// Returns the closest point, how far along the pointarray it is (0-1), and the distance to that point
	std::tuple<Vertex, double, double> LocateVertex(const Vertex &p) const;
};

// Utils
Vertex ClosestPointOnSegment(const Vertex &p, const Vertex &p1, const Vertex &p2);

} // namespace core

} // namespace spatial