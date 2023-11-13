#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include <cmath>
#include <algorithm>

namespace spatial {

namespace core {

double Vertex::Distance(const Vertex &other) const {
	return std::sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
}

double Vertex::DistanceSquared(const Vertex &other) const {
	return (x - other.x) * (x - other.x) + (y - other.y) * (y - other.y);
}

double Vertex::Distance(const Vertex &p1, const Vertex &p2) const {
	return std::sqrt(DistanceSquared(p1, p2));
}

double Vertex::DistanceSquared(const Vertex &p1, const Vertex &p2) const {
	auto p = ClosestPointOnSegment(*this, p1, p2);
	return DistanceSquared(p);
}

void VertexVector::Serialize(Cursor &cursor) const {
	auto ptr = cursor.GetPtr();
	memcpy((void *)ptr, (const char *)data, count * sizeof(Vertex));
	ptr += count * sizeof(Vertex);
	cursor.SetPtr(ptr);
}

void VertexVector::SerializeAndUpdateBounds(Cursor &cursor, BoundingBox &bbox) const {
	for (idx_t i = 0; i < count; i++) {
		auto p = Get(i);
		bbox.minx = std::min(bbox.minx, p.x);
		bbox.miny = std::min(bbox.miny, p.y);
		bbox.maxx = std::max(bbox.maxx, p.x);
		bbox.maxy = std::max(bbox.maxy, p.y);
		cursor.Write(p.x);
		cursor.Write(p.y);
	}
}

double VertexVector::Length() const {
	double length = 0;
	if (count < 2) {
		return 0.0;
	}
	for (uint32_t i = 0; i < count - 1; i++) {
		auto p1 = Get(i);
		auto p2 = Get(i + 1);
		length += std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
	}
	return length;
}

double VertexVector::SignedArea() const {
	if (count < 3) {
		return 0;
	}

	// Subtract the x coordinate of the first vertex from all other vertices
	// to normalize the range and avoid floating point errors
	// We don't need to do this for the y coordinate because we already
	// subtract them between consecutive vertices
	double area = 0;
	auto x0 = Get(0).x;
	for (uint32_t i = 1; i < count - 1; ++i) {
		auto x1 = Get(i).x;
		auto y1 = Get(i + 1).y;
		auto y2 = Get(i - 1).y;
		area += (x1 - x0) * (y2 - y1);
	}

	return area * 0.5;
}

double ColumnarArea(vector<double> xs, vector<double> ys) {
	double area = 0;

	for (uint32_t i = 0; i < xs.size() - 1; i++) {
		area += xs[i] * ys[i + 1];
		area -= xs[i + 1] * ys[i];
	}
	return area;
}

Contains ColumnarContainsPoint(vector<double> xs, vector<double> ys, double x, double y) {

	int winding_number = 0;
	uint32_t count = xs.size();

	auto x1 = xs[0];
	auto y1 = ys[0];

	for (uint32_t i = 0; i < count; i++) {
		auto x2 = xs[i];
		auto y2 = ys[i];

		if (x1 == x2 && y1 == y2) {
			x1 = x2;
			y1 = y2;
			continue;
		}

		auto y_min = std::min(y1, y2);
		auto y_max = std::max(y1, y2);

		if (y > y_max || y < y_min) {
			x1 = x2;
			y1 = y2;
			continue;
		}

		auto side = Side::ON;
		double side_v = ((x - x1) * (y2 - y1) - (x2 - x1) * (y - y1));
		if (side_v == 0) {
			side = Side::ON;
		} else if (side_v < 0) {
			side = Side::LEFT;
		} else {
			side = Side::RIGHT;
		}

		if (side == Side::ON &&
		    (((x1 <= x && x < x2) || (x1 >= x && x > x2)) || ((y1 <= y && y < y2) || (y1 >= y && y > y2)))) {
			return Contains::ON_EDGE;
		} else if (side == Side::LEFT && (y1 < y && y <= y2)) {
			winding_number++;
		} else if (side == Side::RIGHT && (y2 <= y && y < y1)) {
			winding_number--;
		}

		x1 = x2;
		y1 = y2;
	}

	return winding_number == 0 ? Contains::OUTSIDE : Contains::INSIDE;
}

double VertexVector::Area() const {
	return std::abs(SignedArea());
}

bool VertexVector::IsClosed() const {
	if (count == 0) {
		return false;
	}
	if (count == 1) {
		return true;
	}
	return Get(0) == Get(count - 1);
}

bool VertexVector::IsEmpty() const {
	return count == 0;
}

WindingOrder VertexVector::GetWindingOrder() const {
	return SignedArea() > 0 ? WindingOrder::COUNTER_CLOCKWISE : WindingOrder::CLOCKWISE;
}

bool VertexVector::IsClockwise() const {
	return GetWindingOrder() == WindingOrder::CLOCKWISE;
}

bool VertexVector::IsCounterClockwise() const {
	return GetWindingOrder() == WindingOrder::COUNTER_CLOCKWISE;
}

bool VertexVector::IsSimple() const {
	throw NotImplementedException("VertexVector::IsSimple");
}

Contains VertexVector::ContainsVertex(const Vertex &p, bool ensure_closed) const {

	auto p1 = Get(0);
	auto p2 = Get(count - 1);

	if (ensure_closed && p1 != p2) {
		throw InternalException("VertexVector::Contains: VertexVector is not closed");
	}

	int winding_number = 0;

	for (uint32_t i = 0; i < count; i++) {
		p2 = Get(i);
		if (p1 == p2) {
			p1 = p2;
			continue;
		}

		auto y_min = std::min(p1.y, p2.y);
		auto y_max = std::max(p1.y, p2.y);

		if (p.y > y_max || p.y < y_min) {
			p1 = p2;
			continue;
		}

		auto side = p.SideOfLine(p1, p2);
		if (side == Side::ON && p.IsOnSegment(p1, p2)) {
			return Contains::ON_EDGE;
		} else if (side == Side::LEFT && (p1.y < p.y && p.y <= p2.y)) {
			winding_number++;
		} else if (side == Side::RIGHT && (p2.y <= p.y && p.y < p1.y)) {
			winding_number--;
		}
		p1 = p2;
	}
	return winding_number == 0 ? Contains::OUTSIDE : Contains::INSIDE;
}

std::tuple<uint32_t, double> VertexVector::ClosestSegment(const Vertex &p) const {
	double min_distance = std::numeric_limits<double>::max();
	uint32_t min_index = 0;
	// Loop over all segments and find the closest one
	auto p1 = Get(0);
	for (uint32_t i = 1; i < count; i++) {
		auto p2 = Get(i);
		auto distance = p.DistanceSquared(p1, p2);
		if (distance < min_distance) {
			min_distance = distance;
			min_index = i - 1;

			if (min_distance == 0) {
				// if the Vertex is on a segment, then we don't have to search any further
				return make_pair(min_index, 0);
			}
		}

		p1 = p2;
	}
	return make_pair(min_index, std::sqrt(min_distance));
}

std::tuple<uint32_t, double> VertexVector::ClosetVertex(const Vertex &p) const {
	double min_distance = std::numeric_limits<double>::max();
	uint32_t min_index = 0;
	// Loop over all segments and find the closest one

	for (uint32_t i = 0; i < count; i++) {
		auto p1 = Get(i);
		auto distance = p.DistanceSquared(p1);
		if (distance < min_distance) {

			min_distance = distance;
			min_index = i;

			if (min_distance == 0) {
				// if the Vertex is on the VertexVector, then we don't have to search any further
				return make_pair(min_index, 0);
			}
		}
	}
	return make_pair(min_index, std::sqrt(min_distance));
}

std::tuple<Vertex, double, double> VertexVector::LocateVertex(const Vertex &p) const {
	if (count == 0) {
		return std::make_tuple(Vertex(), 0, 0);
	}
	if (count == 1) {
		auto single = Get(0);
		return std::make_tuple(single, 0, p.Distance(single));
	}

	auto min_distance = std::numeric_limits<double>::max();
	uint32_t min_index = 0;

	auto p1 = Get(0);
	auto p2 = Get(1);
	// Search for the closest segment
	for (uint32_t i = 1; i < count; i++) {
		p2 = Get(i);
		auto seg_distance = p.DistanceSquared(p1, p2);
		if (seg_distance < min_distance) {
			min_distance = seg_distance;
			min_index = i - 1;
			if (min_distance == 0) {
				// if the Vertex is on a segment, then we don't have to search any further
				break;
			}
		}
		p1 = p2;
	}

	min_distance = std::sqrt(min_distance);
	// Now we have the closest segment, find the closest Vertex on that segment
	auto closest_Vertex = ClosestPointOnSegment(p, p1, p2);

	// Now we have the closest Vertex, find the distance from the start of the segment
	auto total_length = Length();
	if (total_length == 0) {
		// if the VertexVector is a Vertex, then the closest Vertex is the Vertex itself
		return std::make_tuple(closest_Vertex, 0, min_distance);
	}
	auto Vertex_length = 0.0;
	for (uint32_t i = 0; i < min_index; i++) {
		p1 = Get(i);
		p2 = Get(i + 1);
		Vertex_length += p1.Distance(p2);
	}

	auto location = Vertex_length / total_length;
	return std::make_tuple(closest_Vertex, location, min_distance);
}

// utils
Vertex ClosestPointOnSegment(const Vertex &p, const Vertex &p1, const Vertex &p2) {
	// If the segment is a Vertex, then return that Vertex
	if (p1 == p2) {
		return p1;
	}
	double r = ((p.x - p1.x) * (p2.x - p1.x) + (p.y - p1.y) * (p2.y - p1.y)) /
	           ((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y));
	// If r is less than 0, then the Vertex is outside the segment in the p1 direction
	if (r <= 0) {
		return p1;
	}
	// If r is greater than 1, then the Vertex is outside the segment in the p2 direction
	if (r >= 1) {
		return p2;
	}
	// Interpolate between p1 and p2
	return Vertex(p1.x + r * (p2.x - p1.x), p1.y + r * (p2.y - p1.y));
}

} // namespace core

} // namespace spatial