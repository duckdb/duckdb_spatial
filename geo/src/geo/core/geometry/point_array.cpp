#include "geo/core/geometry/point_array.hpp"

namespace geo {

namespace core {

double Point::Distance(const Point &other) const {
	return std::sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
}

double Point::DistanceSquared(const Point &other) const {
	return (x - other.x) * (x - other.x) + (y - other.y) * (y - other.y);
}

double Point::Distance(const Point &p1, const Point &p2) const {
	return std::sqrt(DistanceSquared(p1, p2));
}

double Point::DistanceSquared(const Point &p1, const Point &p2) const {
	auto p = ClosestPointOnSegment(*this, p1, p2);
	return DistanceSquared(p);
}

double PointArray::Length() const {
	double length = 0;
	for (uint32_t i = 0; i < count - 1; i++) {
		auto &p1 = data[i];
		auto &p2 = data[i + 1];
		length += std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
	}
	return length;
}

double PointArray::SignedArea() const {
	if(count < 3) {
		return 0;
	}
	double area = 0;
	for (uint32_t i = 0; i < count - 1; i++) {
		auto &p1 = data[i];
		auto &p2 = data[i + 1];
		area += p1.x * p2.y - p2.x * p1.y;
	}
	return area * 0.5;
}

double PointArray::Area() const {
	return std::abs(SignedArea());
}

bool PointArray::IsClosed() const {
	if (count == 0) {
		return false;
	}
	if (count == 1) {
		return true;
	}
	return data[0] == data[count - 1];
}

bool PointArray::IsEmpty() const {
	return count == 0;
}

WindingOrder PointArray::GetWindingOrder() const {
	return SignedArea() > 0 ? WindingOrder::COUNTER_CLOCKWISE : WindingOrder::CLOCKWISE;
}

bool PointArray::IsClockwise() const {
	return GetWindingOrder() == WindingOrder::CLOCKWISE;
}

bool PointArray::IsCounterClockwise() const {
	return GetWindingOrder() == WindingOrder::COUNTER_CLOCKWISE;
}

bool PointArray::IsSimple() const {
	throw NotImplementedException("PointArray::IsSimple");
}

Contains PointArray::ContainsPoint(const Point &p, bool ensure_closed) const {

	auto &p1 = data[0];
	auto &p2 = data[count-1];

	if(ensure_closed && p1 != p2) {
		throw InternalException("PointArray::Contains: PointArray is not closed");
	}

	int winding_number = 0;

	for (uint32_t i = 0; i < count; i++) {
		p2 = data[i];
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
		if(side == Side::ON && p.IsOnSegment(p1, p2)) {
			return Contains::ON_EDGE;
		}
		else if(side == Side::LEFT && (p1.y < p.y && p.y <= p2.y)) {
			winding_number++;
		}
		else if(side == Side::RIGHT && (p2.y <= p.y && p.y < p1.y)) {
			winding_number--;
		}
		p1 = p2;
	}
	return winding_number == 0 ? Contains::OUTSIDE : Contains::INSIDE;
}

std::tuple<uint32_t, double> PointArray::ClosestSegment(const Point &p) const {
	double min_distance = std::numeric_limits<double>::max();
	uint32_t min_index = 0;
	// Loop over all segments and find the closest one
	auto &p1 = data[0];
	for (uint32_t i = 1; i < count ; i++) {
		auto &p2 = data[i];
		auto distance = p.DistanceSquared(p1, p2);
		if (distance < min_distance) {
			min_distance = distance;
			min_index = i-1;

			if(min_distance == 0) {
				// if the point is on a segment, then we don't have to search any further
				return make_pair(min_index, 0);
			}
		}

		p1 = p2;
	}
	return make_pair(min_index, std::sqrt(min_distance));
}

std::tuple<uint32_t, double> PointArray::ClosetPoint(const Point &p) const {
	double min_distance = std::numeric_limits<double>::max();
	uint32_t min_index = 0;
	// Loop over all segments and find the closest one

	for (uint32_t i = 0; i < count; i++) {
		auto &p1 = data[i];
		auto distance = p.DistanceSquared(p1);
		if (distance < min_distance) {

			min_distance = distance;
			min_index = i;

			if(min_distance == 0) {
				// if the point is on the pointarray, then we don't have to search any further
				return make_pair(min_index, 0);
			}
		}
	}
	return make_pair(min_index, std::sqrt(min_distance));
}

std::tuple<Point, double, double> PointArray::LocatePoint(const Point &p) const {

	if(count == 0) {
		return std::make_tuple(Point(), 0, 0);
	}
	if (count == 1) {
		auto single = data[0];
		return std::make_tuple(single, 0, p.Distance(single));
	}

	auto min_distance = std::numeric_limits<double>::max();
	uint32_t min_index;

	auto &p1 = data[0];
	auto &p2 = data[1];
	// Search for the closest segment
	for (uint32_t i = 1; i < count; i++) {
		p2 = data[i];
		auto seg_distance = p.DistanceSquared(p1, p2);
		if (seg_distance < min_distance) {
			min_distance = seg_distance;
			min_index = i-1;
			if(min_distance == 0) {
				// if the point is on a segment, then we don't have to search any further
				break;
			}
		}
		p1 = p2;
	}

	min_distance = std::sqrt(min_distance);
	// Now we have the closest segment, find the closest point on that segment
	auto closest_point = ClosestPointOnSegment(p, p1, p2);

	// Now we have the closest point, find the distance from the start of the segment
	auto total_length = Length();
	if (total_length == 0) {
		// if the pointarray is a point, then the closest point is the point itself
		return std::make_tuple(closest_point, 0, min_distance);
	}
	auto point_length = 0.0;
	for (uint32_t i = 0; i < min_index; i++) {
		p1 = data[i];
		p2 = data[i+1];
		point_length += p1.Distance(p2);
	}

	auto location = point_length / total_length;
	return std::make_tuple(closest_point, location, min_distance);
}


// utils
Point ClosestPointOnSegment(const Point &p, const Point &p1, const Point &p2) {
	// If the segment is a point, then return that point
	if(p1 == p2) {
		return p1;
	}
	double r = ((p.x - p1.x) * (p2.x - p1.x) + (p.y - p1.y) * (p2.y - p1.y)) / ((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y));
	// If r is less than 0, then the point is outside the segment in the p1 direction
	if (r <= 0) {
		return p1;
	}
	// If r is greater than 1, then the point is outside the segment in the p2 direction
	if (r >= 1) {
		return p2;
	}
	// Interpolate between p1 and p2
	return Point(p1.x + r * (p2.x - p1.x), p1.y + r * (p2.y - p1.y));
}


} // namespace core

} // namespace geo