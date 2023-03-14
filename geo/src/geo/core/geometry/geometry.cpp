#include "geo/common.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/vertex_vector.hpp"
namespace geo {

namespace core {

//--------------------
// Point
//--------------------
string Point::ToString() const {
	return "POINT (" + std::to_string(data[0].x) + " " + std::to_string(data[0].y) + ")";
}

uint32_t Point::SerializedSize() const {
	return data.SerializedSize();
}


//--------------------
// LineString
//--------------------
double LineString::Length() const {
	return points.Length();
}

Geometry LineString::Centroid() const {
	throw NotImplementedException("Centroid not implemented for LineString");
}

string LineString::ToString() const {
	string result = "LINESTRING (";
	for (uint32_t i = 0; i < points.Count(); i++) {
		result += std::to_string(points[i].x) + " " + std::to_string(points[i].y);
		if (i < points.Count() - 1) {
			result += ", ";
		}
	}
	result += ")";
	return result;
}

uint32_t LineString::SerializedSize() const {
	return points.SerializedSize();
}

//--------------------
// Polygon
//--------------------
double Polygon::Area() const {
	double area = 0;
	for (uint32_t i = 0; i < num_rings; i++) {
		area += rings[i].SignedArea();
	}
	return area;
}

double Polygon::Perimiter() const {
	return rings[0].Length();
}

Geometry Polygon::Centroid() const {
	throw NotImplementedException("Polygon::Centroid()");
}

string Polygon::ToString() const {
	string result = "POLYGON (";
	for (uint32_t i = 0; i < num_rings; i++) {
		result += "(";
		for (uint32_t j = 0; j < rings[i].Count(); j++) {
			result += std::to_string(rings[i][j].x) + " " + std::to_string(rings[i][j].y);
			if (j < rings[i].Count() - 1) {
				result += ", ";
			}
		}
		result += ")";
		if (i < num_rings - 1) {
			result += ", ";
		}
	}
	result += ")";
	return result;
}

uint32_t Polygon::SerializedSize() const {
	uint32_t size = sizeof(uint32_t);
	for (uint32_t i = 0; i < num_rings; i++) {
		size += rings[i].SerializedSize();
	}
	return size;
}

//--------------------
// Geometry
//--------------------
string Geometry::ToString() const {
	string result;
	switch (type) {
	case GeometryType::POINT:
		result = point.ToString();
		break;
	case GeometryType::LINESTRING:
		result = linestring.ToString();
		break;
	case GeometryType::POLYGON:
		result = polygon.ToString();
		break;
	default:
		throw NotImplementedException("Geometry::ToString()");
	}
	return result;
}

// Returns the size of the serialized geometry in bytes, excluding the GeometryPrefix
uint32_t Geometry::SerializedSize() const {
	uint32_t result = 0;
	switch (type) {
	case GeometryType::POINT:
		result = point.SerializedSize();
		break;
	case GeometryType::LINESTRING:
		result = linestring.SerializedSize();
		break;
	case GeometryType::POLYGON:
		result = polygon.SerializedSize();
		break;
	default:
		throw NotImplementedException("Geometry::SerializedSize()");
	}
	return result;
}



} // namespace core

} // namespace geo