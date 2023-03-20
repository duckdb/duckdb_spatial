#include "geo/common.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/vertex_vector.hpp"
namespace geo {

namespace core {

//------------------------------------------------------------------------------
// Point
//------------------------------------------------------------------------------

string Point::ToString() const {
	return "POINT (" + std::to_string(data[0].x) + " " + std::to_string(data[0].y) + ")";
}

uint32_t Point::SerializedSize() const {
	return data.SerializedSize();
}

//------------------------------------------------------------------------------
// LineString
//------------------------------------------------------------------------------

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
	// 4 bytes for the number of points
	return 4 + points.SerializedSize();
}

//------------------------------------------------------------------------------
// Polygon
//------------------------------------------------------------------------------

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
	uint32_t size = sizeof(uint32_t); // 4 bytes for the number of rings
	for (uint32_t i = 0; i < num_rings; i++) {
		size += sizeof(uint32_t);          // 4 bytes for the number of points in the ring
		size += rings[i].SerializedSize(); // size of the ring
	}
	return size;
}
//------------------------------------------------------------------------------
// MultiPoint
//------------------------------------------------------------------------------

string MultiPoint::ToString() const {
	string str = "MULTIPOINT (";
	for (uint32_t i = 0; i < num_points; i++) {
		str += std::to_string(points[i].X()) + " " + std::to_string(points[i].Y());
		if (i < num_points - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

uint32_t MultiPoint::SerializedSize() const {
	return sizeof(uint32_t) + num_points * sizeof(Point); // TODO: this is wrong
}

//------------------------------------------------------------------------------
// MultiLineString
//------------------------------------------------------------------------------

string MultiLineString::ToString() const {
	string str = "MULTILINESTRING (";
	for (uint32_t i = 0; i < num_linestrings; i++) {
		str += "(";
		for (uint32_t j = 0; j < linestrings[i].points.count; j++) {
			str += std::to_string(linestrings[i].points[j].x) + " " + std::to_string(linestrings[i].points[j].y);
			if (j < linestrings[i].points.count - 1) {
				str += ", ";
			}
		}
		str += ")";
		if (i < num_linestrings - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

uint32_t MultiLineString::SerializedSize() const {
	uint32_t size = sizeof(uint32_t); // 4 bytes for the number of linestrings
	for (uint32_t i = 0; i < num_linestrings; i++) {
		size += linestrings[i].SerializedSize();
	}
	return size;
}

//------------------------------------------------------------------------------
// MultiPolygon
//------------------------------------------------------------------------------

string MultiPolygon::ToString() const {
	string str = "MULTIPOLYGON (";
	for (uint32_t i = 0; i < num_polygons; i++) {
		str += "(";
		for (uint32_t j = 0; j < polygons[i].num_rings; j++) {
			str += "(";
			for (uint32_t k = 0; k < polygons[i].rings[j].count; k++) {
				str += std::to_string(polygons[i].rings[j][k].x) + " " + std::to_string(polygons[i].rings[j][k].y);
				if (k < polygons[i].rings[j].count - 1) {
					str += ", ";
				}
			}
			str += ")";
			if (j < polygons[i].num_rings - 1) {
				str += ", ";
			}
		}
		str += ")";
		if (i < num_polygons - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

uint32_t MultiPolygon::SerializedSize() const {
	uint32_t size = sizeof(uint32_t); // 4 bytes for the number of polygons
	for (uint32_t i = 0; i < num_polygons; i++) {
		size += polygons[i].SerializedSize();
	}
	return size;
}

//------------------------------------------------------------------------------
// GeometryCollection
//------------------------------------------------------------------------------

string GeometryCollection::ToString() const {
	string str = "GEOMETRYCOLLECTION (";
	for (uint32_t i = 0; i < num_geometries; i++) {
		str += geometries[i].ToString();
		if (i < num_geometries - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

uint32_t GeometryCollection::SerializedSize() const {
	uint32_t size = sizeof(uint32_t); // 4 bytes for the number of geometries
	for (uint32_t i = 0; i < num_geometries; i++) {
		size += geometries[i].SerializedSize();
	}
	return size;
}

//------------------------------------------------------------------------------
// Geometry
//------------------------------------------------------------------------------
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