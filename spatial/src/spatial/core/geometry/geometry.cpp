#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Point
//------------------------------------------------------------------------------

string Point::ToString() const {
	if(IsEmpty()) {
        return "POINT EMPTY";
    }
	auto &vert = data[0];
	if (std::isnan(vert.x) && std::isnan(vert.y)) {
		// This is a special case for WKB. WKB does not support empty points,
		// and instead writes a point with NaN coordinates. We therefore need to
		// check for this case and return POINT EMPTY instead to round-trip safely
		return "POINT EMPTY";
	}
    return "POINT (" + std::to_string(data[0].x) + " " + std::to_string(data[0].y) + ")";
}

//------------------------------------------------------------------------------
// LineString
//------------------------------------------------------------------------------

double LineString::Length() const {
	return points.Length();
}

bool LineString::IsEmpty() const {
	return points.Count() == 0;
}

Geometry LineString::Centroid() const {
	throw NotImplementedException("Centroid not implemented for LineString");
}

string LineString::ToString() const {
	auto count = points.Count();
	if(count == 0) {
        return "LINESTRING EMPTY";
    }

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

uint32_t LineString::Count() const {
	return points.Count();
}

//------------------------------------------------------------------------------
// Polygon
//------------------------------------------------------------------------------

double Polygon::Area() const {
	double area = 0;
	for (uint32_t i = 0; i < num_rings; i++) {
		if(i == 0) {
			area += rings[i].SignedArea();
		} else {
			area -= rings[i].SignedArea();
		}
	}
	return std::abs(area);
}

bool Polygon::IsEmpty() const {
	return num_rings == 0;
}

double Polygon::Perimiter() const {
	if(IsEmpty()) {
        return 0;
    }
	return rings[0].Length();
}

Geometry Polygon::Centroid() const {
	throw NotImplementedException("Polygon::Centroid()");
}

string Polygon::ToString() const {

	// check if the polygon is empty
	uint32_t total_verts = 0;
	for (uint32_t i = 0; i < num_rings; i++) {
        total_verts += rings[i].Count();
    }
	if(total_verts == 0) {
        return "POLYGON EMPTY";
    }

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

//------------------------------------------------------------------------------
// MultiPoint
//------------------------------------------------------------------------------

string MultiPoint::ToString() const {
	if(num_points == 0) {
        return "MULTIPOINT EMPTY";
    }
	string str = "MULTIPOINT (";
	for (uint32_t i = 0; i < num_points; i++) {
		if(points[i].IsEmpty()) {
			str += "EMPTY";
        } else {
			auto &vert = points[i].GetVertex();
			str += std::to_string(vert.x) + " " + std::to_string(vert.y);
		}
        if (i < num_points - 1) {
            str += ", ";
        }
	}
	return str + ")";
}

bool MultiPoint::IsEmpty() const {
    return num_points == 0;
}

//------------------------------------------------------------------------------
// MultiLineString
//------------------------------------------------------------------------------

string MultiLineString::ToString() const {
	if(num_linestrings == 0) {
        return "MULTILINESTRING EMPTY";
    }
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


double MultiLineString::Length() const {
    double length = 0;
    for (uint32_t i = 0; i < num_linestrings; i++) {
        length += linestrings[i].Length();
    }
    return length;
}

bool MultiLineString::IsEmpty() const {
    return num_linestrings == 0;
}

//------------------------------------------------------------------------------
// MultiPolygon
//------------------------------------------------------------------------------

string MultiPolygon::ToString() const {
	if(num_polygons == 0) {
        return "MULTIPOLYGON EMPTY";
    }
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

bool MultiPolygon::IsEmpty() const {
    return num_polygons == 0;
}

double MultiPolygon::Area() const {
	double area = 0;
	for (uint32_t i = 0; i < num_polygons; i++) {
		area += polygons[i].Area();
	}
	return area;
}

//------------------------------------------------------------------------------
// GeometryCollection
//------------------------------------------------------------------------------

string GeometryCollection::ToString() const {
	if(num_geometries == 0) {
        return "GEOMETRYCOLLECTION EMPTY";
    }
	string str = "GEOMETRYCOLLECTION (";
	for (uint32_t i = 0; i < num_geometries; i++) {
		str += geometries[i].ToString();
		if (i < num_geometries - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

bool GeometryCollection::IsEmpty() const {
    return num_geometries == 0;
}

//------------------------------------------------------------------------------
// Geometry
//------------------------------------------------------------------------------
string Geometry::ToString() const {
	switch (type) {
	case GeometryType::POINT:
		return point.ToString();
	case GeometryType::LINESTRING:
		return linestring.ToString();
    case GeometryType::POLYGON:
        return polygon.ToString();
    case GeometryType::MULTIPOINT:
        return multipoint.ToString();
    case GeometryType::MULTILINESTRING:
        return multilinestring.ToString();
    case GeometryType::MULTIPOLYGON:
        return multipolygon.ToString();
    case GeometryType::GEOMETRYCOLLECTION:
        return geometrycollection.ToString();
	default:
		throw NotImplementedException("Geometry::ToString()");
	}
}


} // namespace core

} // namespace spatial