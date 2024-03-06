#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"

namespace spatial {

namespace core {

// super illegal lol, we should try to get this exposed upstream.
extern "C" int geos_d2sfixed_buffered_n(double f, uint32_t precision, char *result);

string Utils::format_coord(double d) {
	char buf[25];
	auto len = geos_d2sfixed_buffered_n(d, 15, buf);
	buf[len] = '\0';
	return string {buf};
}

string Utils::format_coord(double x, double y) {
	char buf[51];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y] = '\0';
	return string {buf};
}

string Utils::format_coord(double x, double y, double zm) {
	char buf[76];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y++] = ' ';
	auto res_zm = geos_d2sfixed_buffered_n(zm, 15, buf + res_x + res_y);
	buf[res_x + res_y + res_zm] = '\0';
	return string {buf};
}

string Utils::format_coord(double x, double y, double z, double m) {
	char buf[101];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y++] = ' ';
	auto res_z = geos_d2sfixed_buffered_n(z, 15, buf + res_x + res_y);
	buf[res_x + res_y + res_z++] = ' ';
	auto res_m = geos_d2sfixed_buffered_n(m, 15, buf + res_x + res_y + res_z);
	buf[res_x + res_y + res_z + res_m] = '\0';
	return string {buf};
}

//------------------------------------------------------------------------------
// Point
//------------------------------------------------------------------------------

string Point::ToString() const {
	if (IsEmpty()) {
		return "POINT EMPTY";
	}
	auto vert = vertices.Get(0);
	if (std::isnan(vert.x) && std::isnan(vert.y)) {
		// This is a special case for WKB. WKB does not support empty points,
		// and instead writes a point with NaN coordinates. We therefore need to
		// check for this case and return POINT EMPTY instead to round-trip safely
		return "POINT EMPTY";
	}
	return StringUtil::Format("POINT (%s)", Utils::format_coord(vert.x, vert.y));
}

//------------------------------------------------------------------------------
// LineString
//------------------------------------------------------------------------------

string LineString::ToString() const {
	auto count = vertices.Count();
	if (count == 0) {
		return "LINESTRING EMPTY";
	}

	string result = "LINESTRING (";
	for (uint32_t i = 0; i < vertices.Count(); i++) {
		auto x = vertices.Get(i).x;
		auto y = vertices.Get(i).y;
		result += Utils::format_coord(x, y);
		if (i < vertices.Count() - 1) {
			result += ", ";
		}
	}
	result += ")";
	return result;
}

//------------------------------------------------------------------------------
// Polygon
//------------------------------------------------------------------------------

string Polygon::ToString() const {

	// check if the polygon is empty
	uint32_t total_verts = 0;
	auto num_rings = rings.size();
	for (uint32_t i = 0; i < num_rings; i++) {
		total_verts += rings[i].Count();
	}
	if (total_verts == 0) {
		return "POLYGON EMPTY";
	}

	string result = "POLYGON (";
	for (uint32_t i = 0; i < num_rings; i++) {
		result += "(";
		for (uint32_t j = 0; j < rings[i].Count(); j++) {
			auto x = rings[i].Get(j).x;
			auto y = rings[i].Get(j).y;
			result += Utils::format_coord(x, y);
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
	auto num_points = ItemCount();
	if (num_points == 0) {
		return "MULTIPOINT EMPTY";
	}
	string str = "MULTIPOINT (";
	auto &points = Items();
	for (uint32_t i = 0; i < num_points; i++) {
		auto &point = points[i];
		if (point.IsEmpty()) {
			str += "EMPTY";
		} else {
			auto vert = point.Vertices().Get(0);
			str += Utils::format_coord(vert.x, vert.y);
		}
		if (i < num_points - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

//------------------------------------------------------------------------------
// MultiLineString
//------------------------------------------------------------------------------

string MultiLineString::ToString() const {
	auto count = ItemCount();
	if (count == 0) {
		return "MULTILINESTRING EMPTY";
	}
	string str = "MULTILINESTRING (";

	bool first_line = true;
	for (auto &line : *this) {
		if (first_line) {
			first_line = false;
		} else {
			str += ", ";
		}
		str += "(";
		bool first_vert = true;
		for (uint32_t i = 0; i < line.Vertices().Count(); i++) {
			auto vert = line.Vertices().Get(i);
			if (first_vert) {
				first_vert = false;
			} else {
				str += ", ";
			}
			str += Utils::format_coord(vert.x, vert.y);
		}
		str += ")";
	}
	return str + ")";
}

//------------------------------------------------------------------------------
// MultiPolygon
//------------------------------------------------------------------------------

string MultiPolygon::ToString() const {
	auto count = ItemCount();
	if (count == 0) {
		return "MULTIPOLYGON EMPTY";
	}
	string str = "MULTIPOLYGON (";

	bool first_poly = true;
	for (auto &poly : *this) {
		if (first_poly) {
			first_poly = false;
		} else {
			str += ", ";
		}
		str += "(";
		bool first_ring = true;
		for (auto &ring : poly) {
			if (first_ring) {
				first_ring = false;
			} else {
				str += ", ";
			}
			str += "(";
			bool first_vert = true;
			for (uint32_t v = 0; v < ring.Count(); v++) {
				auto vert = ring.Get(v);
				if (first_vert) {
					first_vert = false;
				} else {
					str += ", ";
				}
				str += Utils::format_coord(vert.x, vert.y);
			}
			str += ")";
		}
		str += ")";
	}

	return str + ")";
}

//------------------------------------------------------------------------------
// GeometryCollection
//------------------------------------------------------------------------------

string GeometryCollection::ToString() const {
	auto count = ItemCount();
	if (count == 0) {
		return "GEOMETRYCOLLECTION EMPTY";
	}
	string str = "GEOMETRYCOLLECTION (";
	for (uint32_t i = 0; i < count; i++) {
		str += Items()[i].ToString();
		if (i < count - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

uint32_t GeometryCollection::Dimension() const {
	uint32_t max = 0;
	for (auto &item : *this) {
		max = std::max(max, item.Dimension());
	}
	return max;
}

GeometryCollection GeometryCollection::DeepCopy() const {
	GeometryCollection copy(*this);
	for (auto &item : copy.Items()) {
		item = item.DeepCopy();
	}
	return copy;
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
		return collection.ToString();
	default:
		throw NotImplementedException("Geometry::ToString()");
	}
}

/*
bool Geometry::IsEmpty() const {
    switch (type) {
    case GeometryType::POINT:
        return point.IsEmpty();
    case GeometryType::LINESTRING:
        return linestring.IsEmpty();
    case GeometryType::POLYGON:
        return polygon.IsEmpty();
    case GeometryType::MULTIPOINT:
        return multipoint.IsEmpty();
    case GeometryType::MULTILINESTRING:
        return multilinestring.IsEmpty();
    case GeometryType::MULTIPOLYGON:
        return multipolygon.IsEmpty();
    case GeometryType::GEOMETRYCOLLECTION:
        return geometrycollection.IsEmpty();
    default:
        throw NotImplementedException("Geometry::IsEmpty()");
    }
}*/

bool Geometry::IsCollection() const {
	switch (type) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING:
	case GeometryType::POLYGON:
		return false;
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION:
		return true;
	default:
		throw NotImplementedException("Geometry::IsCollection()");
	}
}

} // namespace core

} // namespace spatial
