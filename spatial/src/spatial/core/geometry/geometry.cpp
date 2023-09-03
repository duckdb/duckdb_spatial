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
	return string(buf);
}

string Utils::format_coord(double x, double y) {
	char buf[51];
	auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
	buf[res_x++] = ' ';
	auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
	buf[res_x + res_y] = '\0';
	return string(buf);
}

//------------------------------------------------------------------------------
// Point
//------------------------------------------------------------------------------

string Point::ToString() const {
	if (IsEmpty()) {
		return "POINT EMPTY";
	}
	auto &vert = vertices[0];
	if (std::isnan(vert.x) && std::isnan(vert.y)) {
		// This is a special case for WKB. WKB does not support empty points,
		// and instead writes a point with NaN coordinates. We therefore need to
		// check for this case and return POINT EMPTY instead to round-trip safely
		return "POINT EMPTY";
	}
	auto x = vertices[0].x;
	auto y = vertices[0].y;
	return StringUtil::Format("POINT (%s)", Utils::format_coord(x, y));
}

bool Point::IsEmpty() const {
	return vertices.Count() == 0;
}

Vertex &Point::GetVertex() {
	return vertices[0];
}

const Vertex &Point::GetVertex() const {
	return vertices[0];
}

Point::operator Geometry() const {
	return Geometry(*this);
}

//------------------------------------------------------------------------------
// LineString
//------------------------------------------------------------------------------

double LineString::Length() const {
	return vertices.Length();
}

bool LineString::IsEmpty() const {
	return vertices.Count() == 0;
}

Geometry LineString::Centroid() const {
	throw NotImplementedException("Centroid not implemented for LineString");
}

string LineString::ToString() const {
	auto count = vertices.Count();
	if (count == 0) {
		return "LINESTRING EMPTY";
	}

	string result = "LINESTRING (";
	for (uint32_t i = 0; i < vertices.Count(); i++) {
		auto x = vertices[i].x;
		auto y = vertices[i].y;
		result += Utils::format_coord(x, y);
		if (i < vertices.Count() - 1) {
			result += ", ";
		}
	}
	result += ")";
	return result;
}

uint32_t LineString::Count() const {
	return vertices.Count();
}

LineString::operator Geometry() const {
	return Geometry(*this);
}

//------------------------------------------------------------------------------
// Polygon
//------------------------------------------------------------------------------

double Polygon::Area() const {
	double area = 0;
	for (uint32_t i = 0; i < num_rings; i++) {
		if (i == 0) {
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
	if (IsEmpty()) {
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
	if (total_verts == 0) {
		return "POLYGON EMPTY";
	}

	string result = "POLYGON (";
	for (uint32_t i = 0; i < num_rings; i++) {
		result += "(";
		for (uint32_t j = 0; j < rings[i].Count(); j++) {
			auto x = rings[i][j].x;
			auto y = rings[i][j].y;
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

uint32_t Polygon::Count() const {
	return num_rings;
}

Polygon::operator Geometry() const {
	return Geometry(*this);
}

//------------------------------------------------------------------------------
// MultiPoint
//------------------------------------------------------------------------------

string MultiPoint::ToString() const {
	if (num_points == 0) {
		return "MULTIPOINT EMPTY";
	}
	string str = "MULTIPOINT (";
	for (uint32_t i = 0; i < num_points; i++) {
		if (points[i].IsEmpty()) {
			str += "EMPTY";
		} else {
			auto &vert = points[i].GetVertex();
			str += Utils::format_coord(vert.x, vert.y);
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

uint32_t MultiPoint::Count() const {
	return num_points;
}

Point &MultiPoint::operator[](uint32_t index) {
	return points[index];
}

const Point &MultiPoint::operator[](uint32_t index) const {
	return points[index];
}

const Point *MultiPoint::begin() const {
	return points;
}
const Point *MultiPoint::end() const {
	return points + num_points;
}
Point *MultiPoint::begin() {
	return points;
}
Point *MultiPoint::end() {
	return points + num_points;
}

MultiPoint::operator Geometry() const {
	return Geometry(*this);
}

//------------------------------------------------------------------------------
// MultiLineString
//------------------------------------------------------------------------------

string MultiLineString::ToString() const {
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
		for (auto &vert : line.Vertices()) {
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

double MultiLineString::Length() const {
	double length = 0;
	for (uint32_t i = 0; i < count; i++) {
		length += lines[i].Length();
	}
	return length;
}

bool MultiLineString::IsEmpty() const {
	return count == 0;
}

uint32_t MultiLineString::Count() const {
	return count;
}

LineString &MultiLineString::operator[](uint32_t index) {
	return lines[index];
}

const LineString &MultiLineString::operator[](uint32_t index) const {
	return lines[index];
}

const LineString *MultiLineString::begin() const {
	return lines;
}
const LineString *MultiLineString::end() const {
	return lines + count;
}
LineString *MultiLineString::begin() {
	return lines;
}
LineString *MultiLineString::end() {
	return lines + count;
}

MultiLineString::operator Geometry() const {
	return Geometry(*this);
}

//------------------------------------------------------------------------------
// MultiPolygon
//------------------------------------------------------------------------------

string MultiPolygon::ToString() const {
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
		for (auto &ring : poly.Rings()) {
			if (first_ring) {
				first_ring = false;
			} else {
				str += ", ";
			}
			str += "(";
			bool first_vert = true;
			for (auto &vert : ring) {
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
double MultiPolygon::Area() const {
	double area = 0;
	for (uint32_t i = 0; i < count; i++) {
		area += polygons[i].Area();
	}
	return area;
}

bool MultiPolygon::IsEmpty() const {
	return count == 0;
}

uint32_t MultiPolygon::Count() const {
	return count;
}

Polygon &MultiPolygon::operator[](uint32_t index) {
	return polygons[index];
}

const Polygon &MultiPolygon::operator[](uint32_t index) const {
	return polygons[index];
}

const Polygon *MultiPolygon::begin() const {
	return polygons;
}

const Polygon *MultiPolygon::end() const {
	return polygons + count;
}

Polygon *MultiPolygon::begin() {
	return polygons;
}

Polygon *MultiPolygon::end() {
	return polygons + count;
}

MultiPolygon::operator Geometry() const {
	return Geometry(*this);
}

//------------------------------------------------------------------------------
// GeometryCollection
//------------------------------------------------------------------------------

string GeometryCollection::ToString() const {
	if (count == 0) {
		return "GEOMETRYCOLLECTION EMPTY";
	}
	string str = "GEOMETRYCOLLECTION (";
	for (uint32_t i = 0; i < count; i++) {
		str += geometries[i].ToString();
		if (i < count - 1) {
			str += ", ";
		}
	}
	return str + ")";
}

bool GeometryCollection::IsEmpty() const {
	return count == 0;
}

uint32_t GeometryCollection::Count() const {
	return count;
}

Geometry &GeometryCollection::operator[](uint32_t index) {
	D_ASSERT(index < count);
	return geometries[index];
}

const Geometry &GeometryCollection::operator[](uint32_t index) const {
	D_ASSERT(index < count);
	return geometries[index];
}

const Geometry *GeometryCollection::begin() const {
	return geometries;
}

const Geometry *GeometryCollection::end() const {
	return geometries + count;
}

Geometry *GeometryCollection::begin() {
	return geometries;
}

Geometry *GeometryCollection::end() {
	return geometries + count;
}

GeometryCollection::operator Geometry() const {
	return Geometry(*this);
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

int32_t Geometry::Dimension() const {
	switch (type) {
	case GeometryType::POINT:
		return 0;
	case GeometryType::LINESTRING:
		return 1;
	case GeometryType::POLYGON:
		return 2;
	case GeometryType::MULTIPOINT:
		return 0;
	case GeometryType::MULTILINESTRING:
		return 1;
	case GeometryType::MULTIPOLYGON:
		return 2;
	case GeometryType::GEOMETRYCOLLECTION:
		return geometrycollection.Aggregate([](Geometry &geom, int32_t d) { return std::max(geom.Dimension(), d); }, 0);
	default:
		throw NotImplementedException("Geometry::Dimension()");
	}
}

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
}

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