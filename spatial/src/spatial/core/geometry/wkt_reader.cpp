#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/wkt_reader.hpp"
#include "fast_float/fast_float.h"

namespace spatial {

namespace core {

// TODO: Support the full EWKT spec (e.g. SRID and other metadata)
// TODO: Support better error messages using DuckDBs new error context
string WKTReader::GetErrorContext() {
	// Return a string of the current position in the input string
	const auto len = 32;
	const auto msg_start = std::max(cursor - len, start);
	const auto msg_end = std::min(cursor + 1, end);
	auto msg = string(msg_start, msg_end);
	if (msg_start != start) {
		msg = "..." + msg;
	}
	// Add an arrow to indicate the position
	msg = "at position " + std::to_string(cursor - start) + " near: '" + msg + "'|<---";
	return msg;
}

bool WKTReader::TryParseDouble(double &data) {
	auto result = duckdb_fast_float::from_chars<double>(cursor, end, data);
	if (result.ec == std::errc()) {
		cursor = result.ptr;
		while (cursor < end && std::isspace(*cursor)) {
			cursor++;
		}
		return true;
	} else {
		return false;
	}
}

double WKTReader::ParseDouble() {
	double result;
	if (!TryParseDouble(result)) {
		auto msg = "WKT Parser: Expected double " + GetErrorContext();
		throw InvalidInputException(msg);
	}
	return result;
};

string WKTReader::ParseWord() {
	auto pos = cursor;
	while (cursor < end && !std::isspace(*cursor) && std::isalnum(*cursor)) {
		cursor++;
	}
	return string(pos, cursor);
}

bool WKTReader::Match(char c) {
	if (*cursor == c) {
		cursor++;
		while (cursor < end && std::isspace(*cursor)) {
			cursor++;
		}
		return true;
	} else {
		return false;
	}
}

bool WKTReader::MatchCI(const char *str) {
	auto pos = cursor;
	while (*str) {
		if (std::tolower(*str) != std::tolower(*cursor)) {
			cursor = pos;
			return false;
		}
		str++;
		cursor++;
	}
	while (cursor < end && std::isspace(*cursor)) {
		cursor++;
	}
	return true;
}

void WKTReader::Expect(char c) {
	if (!Match(c)) {
		auto msg = "WKT Parser: Expected character '" + string(1, c) + "' " + GetErrorContext();
		throw InvalidInputException(msg);
	}
}

void WKTReader::ParseVertex(vector<double> &coords) {
	double x, y, z, m;
	x = ParseDouble();
	y = ParseDouble();
	if (has_z) {
		z = ParseDouble();
	}
	if (has_m) {
		m = ParseDouble();
	}
	coords.push_back(x);
	coords.push_back(y);
	if (has_z) {
		coords.push_back(z);
	}
	if (has_m) {
		coords.push_back(m);
	}
}

pair<uint32_t, vector<double>> WKTReader::ParseVertices() {
	vector<double> coords;
	if (MatchCI("EMPTY")) {
		return {0, coords};
	}
	Expect('(');
	uint32_t count = 0;
	ParseVertex(coords);
	count++;
	while (Match(',')) {
		ParseVertex(coords);
		count++;
	}
	Expect(')');
	return {count, coords};
}

Geometry WKTReader::ParsePoint() {
	if (MatchCI("EMPTY")) {
		return Point::CreateEmpty(has_z, has_m);
	}
	Expect('(');
	vector<double> coords;
	ParseVertex(coords);
	Expect(')');
	return Point::CreateFromCopy(arena, data_ptr_cast(coords.data()), 1, has_z, has_m);
}

Geometry WKTReader::ParseLineString() {
	auto verts = ParseVertices();
	return LineString::CreateFromCopy(arena, data_ptr_cast(verts.second.data()), verts.first, has_z, has_m);
}

Geometry WKTReader::ParsePolygon() {
	if (MatchCI("EMPTY")) {
		return Polygon::CreateEmpty(has_z, has_m);
	}
	Expect('(');
	vector<pair<uint32_t, vector<double>>> rings;
	rings.push_back(ParseVertices());
	while (Match(',')) {
		rings.push_back(ParseVertices());
	}
	Expect(')');
	auto result = Polygon::Create(arena, rings.size(), has_z, has_m);
	for (uint32_t i = 0; i < rings.size(); i++) {
		auto &ring = Polygon::Part(result, i);
		LineString::CopyData(ring, arena, data_ptr_cast(rings[i].second.data()), rings[i].first);
	}
	return result;
}

Geometry WKTReader::ParseMultiPoint() {
	if (MatchCI("EMPTY")) {
		return MultiPoint::CreateEmpty(has_z, has_m);
	}
	// Multipoints are special in that parens around each point is optional.
	Expect('(');
	vector<double> coords;
	vector<Geometry> points;
	bool optional_paren = false;

	if (Match('(')) {
		optional_paren = true;
	}
	ParseVertex(coords);
	if (optional_paren) {
		Expect(')');
		optional_paren = false;
	}
	points.push_back(Point::CreateFromCopy(arena, data_ptr_cast(coords.data()), 1, has_z, has_m));
	coords.clear();
	while (Match(',')) {
		if (Match('(')) {
			optional_paren = true;
		}
		ParseVertex(coords);
		if (optional_paren) {
			Expect(')');
			optional_paren = false;
		}
		points.push_back(Point::CreateFromCopy(arena, data_ptr_cast(coords.data()), 1, has_z, has_m));
		coords.clear();
	}
	Expect(')');
	auto result = MultiPoint::Create(arena, points.size(), has_z, has_m);
	for (uint32_t i = 0; i < points.size(); i++) {
		MultiPoint::Part(result, i) = points[i];
	}
	return result;
}

Geometry WKTReader::ParseMultiLineString() {
	if (MatchCI("EMPTY")) {
		return MultiLineString::CreateEmpty(has_z, has_m);
	}
	Expect('(');
	vector<Geometry> lines;
	lines.push_back(ParseLineString());
	while (Match(',')) {
		lines.push_back(ParseLineString());
	}
	Expect(')');
	auto result = MultiLineString::Create(arena, lines.size(), has_z, has_m);
	for (uint32_t i = 0; i < lines.size(); i++) {
		MultiLineString::Part(result, i) = lines[i];
	}
	return result;
}

Geometry WKTReader::ParseMultiPolygon() {
	if (MatchCI("EMPTY")) {
		return MultiPolygon::CreateEmpty(has_z, has_m);
	}
	Expect('(');
	vector<Geometry> polygons;
	polygons.push_back(ParsePolygon());
	while (Match(',')) {
		polygons.push_back(ParsePolygon());
	}
	Expect(')');
	auto result = MultiPolygon::Create(arena, polygons.size(), has_z, has_m);
	for (uint32_t i = 0; i < polygons.size(); i++) {
		MultiPolygon::Part(result, i) = polygons[i];
	}
	return result;
}

Geometry WKTReader::ParseGeometryCollection() {
	if (MatchCI("EMPTY")) {
		return GeometryCollection::CreateEmpty(has_z, has_m);
	}
	Expect('(');
	vector<Geometry> geometries;
	geometries.push_back(ParseGeometry());
	while (Match(',')) {
		geometries.push_back(ParseGeometry());
	}
	Expect(')');
	auto result = GeometryCollection::Create(arena, geometries.size(), has_z, has_m);
	for (uint32_t i = 0; i < geometries.size(); i++) {
		GeometryCollection::Part(result, i) = geometries[i];
	}
	return result;
}

void WKTReader::CheckZM() {
	bool geom_has_z = false;
	bool geom_has_m = false;
	if (Match('Z')) {
		geom_has_z = true;
		if (Match('M')) {
			geom_has_m = true;
		}
	} else if (Match('M')) {
		geom_has_m = true;
	}

	if (zm_set) {
		if (has_z != geom_has_z || has_m != geom_has_m) {
			auto msg = "WKT Parser: GeometryCollection with mixed Z and M types are not supported, mismatch " +
			           GetErrorContext();
			throw InvalidInputException(msg);
		}
	} else {
		has_z = geom_has_z;
		has_m = geom_has_m;
		zm_set = true;
	}
}

Geometry WKTReader::ParseGeometry() {
	if (MatchCI("POINT")) {
		CheckZM();
		return ParsePoint();
	}
	if (MatchCI("LINESTRING")) {
		CheckZM();
		return ParseLineString();
	}
	if (MatchCI("POLYGON")) {
		CheckZM();
		return ParsePolygon();
	}
	if (MatchCI("MULTIPOINT")) {
		CheckZM();
		return ParseMultiPoint();
	}
	if (MatchCI("MULTILINESTRING")) {
		CheckZM();
		return ParseMultiLineString();
	}
	if (MatchCI("MULTIPOLYGON")) {
		CheckZM();
		return ParseMultiPolygon();
	}
	if (MatchCI("GEOMETRYCOLLECTION")) {
		CheckZM();
		return ParseGeometryCollection();
	}
	auto context = GetErrorContext();
	auto msg = "WKT Parser: Unknown geometry type '" + ParseWord() + "' " + context;
	throw InvalidInputException(msg);
}

Geometry WKTReader::ParseWKT() {
	// TODO: Handle EWKT properly. This is just a temporary fix to ignore SRID
	if (MatchCI("SRID")) {
		// Discard everything until the next semicolon
		while (cursor < end && *cursor != ';') {
			cursor++;
		}
		Expect(';');
		while (cursor < end && std::isspace(*cursor)) {
			cursor++;
		}
	}
	return ParseGeometry();
}

Geometry WKTReader::Parse(const string_t &wkt) {
	start = wkt.GetDataUnsafe();
	cursor = wkt.GetDataUnsafe();
	end = wkt.GetDataUnsafe() + wkt.GetSize();
	zm_set = false;
	has_z = false;
	has_m = false;
	auto geom = ParseWKT();
	return geom;
}

} // namespace core

} // namespace spatial
