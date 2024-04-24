#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

class WKTReader {
private:
	ArenaAllocator &arena;
	const char *cursor;
	const char *start;
	const char *end;
	bool zm_set;
	bool has_z;
	bool has_m;

	string GetErrorContext();
	bool TryParseDouble(double &data);
	double ParseDouble();
	string ParseWord();
	bool Match(char c);
	bool MatchCI(const char *str);
	void Expect(char c);
	void ParseVertex(vector<double> &coords);
	pair<uint32_t, vector<double>> ParseVertices();

	Geometry ParsePoint();
	Geometry ParseLineString();
	Geometry ParsePolygon();
	Geometry ParseMultiPoint();
	Geometry ParseMultiLineString();
	Geometry ParseMultiPolygon();
	Geometry ParseGeometryCollection();
	void CheckZM();
	Geometry ParseGeometry();
	Geometry ParseWKT();

public:
	explicit WKTReader(ArenaAllocator &arena) : arena(arena), cursor(nullptr) {
	}
	bool GeomHasZ() const {
		return has_z;
	}
	bool GeomHasM() const {
		return has_m;
	}
	Geometry Parse(const string_t &wkt);
};

} // namespace core

} // namespace spatial
