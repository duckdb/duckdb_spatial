#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

class WKBReader {
private:
	ArenaAllocator &arena;
	bool has_any_z;
	bool has_any_m;

	struct WKBType {
		GeometryType type;
		bool has_z;
		bool has_m;
	};

	// Primitives
	uint32_t ReadInt(Cursor &cursor, bool little_endian);
	double ReadDouble(Cursor &cursor, bool little_endian);
	WKBType ReadType(Cursor &cursor, bool little_endian);
	void ReadVertices(Cursor &cursor, bool little_endian, bool has_z, bool has_m, Geometry &geometry);

	// Geometries
	Geometry ReadPoint(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadLineString(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadPolygon(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadMultiPoint(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadMultiLineString(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadMultiPolygon(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadGeometryCollection(Cursor &cursor, bool little_endian, bool has_z, bool has_m);
	Geometry ReadGeometry(Cursor &cursor);

public:
	explicit WKBReader(ArenaAllocator &arena) : arena(arena) {
	}
	Geometry Deserialize(const string_t &wkb);
	Geometry Deserialize(const_data_ptr_t wkb, uint32_t size);
	bool GeomHasZ() const {
		return has_any_z;
	}
	bool GeomHasM() const {
		return has_any_m;
	};
};

} // namespace core

} // namespace spatial
