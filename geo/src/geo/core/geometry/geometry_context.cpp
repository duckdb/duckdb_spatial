#include "geo/common.hpp"
#include "geo/core/geometry/geometry_context.hpp"
#include "geo/core/geometry/geometry.hpp"

namespace geo {

namespace core {

Geometry GeometryContext::FromWKT(const char* wkt, uint32_t length) {
	throw NotImplementedException("WKT not implemented yet");
}

string GeometryContext::ToWKT(const Geometry &geometry) {
	throw NotImplementedException("WKT not implemented yet");
}

enum class WKBByteOrder : uint8_t {
	XDR = 0, // Big endian
	NDR = 1, // Little endian
};

enum class WKBGeometryType : uint32_t {
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7
};

struct WKBReader {
private:
	const char* data;
	uint32_t length;
	uint32_t cursor;
	GeometryContext& ctx;
public:
	template<WKBByteOrder ORDER>
	uint32_t ReadInt() = delete;

	template<WKBByteOrder ORDER>
	double ReadDouble() = delete;

	WKBReader(const char* data, uint32_t length, GeometryContext &ctx) :
		data(data), length(length), cursor(0), ctx(ctx) {
	}

	Geometry Read() {
		cursor = 0;
		auto order = static_cast<WKBByteOrder>(data[cursor++]);
		switch (order) {
		case WKBByteOrder::XDR: return ReadGeometry<WKBByteOrder::XDR>();
		case WKBByteOrder::NDR: return ReadGeometry<WKBByteOrder::NDR>();
		default: throw NotImplementedException("Unknown WKB byte order");
		}
	}
private:
	template<WKBByteOrder ORDER>
	Geometry ReadGeometry() {
		auto type = static_cast<WKBGeometryType>(ReadInt<ORDER>());
		switch (type) {
		case WKBGeometryType::POINT: return ReadPoint<ORDER>();
		case WKBGeometryType::LINESTRING: return ReadLineString<ORDER>();
		case WKBGeometryType::POLYGON: return ReadPolygon<ORDER>();
		case WKBGeometryType::MULTIPOINT: return ReadMultiPoint<ORDER>();
		case WKBGeometryType::MULTILINESTRING: return ReadMultiLineString<ORDER>();
		case WKBGeometryType::MULTIPOLYGON: return ReadMultiPolygon<ORDER>();
		case WKBGeometryType::GEOMETRYCOLLECTION: return ReadGeometryCollection<ORDER>();
		default: throw NotImplementedException("Unknown WKB geometry type");
		}
	}

	template<WKBByteOrder ORDER>
	Geometry ReadPoint() {
		auto x = ReadDouble<ORDER>();
		auto y = ReadDouble<ORDER>();
		auto point_data = ctx.AllocateVertexVector(1);
		point_data.Add(Vertex(x, y));
		return Geometry(Point(std::move(point_data)));
	}

	template<WKBByteOrder ORDER>
	Geometry ReadLineString() {
		auto num_points = ReadInt<ORDER>();
		auto line_data = ctx.AllocateVertexVector(num_points);
		for (uint32_t i = 0; i < num_points; i++) {
			auto x = ReadDouble<ORDER>();
			auto y = ReadDouble<ORDER>();
			line_data.Add(Vertex(x, y));
		}
		return Geometry(LineString(std::move(line_data)));
	}

	template<WKBByteOrder ORDER>
	Geometry ReadPolygon() {
		auto num_rings = ReadInt<ORDER>();
		auto rings = reinterpret_cast<VertexVector*>(ctx.allocator.Allocate(sizeof(VertexVector) * num_rings));

		for (uint32_t i = 0; i < num_rings; i++) {
			auto num_points = ReadInt<ORDER>();
			rings[i] = ctx.AllocateVertexVector(num_points);
			auto &ring = rings[i];

			for (uint32_t j = 0; j < num_points; j++) {
				auto x = ReadDouble<ORDER>();
				auto y = ReadDouble<ORDER>();
				ring.Add(Vertex(x, y));
			}
		}

		return Geometry(Polygon(rings, num_rings));
	}

	template<WKBByteOrder ORDER>
	Geometry ReadMultiPoint() {
		throw NotImplementedException("ReadMultiPoint");
	}

	template<WKBByteOrder ORDER>
	Geometry ReadMultiLineString() {
		throw NotImplementedException("ReadMultiLineString");
	}

	template<WKBByteOrder ORDER>
	Geometry ReadMultiPolygon() {
		throw NotImplementedException("ReadMultiPolygon");
	}

	template<WKBByteOrder ORDER>
	Geometry ReadGeometryCollection() {
		throw NotImplementedException("ReadGeometryCollection");
	}
};

template<>
uint32_t WKBReader::ReadInt<WKBByteOrder::NDR>() {
	D_ASSERT(cursor + sizeof(uint32_t) <= length);
	// Read uint32_t in little endian
	uint32_t result = 0;
	result |= (uint32_t)data[cursor + 0] << 0 & 0x000000FF;
	result |= (uint32_t)data[cursor + 1] << 8 & 0x0000FF00;
	result |= (uint32_t)data[cursor + 2] << 16 & 0x00FF0000;
	result |= (uint32_t)data[cursor + 3] << 24 & 0xFF000000;
	cursor += sizeof(uint32_t);
	return result;
}

template<>
double WKBReader::ReadDouble<WKBByteOrder::NDR>() {
	D_ASSERT(cursor + sizeof(double) <= length);
	// Read double in little endian
	uint64_t result = 0;
	result |= (uint64_t)data[cursor + 0] << 0 & 0x00000000000000FF;
	result |= (uint64_t)data[cursor + 1] << 8 & 0x000000000000FF00;
	result |= (uint64_t)data[cursor + 2] << 16 & 0x0000000000FF0000;
	result |= (uint64_t)data[cursor + 3] << 24 & 0x00000000FF000000;
	result |= (uint64_t)data[cursor + 4] << 32 & 0x000000FF00000000;
	result |= (uint64_t)data[cursor + 5] << 40 & 0x0000FF0000000000;
	result |= (uint64_t)data[cursor + 6] << 48 & 0x00FF000000000000;
	result |= (uint64_t)data[cursor + 7] << 56 & 0xFF00000000000000;
	cursor += sizeof(double);
	return *reinterpret_cast<double *>(&result);
}

template<>
uint32_t WKBReader::ReadInt<WKBByteOrder::XDR>() {
	D_ASSERT(cursor + sizeof(uint32_t) <= length);
	// Read uint32_t in big endian
	uint32_t result = 0;
	result |= (uint32_t)data[cursor + 0] << 24 & 0xFF000000;
	result |= (uint32_t)data[cursor + 1] << 16 & 0x00FF0000;
	result |= (uint32_t)data[cursor + 2] << 8 & 0x0000FF00;
	result |= (uint32_t)data[cursor + 3] << 0 & 0x000000FF;
	cursor += sizeof(uint32_t);
	return result;
}

template<>
double WKBReader::ReadDouble<WKBByteOrder::XDR>() {
	D_ASSERT(cursor + sizeof(double) <= length);
	// Read double in big endian
	uint64_t result = 0;
	result |= (uint64_t)data[cursor + 0] << 56 & 0xFF00000000000000;
	result |= (uint64_t)data[cursor + 1] << 48 & 0x00FF000000000000;
	result |= (uint64_t)data[cursor + 2] << 40 & 0x0000FF0000000000;
	result |= (uint64_t)data[cursor + 3] << 32 & 0x000000FF00000000;
	result |= (uint64_t)data[cursor + 4] << 24 & 0x00000000FF000000;
	result |= (uint64_t)data[cursor + 5] << 16 & 0x0000000000FF0000;
	result |= (uint64_t)data[cursor + 6] << 8 & 0x000000000000FF00;
	result |= (uint64_t)data[cursor + 7] << 0 & 0x00000000000000FF;
	cursor += sizeof(double);
	return *reinterpret_cast<double *>(&result);
}

// Parse "standard" WKB format
Geometry GeometryContext::FromWKB(const char* wkb, uint32_t length) {
	WKBReader reader(wkb, length, *this);
	return reader.Read();
}

string GeometryContext::ToWKB(const Geometry &geometry) {
	return string();
}

VertexVector GeometryContext::AllocateVertexVector(uint32_t capacity) {
	auto data = reinterpret_cast<Vertex*>(allocator.AllocateAligned(sizeof(Vertex) * capacity));
	return VertexVector(data, 0, capacity, false);
}

Point GeometryContext::CreatePoint(double x, double y) {
	auto data = AllocateVertexVector(1);
	data.Add(Vertex(x, y));
	return Point(std::move(data));
}

LineString GeometryContext::CreateLineString(uint32_t num_points) {
	return LineString( AllocateVertexVector(num_points));
}

Polygon GeometryContext::CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities) {
	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));
	for (uint32_t i = 0; i < num_rings; i++) {
		rings[i] = AllocateVertexVector(ring_capacities[i]);
	}
	return Polygon(rings, num_rings);
}


string_t GeometryContext::Serialize(Vector &result, const Geometry &geometry) {
	// We always want the coordinates to be double aligned (8 bytes)
	// layout:
	// GeometryPrefix (4 bytes)
	// Padding (4 bytes) (or SRID?)
	// Data (variable length)
	// -- Point
	// 	  Type ( 4 bytes)
	//    Padding (4 bytes)
	//    X (8 bytes)
	//    Y (8 bytes)
	// -- LineString
	//    Type (4 bytes)
	//    Length (4 bytes)
	//    Points (variable length)
	// -- Polygon
	//    Type (4 bytes)
	//    NumRings (4 bytes)
	//    Rings (variable length)
	auto type = geometry.Type();
	GeometryPrefix prefix(0, type);
	switch (type) {
	case GeometryType::POINT: {
		auto &p = geometry.GetPoint();
		uint32_t total_size = sizeof(GeometryPrefix) + 4 + 4 + 4 + p.SerializedSize();
		auto ptr = allocator.AllocateAligned(total_size);
		auto start = ptr;
		// write prefix
		ptr += prefix.Serialize(ptr);
		ptr += sizeof(uint32_t); // padding

		// write type
		auto ty = (uint32_t)GeometryType::POINT;
		memcpy(ptr, &ty, sizeof(uint32_t));
		ptr += sizeof(uint32_t);

		// write point
		ptr += sizeof(uint32_t); // padding
		p.data.Serialize(ptr);

		return StringVector::AddStringOrBlob(result, (const char*)start, total_size);
	}
	case GeometryType::LINESTRING: {
		throw NotImplementedException("Geometry::Serialize(<LineString>)");
	}
	case GeometryType::POLYGON: {
		throw NotImplementedException("Geometry::Serialize(<Polygon>)");
	}
	default:
		throw NotImplementedException("Geometry::Serialize(<Unknown>)");
	}
}

Geometry GeometryContext::Deserialize(const string_t &data) {
	auto ptr = (const_data_ptr_t)data.GetDataUnsafe();
	// read prefix
	auto type = (GeometryType)Load<uint8_t>(ptr++);
	auto flags = (uint8_t)Load<uint8_t>(ptr++);
	auto _pad1 = (uint8_t)Load<uint8_t>(ptr++);
	auto _pad2 = (uint8_t)Load<uint8_t>(ptr++);

	GeometryPrefix prefix(flags, type);

	// read padding
	ptr += sizeof(uint32_t);

	// read type
	auto _type = (GeometryType)Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	switch (_type) {
	case GeometryType::POINT: {
		// read padding
		ptr += sizeof(uint32_t);

		// Now read point data;
		VertexVector vertex_data((Vertex*)ptr, 1, 1, false );
		return Geometry(Point(std::move(vertex_data)));
	}
	default:
		throw NotImplementedException("Geometry::Deserialize(<Unknown>)");
	}
}


} // namespace core

} // namespace geo
