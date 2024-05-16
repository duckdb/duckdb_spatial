#include "spatial/common.hpp"
#include "spatial/core/util/cursor.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"
#include "spatial/core/util/math.hpp"

namespace spatial {

namespace core {

//----------------------------------------------------------------------
// Serialization
//----------------------------------------------------------------------
// We always want the coordinates to be double aligned (8 bytes)
// layout:
// GeometryHeader (4 bytes)
// Padding (4 bytes) (or SRID?)
// Data (variable length)
// -- Point
// 	  Type ( 4 bytes)
//    Count (4 bytes) (count == 0 if empty point, otherwise 1)
//    X (8 bytes)
//    Y (8 bytes)
// -- LineString
//    Type (4 bytes)
//    Length (4 bytes)
//    Points (variable length)
// -- Polygon
//    Type (4 bytes)
//    NumRings (4 bytes)
// 	  RingsLengths (variable length)
//    padding (4 bytes if num_rings is odd)
//    RingsData (variable length)
// --- Multi/Point/LineString/Polygon & GeometryCollection
//    Type (4 bytes)
//    NumGeometries (4 bytes)
//    Geometries (variable length)

template <class VERTEX>
struct GetRequiredSizeOp {
	static uint32_t Case(Geometry::Tags::SinglePartGeometry, const Geometry &geom) {
		// 4 bytes for the type
		// 4 bytes for the length
		// sizeof(vertex) * count
		return 4 + 4 + (geom.Count() * sizeof(VERTEX));
	}

	static uint32_t Case(Geometry::Tags::Polygon, const Geometry &polygon) {
		// Polygons are special because they may pad between the rings and the ring data
		// 4 bytes for the type
		// 4 bytes for the number of rings
		// 4 bytes for the number of vertices in each ring
		// - sizeof(vertex) * count for each ring
		// (+ 4 bytes for padding if num_rings is odd)
		uint32_t size = 4 + 4;
		for (uint32_t i = 0; i < Polygon::PartCount(polygon); i++) {
			size += 4;
			size += Polygon::Part(polygon, i).Count() * sizeof(VERTEX);
		}
		if (Polygon::PartCount(polygon) % 2 == 1) {
			size += 4;
		}
		return size;
	}

	static uint32_t Case(Geometry::Tags::CollectionGeometry, const Geometry &collection) {
		// 4 bytes for the type
		// 4 bytes for the number of items
		// recursive call for each item
		uint32_t size = 4 + 4;
		for (uint32_t i = 0; i < CollectionGeometry::PartCount(collection); i++) {
			auto &part = CollectionGeometry::Part(collection, i);
			size += Geometry::Match<GetRequiredSizeOp<VERTEX>>(part);
		}
		return size;
	}
};

template <class VERTEX>
struct SerializeOp {
	static constexpr uint32_t MAX_DEPTH = 256;

	static void SerializeVertices(const Geometry &verts, Cursor &cursor, BoundingBox &bbox, bool update_bounds) {
		// Write the vertex data
		auto byte_size = SinglePartGeometry::ByteSize(verts);
		memcpy(cursor.GetPtr(), verts.GetData(), byte_size);
		// Move the cursor forward
		cursor.Skip(byte_size);
		// Also update the bounds real quick
		if (update_bounds) {
			for (uint32_t i = 0; i < verts.Count(); i++) {
				auto vertex = SinglePartGeometry::GetVertex<VERTEX>(verts, i);
				bbox.Stretch(vertex);
			}
		}
	}

	static void Case(Geometry::Tags::Point, const Geometry &point, Cursor &cursor, BoundingBox &bbox, uint32_t depth) {
		D_ASSERT(point.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(point.GetProperties().HasM() == VERTEX::HAS_M);

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::POINT);

		// Write point count (0 or 1) (4 bytes)
		cursor.Write<uint32_t>(point.Count());

		// write data
		// We only update the bounds if this is a point part of a larger geometry
		SerializeVertices(point, cursor, bbox, depth != 0);
	}

	static void Case(Geometry::Tags::LineString, const Geometry &linestring, Cursor &cursor, BoundingBox &bbox,
	                 uint32_t) {
		D_ASSERT(linestring.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(linestring.GetProperties().HasM() == VERTEX::HAS_M);

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::LINESTRING);

		// Write point count (4 bytes)
		cursor.Write<uint32_t>(linestring.Count());

		// write data
		SerializeVertices(linestring, cursor, bbox, true);
	}

	static void Case(Geometry::Tags::Polygon, const Geometry &polygon, Cursor &cursor, BoundingBox &bbox, uint32_t) {
		D_ASSERT(polygon.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(polygon.GetProperties().HasM() == VERTEX::HAS_M);

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::POLYGON);

		// Write number of rings (4 bytes)
		cursor.Write<uint32_t>(polygon.Count());

		// Write ring lengths
		for (uint32_t i = 0; i < Polygon::PartCount(polygon); i++) {
			cursor.Write<uint32_t>(Polygon::Part(polygon, i).Count());
		}

		if (polygon.Count() % 2 == 1) {
			// Write padding (4 bytes)
			cursor.Write<uint32_t>(0);
		}

		// Write ring data
		for (uint32_t i = 0; i < polygon.Count(); i++) {
			// The first ring is always the shell, and must be the only ring contributing to the bounding box
			// or the geometry is invalid.
			SerializeVertices(Polygon::Part(polygon, i), cursor, bbox, i == 0);
		}
	}

	static void Case(Geometry::Tags::MultiPoint, const Geometry &multipoint, Cursor &cursor, BoundingBox &bbox,
	                 uint32_t depth) {
		D_ASSERT(multipoint.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(multipoint.GetProperties().HasM() == VERTEX::HAS_M);

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::MULTIPOINT);

		// Write number of points (4 bytes)
		cursor.Write<uint32_t>(multipoint.Count());

		// Write point data
		for (uint32_t i = 0; i < MultiPoint::PartCount(multipoint); i++) {
			Case(Geometry::Tags::Point {}, MultiPoint::Part(multipoint, i), cursor, bbox, depth + 1);
		}
	}

	static void Case(Geometry::Tags::MultiLineString, const Geometry &multilinestring, Cursor &cursor,
	                 BoundingBox &bbox, uint32_t depth) {
		D_ASSERT(multilinestring.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(multilinestring.GetProperties().HasM() == VERTEX::HAS_M);

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::MULTILINESTRING);

		// Write number of linestrings (4 bytes)
		cursor.Write<uint32_t>(multilinestring.Count());

		// Write linestring data
		for (uint32_t i = 0; i < MultiLineString::PartCount(multilinestring); i++) {
			Case(Geometry::Tags::LineString {}, MultiLineString::Part(multilinestring, i), cursor, bbox, depth + 1);
		}
	}

	static void Case(Geometry::Tags::MultiPolygon, const Geometry &multipolygon, Cursor &cursor, BoundingBox &bbox,
	                 uint32_t depth) {
		D_ASSERT(multipolygon.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(multipolygon.GetProperties().HasM() == VERTEX::HAS_M);

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::MULTIPOLYGON);

		// Write number of polygons (4 bytes)
		cursor.Write<uint32_t>(multipolygon.Count());

		// Write polygon data
		for (uint32_t i = 0; i < MultiPolygon::PartCount(multipolygon); i++) {
			Case(Geometry::Tags::Polygon {}, MultiPolygon::Part(multipolygon, i), cursor, bbox, depth + 1);
		}
	}

	static void Case(Geometry::Tags::GeometryCollection, const Geometry &collection, Cursor &cursor, BoundingBox &bbox,
	                 uint32_t depth) {
		D_ASSERT(collection.GetProperties().HasZ() == VERTEX::HAS_Z);
		D_ASSERT(collection.GetProperties().HasM() == VERTEX::HAS_M);

		// TODO: Maybe make this configurable?
		if (depth > MAX_DEPTH) {
			throw SerializationException("GeometryCollection depth exceeded 256!");
		}

		// Write type (4 bytes)
		cursor.Write<SerializedGeometryType>(SerializedGeometryType::GEOMETRYCOLLECTION);

		// Write number of geometries (4 bytes)
		cursor.Write<uint32_t>(collection.Count());

		// write geometry data
		for (uint32_t i = 0; i < GeometryCollection::PartCount(collection); i++) {
			auto &geom = GeometryCollection::Part(collection, i);
			Geometry::Match<SerializeOp<VERTEX>>(geom, cursor, bbox, depth + 1);
		}
	}
};

geometry_t Geometry::Serialize(const Geometry &geom, Vector &result) {
	auto type = geom.GetType();
	bool has_bbox = type != GeometryType::POINT && !Geometry::IsEmpty(geom);

	auto properties = geom.GetProperties();
	auto has_z = properties.HasZ();
	auto has_m = properties.HasM();
	properties.SetBBox(has_bbox);

	uint32_t geom_size = 0;
	if (has_z && has_m) {
		geom_size = Geometry::Match<GetRequiredSizeOp<VertexXYZM>>(geom);
	} else if (has_z) {
		geom_size = Geometry::Match<GetRequiredSizeOp<VertexXYZ>>(geom);
	} else if (has_m) {
		geom_size = Geometry::Match<GetRequiredSizeOp<VertexXYM>>(geom);
	} else {
		geom_size = Geometry::Match<GetRequiredSizeOp<VertexXY>>(geom);
	}

	auto header_size = 4;
	auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
	auto bbox_size = has_bbox ? (sizeof(float) * 2 * dims) : 0;
	auto size = header_size + 4 + bbox_size + geom_size; // + 4 for padding, + 16 for bbox
	auto blob = StringVector::EmptyString(result, size);

	Cursor cursor(blob);

	// Write the header
	cursor.Write<GeometryType>(type);
	cursor.Write<GeometryProperties>(properties);
	cursor.Write<uint16_t>(0);
	// Pad with 4 bytes (we might want to use this to store SRID in the future)
	cursor.Write<uint32_t>(0);

	// All geometries except points have a bounding box
	BoundingBox bbox;
	auto bbox_ptr = cursor.GetPtr();

	// skip the bounding box for now
	// we will come back and write it later
	cursor.Skip(bbox_size);

	if (has_z && has_m) {
		Geometry::Match<SerializeOp<VertexXYZM>>(geom, cursor, bbox, 0);
	} else if (has_z) {
		Geometry::Match<SerializeOp<VertexXYZ>>(geom, cursor, bbox, 0);
	} else if (has_m) {
		Geometry::Match<SerializeOp<VertexXYM>>(geom, cursor, bbox, 0);
	} else {
		Geometry::Match<SerializeOp<VertexXY>>(geom, cursor, bbox, 0);
	}

	// Now write the bounding box
	if (has_bbox) {
		cursor.SetPtr(bbox_ptr);
		// We serialize the bounding box as floats to save space, but ensure that the bounding box is
		// still large enough to contain the original double values by rounding up and down
		cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.minx));
		cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.miny));
		cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.maxx));
		cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.maxy));
		if (has_z) {
			cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.minz));
			cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.maxz));
		}
		if (has_m) {
			cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.minm));
			cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.maxm));
		}
	}
	blob.Finalize();
	return geometry_t(blob);
}

//----------------------------------------------------------------------
// Deserialization
//----------------------------------------------------------------------
class GeometryDeserializer final : GeometryProcessor<Geometry> {
	ArenaAllocator &allocator;

	Geometry ProcessPoint(const VertexData &vertices) override {
		auto point = Point::CreateEmpty(HasZ(), HasM());
		if (!vertices.IsEmpty()) {
			Point::ReferenceData(point, vertices.data[0], vertices.count);
		}
		return point;
	}

	Geometry ProcessLineString(const VertexData &vertices) override {
		auto line_string = LineString::Create(allocator, vertices.count, HasZ(), HasM());
		if (!vertices.IsEmpty()) {
			LineString::ReferenceData(line_string, vertices.data[0], vertices.count);
		}
		return line_string;
	}

	Geometry ProcessPolygon(PolygonState &state) override {
		auto polygon = Polygon::Create(allocator, state.RingCount(), HasZ(), HasM());
		for (auto i = 0; i < state.RingCount(); i++) {
			auto vertices = state.Next();
			if (!vertices.IsEmpty()) {
				auto &part = Polygon::Part(polygon, i);
				LineString::ReferenceData(part, vertices.data[0], vertices.count);
			}
		}
		return polygon;
	}

	Geometry ProcessCollection(CollectionState &state) override {
		switch (CurrentType()) {
		case GeometryType::MULTIPOINT: {
			auto multi_point = MultiPoint::Create(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				MultiPoint::Part(multi_point, i) = state.Next();
			}
			return multi_point;
		}
		case GeometryType::MULTILINESTRING: {
			auto multi_line_string = MultiLineString::Create(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				MultiLineString::Part(multi_line_string, i) = state.Next();
			}
			return multi_line_string;
		}
		case GeometryType::MULTIPOLYGON: {
			auto multi_polygon = MultiPolygon::Create(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				MultiPolygon::Part(multi_polygon, i) = state.Next();
			}
			return multi_polygon;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			auto collection = GeometryCollection::Create(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				GeometryCollection::Part(collection, i) = state.Next();
			}
			return collection;
		}
		default:
			throw NotImplementedException("GeometryDeserializer: Unimplemented geometry type: %d", CurrentType());
		}
	}

public:
	explicit GeometryDeserializer(ArenaAllocator &allocator) : allocator(allocator) {
	}
	Geometry Execute(const geometry_t &data) {
		return Process(data);
	}
};

Geometry Geometry::Deserialize(ArenaAllocator &arena, const geometry_t &data) {
	GeometryDeserializer deserializer(arena);
	return deserializer.Execute(data);
}

} // namespace core

} // namespace spatial
