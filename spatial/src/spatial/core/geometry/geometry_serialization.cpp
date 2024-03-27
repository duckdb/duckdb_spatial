#include "spatial/common.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

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

template<class VERTEX>
struct GetRequiredSizeOp {
    static uint32_t Apply(const SinglePartGeometry &geom) {
        // 4 bytes for the type
        // 4 bytes for the length
        // sizeof(vertex) * count
        return 4 + 4 + (geom.Count() * sizeof(VERTEX));
    }

    static uint32_t Apply(const Polygon &polygon) {
        // Polygons are special because they may pad between the rings and the ring data
        // 4 bytes for the type
        // 4 bytes for the number of rings
        // 4 bytes for the number of vertices in each ring
        // - sizeof(vertex) * count for each ring
        // (+ 4 bytes for padding if num_rings is odd)
        uint32_t size = 4 + 4;
        for (const auto &ring : polygon) {
            size += 4;
            size += ring.Count() * sizeof(VERTEX);
        }
        if (polygon.Count() % 2 == 1) {
            size += 4;
        }
        return size;
    }

    template<class ITEM>
    static uint32_t Apply(const TypedCollectionGeometry<ITEM> &collection) {
        // 4 bytes for the type
        // 4 bytes for the number of items
        // recursive call for each item
        uint32_t size = 4 + 4;
        for (const auto &item : collection) {
            size += Apply(item);
        }
        return size;
    }

    static uint32_t Apply(const GeometryCollection &collection) {
        // 4 bytes for the type
        // 4 bytes for the number of geometries
        // sizeof(geometry) * count
        uint32_t size = 4 + 4;
        for (const auto &geom : collection) {
            size += geom.Visit<GetRequiredSizeOp<VERTEX>>();
        }
        return size;
    }
};



template<class VERTEX>
struct SerializeOp {
    static constexpr uint32_t MAX_DEPTH = 256;

    static void SerializeVertices(const SinglePartGeometry &verts, Cursor &cursor, BoundingBox &bbox, bool update_bounds) {
        // Write the vertex data
        auto byte_size = verts.ByteSize();
        memcpy(cursor.GetPtr(), verts.GetData(), byte_size);
        // Move the cursor forward
        cursor.Skip(byte_size);
        // Also update the bounds real quick
        if (update_bounds) {
            for (uint32_t i = 0; i < verts.Count(); i++) {
                auto vertex = verts.GetExact<VERTEX>(i);
                bbox.Stretch(vertex);
            }
        }
    }

    static void Apply(const Point &point, Cursor &cursor, BoundingBox &bbox, uint32_t depth) {
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

    static void Apply(const LineString &linestring, Cursor &cursor, BoundingBox &bbox, uint32_t) {
        D_ASSERT(linestring.GetProperties().HasZ() == VERTEX::HAS_Z);
        D_ASSERT(linestring.GetProperties().HasM() == VERTEX::HAS_M);

        // Write type (4 bytes)
        cursor.Write<SerializedGeometryType>(SerializedGeometryType::LINESTRING);

        // Write point count (4 bytes)
        cursor.Write<uint32_t>(linestring.Count());

        // write data
        SerializeVertices(linestring, cursor, bbox, true);
    }

    static void Apply(const Polygon &polygon, Cursor &cursor, BoundingBox &bbox, uint32_t) {
        D_ASSERT(polygon.GetProperties().HasZ() == VERTEX::HAS_Z);
        D_ASSERT(polygon.GetProperties().HasM() == VERTEX::HAS_M);

        // Write type (4 bytes)
        cursor.Write<SerializedGeometryType>(SerializedGeometryType::POLYGON);

        // Write number of rings (4 bytes)
        cursor.Write<uint32_t>(polygon.Count());

        // Write ring lengths
        for (const auto &ring : polygon) {
            cursor.Write<uint32_t>(ring.Count());
        }

        if (polygon.Count() % 2 == 1) {
            // Write padding (4 bytes)
            cursor.Write<uint32_t>(0);
        }

        // Write ring data
        for (uint32_t i = 0; i < polygon.Count(); i++) {
            // The first ring is always the shell, and must be the only ring contributing to the bounding box
            // or the geometry is invalid.
            SerializeVertices(polygon[i], cursor, bbox, i == 0);
        }
    }

    static void Apply(const MultiPoint &multipoint, Cursor &cursor, BoundingBox &bbox, uint32_t depth) {
        D_ASSERT(multipoint.GetProperties().HasZ() == VERTEX::HAS_Z);
        D_ASSERT(multipoint.GetProperties().HasM() == VERTEX::HAS_M);

        // Write type (4 bytes)
        cursor.Write<SerializedGeometryType>(SerializedGeometryType::MULTIPOINT);

        // Write number of points (4 bytes)
        cursor.Write<uint32_t>(multipoint.Count());

        // Write point data
        for (const auto &point : multipoint) {
            Apply(point, cursor, bbox, depth + 1);
        }
    }

    static void Apply(const MultiLineString &multilinestring, Cursor &cursor, BoundingBox &bbox, uint32_t depth) {
        D_ASSERT(multilinestring.GetProperties().HasZ() == VERTEX::HAS_Z);
        D_ASSERT(multilinestring.GetProperties().HasM() == VERTEX::HAS_M);

        // Write type (4 bytes)
        cursor.Write<SerializedGeometryType>(SerializedGeometryType::MULTILINESTRING);

        // Write number of linestrings (4 bytes)
        cursor.Write<uint32_t>(multilinestring.Count());

        // Write linestring data
        for (const auto &linestring : multilinestring) {
            Apply(linestring, cursor, bbox, depth + 1);
        }
    }

    static void Apply(const MultiPolygon &multipolygon, Cursor &cursor, BoundingBox &bbox, uint32_t depth) {
        D_ASSERT(multipolygon.GetProperties().HasZ() == VERTEX::HAS_Z);
        D_ASSERT(multipolygon.GetProperties().HasM() == VERTEX::HAS_M);

        // Write type (4 bytes)
        cursor.Write<SerializedGeometryType>(SerializedGeometryType::MULTIPOLYGON);

        // Write number of polygons (4 bytes)
        cursor.Write<uint32_t>(multipolygon.Count());

        // Write polygon data
        for (const auto &polygon : multipolygon) {
            Apply(polygon, cursor, bbox, depth + 1);
        }
    }

    static void Apply(const GeometryCollection &collection, Cursor &cursor, BoundingBox &bbox, uint32_t depth) {
        D_ASSERT(collection.GetProperties().HasZ() == VERTEX::HAS_Z);
        D_ASSERT(collection.GetProperties().HasM() == VERTEX::HAS_M);

        // TODO: Maybe make this configurable?
        if(depth > MAX_DEPTH) {
            throw SerializationException("GeometryCollection depth exceeded 256!");
        }

        // Write type (4 bytes)
        cursor.Write<SerializedGeometryType>(SerializedGeometryType::GEOMETRYCOLLECTION);

        // Write number of geometries (4 bytes)
        cursor.Write<uint32_t>(collection.Count());

        // write geometry data
        for (const auto &geom : collection) {
            geom.Visit<SerializeOp<VERTEX>>(cursor, bbox, depth + 1);
        }
    }
};

geometry_t Geometry::Serialize(Vector &result) {
	auto type = GetType();
	bool has_bbox = type != GeometryType::POINT && !IsEmpty();

    auto properties = GetProperties();
    auto has_z = properties.HasZ();
    auto has_m = properties.HasM();
    properties.SetBBox(has_bbox);

    uint32_t geom_size = 0;
    if(has_z && has_m) {
        geom_size = Visit<GetRequiredSizeOp<VertexXYZM>>();
    } else if(has_z) {
        geom_size = Visit<GetRequiredSizeOp<VertexXYZ>>();
    } else if(has_m) {
        geom_size = Visit<GetRequiredSizeOp<VertexXYM>>();
    } else {
        geom_size = Visit<GetRequiredSizeOp<VertexXY>>();
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

    if(has_z && has_m) {
        Visit<SerializeOp<VertexXYZM>>(cursor, bbox, 0);
    } else if(has_z) {
        Visit<SerializeOp<VertexXYZ>>(cursor, bbox, 0);
    } else if(has_m) {
        Visit<SerializeOp<VertexXYM>>(cursor, bbox, 0);
    } else {
        Visit<SerializeOp<VertexXY>>(cursor, bbox, 0);
    }

	// Now write the bounding box
	if (has_bbox) {
		cursor.SetPtr(bbox_ptr);
		// We serialize the bounding box as floats to save space, but ensure that the bounding box is
		// still large enough to contain the original double values by rounding up and down
		cursor.Write<float>(Utils::DoubleToFloatDown(bbox.minx));
		cursor.Write<float>(Utils::DoubleToFloatDown(bbox.miny));
		cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxx));
		cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxy));
		if (has_z) {
			cursor.Write<float>(Utils::DoubleToFloatDown(bbox.minz));
			cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxz));
		}
		if (has_m) {
			cursor.Write<float>(Utils::DoubleToFloatDown(bbox.minm));
			cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxm));
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
        Point point(HasZ(), HasM());
		if (!vertices.IsEmpty()) {
            point.ReferenceData(vertices.data[0], vertices.count);
		}
        return point;
	}

	Geometry ProcessLineString(const VertexData &vertices) override {
        LineString line_string(allocator, vertices.count, HasZ(), HasM());
        if (!vertices.IsEmpty()) {
            line_string.ReferenceData(vertices.data[0], vertices.count);
        }
        return line_string;
	}

	Geometry ProcessPolygon(PolygonState &state) override {
		Polygon polygon(allocator, state.RingCount(), HasZ(), HasM());
		for (auto i = 0; i < state.RingCount(); i++) {
            auto vertices = state.Next();
            if (!vertices.IsEmpty()) {
                polygon[i].ReferenceData(vertices.data[0], vertices.count);
            }
		}
		return polygon;
	}

	Geometry ProcessCollection(CollectionState &state) override {
		switch (CurrentType()) {
		case GeometryType::MULTIPOINT: {
			MultiPoint multi_point(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				multi_point[i] = state.Next().As<Point>();
			}
			return multi_point;
		}
		case GeometryType::MULTILINESTRING: {
			MultiLineString multi_line_string(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				multi_line_string[i] = state.Next().As<LineString>();
			}
			return multi_line_string;
		}
		case GeometryType::MULTIPOLYGON: {
			MultiPolygon multi_polygon(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				multi_polygon[i] = state.Next().As<Polygon>();
			}
			return multi_polygon;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			GeometryCollection collection(allocator, state.ItemCount(), HasZ(), HasM());
			for (auto i = 0; i < state.ItemCount(); i++) {
				collection[i] = state.Next();
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
