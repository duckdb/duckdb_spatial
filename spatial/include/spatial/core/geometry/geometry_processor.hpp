#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------
// GeometryProcessor
//------------------------------------------------------------------------
// The GeometryProcessor class is used to process a serialized geometry.
// By subclassing and overriding the appropriate methods an algorithm
// can be implemented to process the geometry in a streaming fashion.
//------------------------------------------------------------------------
class GeometryProcessor {
private:
    static const constexpr double EMPTY_DATA = 0;
    idx_t nesting_level;
    bool has_z;
    bool has_m;
    GeometryType current_type;

    void ProcessVertexData(Cursor &cursor, idx_t count);

    void ProcessPoint(Cursor &cursor);

    void ProcessLineString(Cursor &cursor);

    void ProcessPolygon(Cursor &cursor);

    void ProcessMultiPoint(Cursor &cursor);

    void ProcessMultiLineString(Cursor &cursor);

    void ProcessMultiPolygon(Cursor &cursor);

    void ProcessGeometryCollection(Cursor &cursor);

public:
    void Execute(const geometry_t &geom);

protected:
    GeometryProcessor() : nesting_level(0), has_z(false), has_m(false), current_type(GeometryType::POINT) {}
    idx_t CurrentNestingLevel() const { return nesting_level; }
    bool IsNested() const { return nesting_level > 0; }
    bool HasZ() const { return has_z; }
    bool HasM() const { return has_m; }
    GeometryType CurrentType() const { return current_type; }
protected:
    virtual void OnVertexData(const_data_ptr_t vertex_data[4], idx_t vertex_stride[4], idx_t count) {}
    virtual void OnBegin() {}
    virtual void OnEnd() {}
    virtual void OnPointBegin(bool is_empty) {}
    virtual void OnPointEnd(bool is_empty) {}
    virtual void OnLineBegin(uint32_t num_points) {}
    virtual void OnLineEnd(uint32_t num_points) {}
    virtual void OnPolygonBegin(uint32_t num_rings) {}
    virtual void OnPolygonEnd(uint32_t num_rings) {}
    virtual void OnPolygonRingBegin(uint32_t ring_idx) {}
    virtual void OnPolygonRingEnd(uint32_t ring_idx) {}
    virtual void OnCollectionBegin(uint32_t num_items) {}
    virtual void OnCollectionEnd(uint32_t num_items) {}
    virtual void OnCollectionItemBegin(uint32_t item_idx) {}
    virtual void OnCollectionItemEnd(uint32_t item_idx) {}
};

void GeometryProcessor::Execute(const geometry_t &geom) {
    // Reset state
    nesting_level = 0;
    has_z = geom.GetProperties().HasZ();
    has_m = geom.GetProperties().HasM();
    current_type = geom.GetType();
    OnBegin();

    // Create a cursor to iterate over the geometry
    Cursor cursor(geom);

    // Skip header
    cursor.Skip<GeometryType>();
    cursor.Skip<GeometryProperties>();
    cursor.Skip<uint16_t>();

    // Skip padding
    cursor.Skip(4);

    if (geom.GetProperties().HasBBox()) {
        cursor.Skip(sizeof(float) * 4);
    }

    auto serialized_type = cursor.Peek<SerializedGeometryType>();

    switch (serialized_type) {
        case SerializedGeometryType::POINT:
            ProcessPoint(cursor);
            break;
        case SerializedGeometryType::LINESTRING:
            ProcessLineString(cursor);
            break;
        case SerializedGeometryType::POLYGON:
            ProcessPolygon(cursor);
            break;
        case SerializedGeometryType::MULTIPOINT:
            ProcessMultiPoint(cursor);
            break;
        case SerializedGeometryType::MULTILINESTRING:
            ProcessMultiLineString(cursor);
            break;
        case SerializedGeometryType::MULTIPOLYGON:
            ProcessMultiPolygon(cursor);
            break;
        case SerializedGeometryType::GEOMETRYCOLLECTION:
            ProcessGeometryCollection(cursor);
            break;
        default:
            throw SerializationException("Unknown geometry type (%ud)", static_cast<uint32_t>(serialized_type));
    }

    current_type = geom.GetType();
    OnEnd();
}

void GeometryProcessor::ProcessVertexData(Cursor &cursor, idx_t count) {
    const_data_ptr_t vertex_data[4];
    idx_t vertex_stride[4];

    // Get the data at the current cursor position
    auto data = cursor.GetPtr();

    vertex_data[0] = data;
    vertex_data[1] = data + sizeof(double);
    vertex_data[2] = has_m ? const_data_ptr_cast(&EMPTY_DATA) : data + 2 * sizeof(double);
    vertex_data[3] = has_z ? const_data_ptr_cast(&EMPTY_DATA) : data + 3 * sizeof(double);

    vertex_stride[0] = sizeof(double);
    vertex_stride[1] = sizeof(double);
    vertex_stride[2] = has_m ? sizeof(double) : 0;
    vertex_stride[3] = has_z ? sizeof(double) : 0;

    OnVertexData(vertex_data, vertex_stride, count);

    // Move the cursor forward
    cursor.Skip(count * (vertex_stride[0] + vertex_stride[1] + vertex_stride[2] + vertex_stride[3]));
}

void GeometryProcessor::ProcessPoint(Cursor &cursor) {
    auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::POINT);
    (void)type;
    current_type = GeometryType::POINT;

    // Points can be empty, in which case the count is 0
    auto count = cursor.Read<uint32_t>();
    D_ASSERT(count == 1 || count == 0);
    OnPointBegin(count == 0);
    ProcessVertexData(cursor, count);
    OnPointEnd(count == 0);
}

void GeometryProcessor::ProcessLineString(Cursor &cursor) {
    auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::LINESTRING);
    (void)type;
    current_type = GeometryType::LINESTRING;

    auto count = cursor.Read<uint32_t>();
    D_ASSERT(count > 0);

    OnLineBegin(count);
    ProcessVertexData(cursor, count);
    OnLineEnd(count);
}

void GeometryProcessor::ProcessPolygon(Cursor &cursor) {
auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::POLYGON);
    (void)type;
    current_type = GeometryType::POLYGON;

    auto ring_count = cursor.Read<uint32_t>();
    auto count_cursor = cursor;
    // Skip over the ring counts (and the padding if the ring count is odd)
    cursor.Skip(ring_count * sizeof(uint32_t) + ((ring_count % 2) * sizeof(uint32_t)));

    OnPolygonBegin(ring_count);

    for (uint32_t i = 0; i < ring_count; i++) {
        auto ring_size = count_cursor.Read<uint32_t>();
        OnPolygonRingBegin(i);
        ProcessVertexData(cursor, ring_size);
        OnPolygonRingEnd(i);
    }

    OnPolygonEnd(ring_count);
}

void GeometryProcessor::ProcessMultiPoint(Cursor &cursor) {
    auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::MULTIPOINT);
    (void)type;
    current_type = GeometryType::MULTIPOINT;

    auto count = cursor.Read<uint32_t>();
    D_ASSERT(count > 0);

    OnCollectionBegin(count);
    nesting_level++;
    for (uint32_t i = 0; i < count; i++) {
        OnCollectionItemBegin(i);
        ProcessPoint(cursor);
        current_type = GeometryType::MULTIPOINT;
        OnCollectionItemEnd(i);
    }
    nesting_level--;
    OnCollectionEnd(count);
}

void GeometryProcessor::ProcessMultiLineString(Cursor &cursor) {
    auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::MULTILINESTRING);
    (void)type;
    current_type = GeometryType::MULTILINESTRING;
    auto count = cursor.Read<uint32_t>();
    D_ASSERT(count > 0);

    OnCollectionBegin(count);
    nesting_level++;
    for (uint32_t i = 0; i < count; i++) {
        OnCollectionItemBegin(i);
        ProcessLineString(cursor);
        current_type = GeometryType::MULTILINESTRING;
        OnCollectionItemEnd(i);
    }
    nesting_level--;
    OnCollectionEnd(count);
}

void GeometryProcessor::ProcessMultiPolygon(Cursor &cursor) {
    auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::MULTIPOLYGON);
    (void) type;
    current_type = GeometryType::MULTIPOLYGON;
    auto count = cursor.Read<uint32_t>();
    D_ASSERT(count > 0);

    OnCollectionBegin(count);
    nesting_level++;
    for (uint32_t i = 0; i < count; i++) {
        OnCollectionItemBegin(i);
        ProcessPolygon(cursor);
        current_type = GeometryType::MULTIPOLYGON;
        OnCollectionItemEnd(i);
    }
    nesting_level--;
    OnCollectionEnd(count);
}

// NOLINTNEXTLINE
// TODO: We could use a stack here to avoid the recursion
void GeometryProcessor::ProcessGeometryCollection(Cursor &cursor) {
    if(nesting_level > 255) {
        throw SerializationException("Geometry nesting level too deep");
    }

    auto type = cursor.Read<SerializedGeometryType>();
    D_ASSERT(type == SerializedGeometryType::GEOMETRYCOLLECTION);
    (void) type;
    current_type = GeometryType::GEOMETRYCOLLECTION;

    auto count = cursor.Read<uint32_t>();
    D_ASSERT(count > 0);

    OnCollectionBegin(count);
    nesting_level++;
    for (uint32_t i = 0; i < count; i++) {
        OnCollectionItemBegin(i);
        auto serialized_type = cursor.Peek<SerializedGeometryType>();
        switch (serialized_type) {
            case SerializedGeometryType::POINT:
                ProcessPoint(cursor);
                break;
            case SerializedGeometryType::LINESTRING:
                ProcessLineString(cursor);
                break;
            case SerializedGeometryType::POLYGON:
                ProcessPolygon(cursor);
                break;
            case SerializedGeometryType::MULTIPOINT:
                ProcessMultiPoint(cursor);
                break;
            case SerializedGeometryType::MULTILINESTRING:
                ProcessMultiLineString(cursor);
                break;
            case SerializedGeometryType::MULTIPOLYGON:
                ProcessMultiPolygon(cursor);
                break;
            case SerializedGeometryType::GEOMETRYCOLLECTION:
                ProcessGeometryCollection(cursor);
                break;
            default:
                throw SerializationException("Unknown geometry type (%ud)", static_cast<uint32_t>(serialized_type));
        }
        current_type = GeometryType::GEOMETRYCOLLECTION;
        OnCollectionItemEnd(i);
    }
    nesting_level--;
    OnCollectionEnd(count);
}

} // namespace core

} // namespace spatial