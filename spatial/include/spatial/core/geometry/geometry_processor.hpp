#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

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
    GeometryType parent_type;

private:
    void ProcessVertexData(Cursor &cursor, uint32_t count) {
        if(count == 0) {
            return;
        }

        const_data_ptr_t vertex_data[4];
        ptrdiff_t vertex_stride[4];

        // Get the data at the current cursor position
        auto data = cursor.GetPtr();

        // TODO: Maybe we should always keep the z in the 3rd position and the m in the 4th position?
        // TODO: These calculations are all constant, we could move it to Execute() instead.
        vertex_data[0] = data;
        vertex_data[1] = data + sizeof(double);
        vertex_data[2] = (has_z || has_m) ? data + 2 * sizeof(double) : const_data_ptr_cast(&EMPTY_DATA);
        vertex_data[3] = (has_m && has_z) ? data + 3 * sizeof(double) : const_data_ptr_cast(&EMPTY_DATA);

        auto vertex_size = static_cast<ptrdiff_t>(sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)));

        vertex_stride[0] = vertex_size;
        vertex_stride[1] = vertex_size;
        vertex_stride[2] = (has_z || has_m) ? vertex_size : 0;
        vertex_stride[3] = (has_z && has_m) ? vertex_size : 0;

        OnVertexData(vertex_data, vertex_stride, count);

        // Move the cursor forward
        cursor.Skip(count * vertex_size);
    }

    void ProcessPoint(Cursor &cursor) {
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


    void ProcessLineString(Cursor &cursor) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::LINESTRING);
        (void)type;
        current_type = GeometryType::LINESTRING;

        auto count = cursor.Read<uint32_t>();
        D_ASSERT(count == 0 || count > 1);

        OnLineStringBegin(count);
        ProcessVertexData(cursor, count);
        OnLineStringEnd(count);
    }

    void ProcessPolygon(Cursor &cursor) {
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
            bool is_last = i == ring_count - 1;
            auto ring_size = count_cursor.Read<uint32_t>();
            OnPolygonRingBegin(i, is_last);
            ProcessVertexData(cursor, ring_size);
            OnPolygonRingEnd(i, is_last);
        }

        OnPolygonEnd(ring_count);
    }

    void ProcessMultiPoint(Cursor &cursor) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::MULTIPOINT);
        (void)type;
        current_type = GeometryType::MULTIPOINT;

        auto count = cursor.Read<uint32_t>();

        OnCollectionBegin(count);
        auto prev_parent_type = parent_type;
        nesting_level++;
        parent_type = current_type;
        for (uint32_t i = 0; i < count; i++) {
            bool is_last = i == count - 1;
            OnCollectionItemBegin(i, is_last);
            ProcessPoint(cursor);
            current_type = GeometryType::MULTIPOINT;
            OnCollectionItemEnd(i, is_last);
        }
        nesting_level--;
        parent_type = prev_parent_type;
        OnCollectionEnd(count);
    }

    void ProcessMultiLineString(Cursor &cursor) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::MULTILINESTRING);
        (void)type;
        current_type = GeometryType::MULTILINESTRING;
        auto count = cursor.Read<uint32_t>();

        OnCollectionBegin(count);
        auto prev_parent_type = parent_type;
        nesting_level++;
        parent_type = current_type;
        for (uint32_t i = 0; i < count; i++) {
            bool is_last = i == count - 1;
            OnCollectionItemBegin(i, is_last);
            ProcessLineString(cursor);
            current_type = GeometryType::MULTILINESTRING;
            OnCollectionItemEnd(i, is_last);
        }
        nesting_level--;
        parent_type = prev_parent_type;
        OnCollectionEnd(count);
    }

    void ProcessMultiPolygon(Cursor &cursor) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::MULTIPOLYGON);
        (void) type;
        current_type = GeometryType::MULTIPOLYGON;
        auto count = cursor.Read<uint32_t>();

        OnCollectionBegin(count);
        auto prev_parent_type = parent_type;
        nesting_level++;
        parent_type = current_type;
        for (uint32_t i = 0; i < count; i++) {
            bool is_last = i == count - 1;
            OnCollectionItemBegin(i, is_last);
            ProcessPolygon(cursor);
            current_type = GeometryType::MULTIPOLYGON;
            OnCollectionItemEnd(i, is_last);
        }
        nesting_level--;
        parent_type = prev_parent_type;
        OnCollectionEnd(count);
    }

    // TODO: We could use a stack here to avoid the recursion
    // NOLINTNEXTLINE
    void ProcessGeometryCollection(Cursor &cursor) {
        if(nesting_level > 255) {
            throw SerializationException("Geometry nesting level too deep");
        }

        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::GEOMETRYCOLLECTION);
        (void) type;
        current_type = GeometryType::GEOMETRYCOLLECTION;

        auto count = cursor.Read<uint32_t>();

        OnCollectionBegin(count);
        auto prev_parent_type = parent_type;
        nesting_level++;
        parent_type = current_type;
        for (uint32_t i = 0; i < count; i++) {
            bool is_last = i == count - 1;
            OnCollectionItemBegin(i, is_last);
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
            OnCollectionItemEnd(i, is_last);
        }
        nesting_level--;
        parent_type = prev_parent_type;
        OnCollectionEnd(count);
    }

public:
    void Execute(const geometry_t &geom) {
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
protected:
    GeometryProcessor() : nesting_level(0), has_z(false), has_m(false), current_type(GeometryType::POINT), parent_type(GeometryType::POINT) {}
    idx_t CurrentNestingLevel() const { return nesting_level; }
    bool IsNested() const { return nesting_level > 0; }
    bool HasZ() const { return has_z; }
    bool HasM() const { return has_m; }
    GeometryType CurrentType() const { return current_type; }
    GeometryType ParentType() const { return parent_type; }
protected:
    // This will never be called with count == 0
    virtual void OnVertexData(const_data_ptr_t vertex_data[4], ptrdiff_t vertex_stride[4], uint32_t count) {}
    virtual void OnBegin() {}
    virtual void OnEnd() {}
    virtual void OnPointBegin(bool is_empty) {}
    virtual void OnPointEnd(bool is_empty) {}
    virtual void OnLineStringBegin(uint32_t num_points) {}
    virtual void OnLineStringEnd(uint32_t num_points) {}
    virtual void OnPolygonBegin(uint32_t num_rings) {}
    virtual void OnPolygonEnd(uint32_t num_rings) {}
    virtual void OnPolygonRingBegin(uint32_t ring_idx, bool is_last) {}
    virtual void OnPolygonRingEnd(uint32_t ring_idx, bool is_last) {}
    virtual void OnCollectionBegin(uint32_t num_items) {}
    virtual void OnCollectionEnd(uint32_t num_items) {}
    virtual void OnCollectionItemBegin(uint32_t item_idx, bool is_last) {}
    virtual void OnCollectionItemEnd(uint32_t item_idx, bool is_last) {}
};



class VertexData {
private:
    static const constexpr double EMPTY_DATA = 0;
public:
    const_data_ptr_t vertex_data[4];
    ptrdiff_t vertex_stride[4];
    uint32_t vertex_count;

    VertexData(const_data_ptr_t data, uint32_t count, bool has_z, bool has_m) {

        // Get the data at the current cursor position

        // TODO: Maybe we should always keep the z in the 3rd position and the m in the 4th position?
        // TODO: These calculations are all constant, we could move it to Execute() instead.
        vertex_data[0] = data;
        vertex_data[1] = data + sizeof(double);
        vertex_data[2] = (has_z || has_m) ? data + 2 * sizeof(double) : const_data_ptr_cast(&EMPTY_DATA);
        vertex_data[3] = (has_m && has_z) ? data + 3 * sizeof(double) : const_data_ptr_cast(&EMPTY_DATA);

        auto vertex_size = static_cast<ptrdiff_t>(sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)));

        vertex_stride[0] = vertex_size;
        vertex_stride[1] = vertex_size;
        vertex_stride[2] = (has_z || has_m) ? vertex_size : 0;
        vertex_stride[3] = (has_z && has_m) ? vertex_size : 0;
    }
};

class RingIterator {
    const_data_ptr_t count_ptr;
    const_data_ptr_t data_ptr;
    bool has_z;
    bool has_m;
public:
    RingIterator(bool has_z, bool has_m, const_data_ptr_t count_ptr, const_data_ptr_t data_ptr)
        : count_ptr(count_ptr), data_ptr(data_ptr), has_z(has_z), has_m(has_m) {}

    void operator++() {
        // Move to the next ring
        auto size = Load<uint32_t>(count_ptr);
        data_ptr += size * sizeof(double);
        count_ptr += sizeof(uint32_t);
    }
    bool operator!=(const RingIterator &other) {
        return data_ptr != other.data_ptr;
    }

    VertexData operator*() {
        auto count = Load<uint32_t>(count_ptr);
        VertexData data(data_ptr, count, has_z, has_m);
        return data;
    }
};


class PolygonRings {
private:
    bool has_z;
    bool has_m;
    uint32_t num_rings;
    const_data_ptr_t start_count_ptr;
    const_data_ptr_t start_data_ptr;
public:
    PolygonRings(bool has_z, bool has_m, uint32_t num_rings, const_data_ptr_t start_count_ptr, const_data_ptr_t start_data_ptr)
    : has_z(has_z), has_m(has_m), num_rings(num_rings), start_count_ptr(start_count_ptr), start_data_ptr(start_data_ptr) {}

    RingIterator begin() {
        return RingIterator { has_z, has_m, start_count_ptr, start_data_ptr };
    }

    RingIterator end() {
        return RingIterator { has_z, has_m, start_count_ptr + num_rings * sizeof(uint32_t), nullptr };
    }
};


template<class IMPL, class ...PARAMS>
class Parent {
private:
    bool has_z = false;
    bool has_m = false;
protected:
    bool HasZ() const { return has_z; }
    bool HasM() const { return has_m; }
public:
    void ProcessPoint(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::POINT);
        (void)type;
        auto count = cursor.Read<uint32_t>();
        D_ASSERT(count == 1 || count == 0);
        // setup vertex data TODO:
        VertexData data(cursor.GetPtr(), count, has_z, has_m);
        static_cast<IMPL *>(this)->OnPoint(data, params...);
    }

    void ProcessLineString(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::LINESTRING);
        (void)type;
        auto count = cursor.Read<uint32_t>();
        D_ASSERT(count == 0 || count > 1);
        // setup vertex data TODO:
        VertexData data(cursor.GetPtr(), count, has_z, has_m);
        static_cast<IMPL *>(this)->OnLineString(data, params...);
    }

    void ProcessPolygon(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::POLYGON);
        (void)type;
        auto ring_count = cursor.Read<uint32_t>();
        auto count_cursor = cursor;
        cursor.Skip(ring_count * sizeof(uint32_t) + ((ring_count % 2) * sizeof(uint32_t)));

        PolygonRings rings(HasZ(), HasM(), ring_count, count_cursor.GetPtr(), cursor.GetPtr());

        static_cast<IMPL *>(this)->OnPolygon(ring_count, rings, params...);
    }

    void ProcessMultiPoint(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::MULTIPOINT);
        (void)type;

        auto count = cursor.Read<uint32_t>();

        static_cast<IMPL *>(this)->OnCollection(count, [&](PARAMS ...params) {
            // Do something
            ProcessPoint(cursor, params...);
        }, params...);
    }

    void ProcessMultiLineString(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::MULTILINESTRING);
        (void)type;
        auto count = cursor.Read<uint32_t>();

        static_cast<IMPL *>(this)->OnCollection(count, [&](PARAMS ...params) {
            // Do something
            ProcessLineString(cursor, params...);
        }, params...);
    }

    void ProcessMultiPolygon(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::MULTIPOLYGON);
        (void)type;
        auto count = cursor.Read<uint32_t>();

        static_cast<IMPL *>(this)->OnCollection(count, [&](PARAMS ...params) {
            // Do something
            ProcessPolygon(cursor, params...);
        }, params...);
    }

    //NOLINTNEXTLINE
    void ProcessGeometryCollection(Cursor &cursor, PARAMS ...params) {
        auto type = cursor.Read<SerializedGeometryType>();
        D_ASSERT(type == SerializedGeometryType::GEOMETRYCOLLECTION);
        (void)type;

        auto count = cursor.Read<uint32_t>();

        //NOLINTNEXTLINE
        static_cast<IMPL *>(this)->OnCollection(count, [&](PARAMS ...params) {
            auto item_type = cursor.Peek<SerializedGeometryType>();
            switch (item_type) {
                case SerializedGeometryType::POINT:
                    ProcessPoint(cursor, params...);
                    break;
                case SerializedGeometryType::LINESTRING:
                    ProcessLineString(cursor, params...);
                    break;
                case SerializedGeometryType::POLYGON:
                    ProcessPolygon(cursor, params...);
                    break;
                case SerializedGeometryType::MULTIPOINT:
                    ProcessMultiPoint(cursor, params...);
                    break;
                case SerializedGeometryType::MULTILINESTRING:
                    ProcessMultiLineString(cursor, params...);
                    break;
                case SerializedGeometryType::MULTIPOLYGON:
                    ProcessMultiPolygon(cursor, params...);
                    break;
                case SerializedGeometryType::GEOMETRYCOLLECTION:
                    ProcessGeometryCollection(cursor, params...);
                    break;
                default:
                    throw SerializationException("Unknown geometry type (%ud)", static_cast<uint32_t>(item_type));
            }
        }, params...);
    }

    void Execute(const geometry_t &geom, PARAMS ...params) {

        has_z = geom.GetProperties().HasZ();
        has_m = geom.GetProperties().HasM();

        Cursor cursor(geom);

        cursor.Skip<GeometryType>();
        cursor.Skip<GeometryProperties>();
        cursor.Skip<uint16_t>();
        cursor.Skip(4);
        if (geom.GetProperties().HasBBox()) {
            cursor.Skip(sizeof(float) * 4);
        }

        auto serialized_type = cursor.Peek<SerializedGeometryType>();
        switch (serialized_type) {
            case SerializedGeometryType::POINT:
                ProcessPoint(cursor, params...);
                break;
            case SerializedGeometryType::LINESTRING:
                ProcessLineString(cursor, params...);
                break;
            case SerializedGeometryType::POLYGON:
                ProcessPolygon(cursor, params...);
                break;
            case SerializedGeometryType::MULTIPOINT:
                ProcessMultiPoint(cursor, params...);
                break;
            case SerializedGeometryType::MULTILINESTRING:
                ProcessMultiLineString(cursor, params...);
                break;
            case SerializedGeometryType::MULTIPOLYGON:
                ProcessMultiPolygon(cursor, params...);
                break;
            case SerializedGeometryType::GEOMETRYCOLLECTION:
                ProcessGeometryCollection(cursor, params...);
                break;
            default:
                throw SerializationException("Unknown geometry type (%ud)", static_cast<uint32_t>(serialized_type));
        }
    }
};

/*
class Sub : public Parent<Sub, int> {
    friend class Parent<Sub, int>;

    void OnPoint(const VertexData &data, int param) {

    }

    void OnLineString(const VertexData &data, int param) {

    }

    void OnPolygon(uint32_t ring_count, PolygonRings &rings, int param) {
        for (const auto data : rings) {

        }
    }

    template<class OnItem>
    void OnCollection(uint32_t count, OnItem &&item, int param) {
        for(int i = 0; i < count; i++) {
            item(i);
        }
    }
};

*/

} // namespace core

} // namespace spatial