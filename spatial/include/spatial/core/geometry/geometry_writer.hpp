#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/util/cursor.hpp"
#include "spatial/core/util/math.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"
namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// WriteBuffer
//------------------------------------------------------------------------------
class WriteBuffer {
	ArenaAllocator &allocator;
	data_ptr_t start;
	uint32_t size;
	uint32_t capacity;

public:
	explicit WriteBuffer(ArenaAllocator &allocator_p) : allocator(allocator_p), start(nullptr), size(0), capacity(0) {
	}

	// Begin the buffer with an initial capacity
	void Begin(bool has_z, bool has_m) {
		// The default initial capacity is 32 (+ 16) bytes which is enough to fit a single point geometry
		auto initial_capacity = 32 + (has_z ? sizeof(double) : 0) + (has_m ? sizeof(double) : 0);
		size = 0;
		start = allocator.ReallocateAligned(start, capacity, initial_capacity);
		capacity = initial_capacity;
	}

	// Shrink the buffer to the current size
	void End() {
		start = allocator.ReallocateAligned(start, capacity, size);
		capacity = size;
	}

	void AddCapacity(uint32_t capacity_p) {
		start = allocator.ReallocateAligned(start, capacity, capacity + capacity_p);
		capacity += capacity_p;
	}

	void Write(const void *data, uint32_t write_size) {
		if (size + write_size > capacity) {
			auto new_capacity = capacity * 2;
			start = allocator.ReallocateAligned(start, capacity, new_capacity);
			capacity = new_capacity;
		}
		memcpy(start + size, data, write_size);
		size += write_size;
	}

	template <class T>
	void Write(const T &value) {
		Write(&value, sizeof(T));
	}

	// Offset has to be less than count
	template <class T>
	void WriteOffset(const T &value, uint32_t offset) {
		if (offset + sizeof(T) > size) {
			throw SerializationException("Offset out of bounds");
		}
		memcpy(start + offset, &value, sizeof(T));
	}
	uint32_t Size() const {
		return size;
	}
	uint32_t Capacity() const {
		return capacity;
	}
	data_ptr_t GetPtr() const {
		return start;
	}
};

//------------------------------------------------------------------------------
// GeometryWriter
//------------------------------------------------------------------------------
struct GeometryStats {
	uint32_t vertex_count = 0;
	double min_x = std::numeric_limits<double>::max();
	double min_y = std::numeric_limits<double>::max();
	double max_x = std::numeric_limits<double>::lowest();
	double max_y = std::numeric_limits<double>::lowest();

	void Reset() {
		vertex_count = 0;
		min_x = std::numeric_limits<double>::max();
		min_y = std::numeric_limits<double>::max();
		max_x = std::numeric_limits<double>::lowest();
		max_y = std::numeric_limits<double>::lowest();
	}

	void Update(double x, double y) {
		vertex_count++;
		min_x = std::min(min_x, x);
		min_y = std::min(min_y, y);
		max_x = std::max(max_x, x);
		max_y = std::max(max_y, y);
	}
};

class GeometryWriter {
private:
	WriteBuffer buffer;
	bool has_z = false;
	bool has_m = false;
	GeometryType type;
	uint32_t ring_count_offset = 0;
	GeometryStats stats;

public:
	explicit GeometryWriter(ArenaAllocator &allocator_p) : buffer(allocator_p) {
	}

	void Begin(GeometryType geom_type, bool has_z_dim, bool has_m_dim) {
		has_z = has_z_dim;
		has_m = has_m_dim;
		type = geom_type;
		buffer.Begin(has_z, has_m);

		buffer.Write<GeometryType>(type);
		buffer.Write<uint8_t>(0);  // properties
		buffer.Write<uint16_t>(0); // Hash
		buffer.Write<uint32_t>(0); // padding

		// We dont write the bbox yet, we will write it at the end, but we reserve space for it
		if (type != GeometryType::POINT) {
			buffer.Write(sizeof(float) * 4);
		}
	}

	string_t End() {
		// Shrink the buffer to the actual size
		buffer.End();

		GeometryProperties properties;
		properties.SetZ(has_z);
		properties.SetM(has_m);
		properties.SetBBox(false);
		if (stats.vertex_count > 0 && type != GeometryType::POINT) {
			properties.SetBBox(true);
		}

		// Write the properties at the beginning of the buffer (after geometry type)
		buffer.WriteOffset(properties, 1);

		if (properties.HasBBox()) {
			// Write the bbox after the first 8 bytes
			buffer.WriteOffset<float>(MathUtil::DoubleToFloatDown(stats.min_x), 8);
			buffer.WriteOffset<float>(MathUtil::DoubleToFloatDown(stats.min_y), 12);
			buffer.WriteOffset<float>(MathUtil::DoubleToFloatUp(stats.max_x), 16);
			buffer.WriteOffset<float>(MathUtil::DoubleToFloatUp(stats.max_y), 20);
			string_t blob = string_t {const_char_ptr_cast(buffer.GetPtr()), buffer.Size()};
			return blob;
		} else {
			// Move the header forwards by 16 bytes
			auto start = buffer.GetPtr();
			std::memmove(start + 16, start, buffer.Size() - 16);
			string_t blob = string_t {const_char_ptr_cast(start), buffer.Size()};
			return blob;
		}
	}

	void AddVertex(double x, double y) {
		D_ASSERT(!has_z && !has_m);
		buffer.Write(x);
		buffer.Write(y);

		stats.Update(x, y);
	}

	void AddVertex(double x, double y, double zm) {
		D_ASSERT(has_z || has_m);
		buffer.Write(x);
		buffer.Write(y);
		buffer.Write(zm);

		stats.Update(x, y);
	}

	void AddVertex(double x, double y, double z, double m) {
		D_ASSERT(has_z && has_m);
		buffer.Write(x);
		buffer.Write(y);
		buffer.Write(z);
		buffer.Write(m);

		stats.Update(x, y);
	}

	void AddPoint(bool is_empty) {
		buffer.Write<uint32_t>(static_cast<uint32_t>(GeometryType::POINT));
		buffer.Write<uint32_t>(is_empty ? 0 : 1);
	}

	void AddLineString(uint32_t vertex_count) {
		buffer.Write<uint32_t>(static_cast<uint32_t>(GeometryType::LINESTRING));
		buffer.Write<uint32_t>(vertex_count);
	}

	void AddPolygon(uint32_t ring_count) {
		buffer.Write<uint32_t>(static_cast<uint32_t>(GeometryType::POLYGON));
		buffer.Write<uint32_t>(ring_count);
		ring_count_offset = buffer.Size();
		for (auto i = 0; i < ring_count; i++) {
			buffer.Write<uint32_t>(0);
		}
	}

	void AddRing(uint32_t vertex_count) {
		buffer.WriteOffset(vertex_count, ring_count_offset);
		ring_count_offset += sizeof(uint32_t);
	}

	void AddCollection(GeometryType collection_type, uint32_t item_count) {
		buffer.Write<uint32_t>(static_cast<uint32_t>(collection_type));
		buffer.Write<uint32_t>(item_count);
	}
};

} // namespace core

} // namespace spatial