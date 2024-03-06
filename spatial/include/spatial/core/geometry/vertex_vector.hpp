#pragma once

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include <cmath>

namespace spatial {

namespace core {

struct VertexXY {
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = false;
	static const constexpr bool HAS_M = false;

	double x;
	double y;
};

struct VertexXYZ {
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = true;
	static const constexpr bool HAS_M = false;

	double x;
	double y;
	double z;
};

struct VertexXYM {
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = false;
	static const constexpr bool HAS_M = true;

	double x;
	double y;
	double m;
};

struct VertexXYZM {
	static const constexpr bool IS_VERTEX = true;
	static const constexpr bool HAS_Z = true;
	static const constexpr bool HAS_M = true;

	double x;
	double y;
	double z;
	double m;
};

// A vertex array represents a copy-on-write array of potentially non-owned vertex data
class VertexArray {
private:
	class Properties {
	private:
		uint32_t data;
		uint32_t vertex_size;

	public:
		Properties() : data(0), vertex_size(sizeof(double) * 2) {
		}
		Properties(bool has_z, bool has_m)
		    : data((has_z ? 1 : 0) | (has_m ? 2 : 0)),
		      vertex_size(sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0))) {
		}
		bool HasZ() const {
			return (data & 1) != 0;
		}
		bool HasM() const {
			return (data & 2) != 0;
		}
		void SetZ(bool has_z) {
			data = (data & 2) | (has_z ? 1 : 0);
			vertex_size = sizeof(double) * (2 + (has_z ? 1 : 0) + (HasM() ? 1 : 0));
		}
		void SetM(bool has_m) {
			data = (data & 1) | (has_m ? 2 : 0);
			vertex_size = sizeof(double) * (2 + (HasZ() ? 1 : 0) + (has_m ? 1 : 0));
		}
		uint32_t VertexSize() const {
			return vertex_size;
		}
	};

private:
	reference<Allocator> alloc;
	data_ptr_t vertex_data;
	uint32_t vertex_count;
	uint32_t owned_capacity;
	Properties properties;

public:
	// Create a non-owning reference to the vertex data
	VertexArray(Allocator &alloc_p, data_ptr_t vertex_data_p, uint32_t vertex_count_p, bool has_z, bool has_m)
	    : alloc(alloc_p), vertex_data(vertex_data_p), vertex_count(vertex_count_p), owned_capacity(0),
	      properties(has_z, has_m) {
	}

	// Create an owning array with the given capacity
	VertexArray(Allocator &alloc_p, uint32_t owned_capacity_p, bool has_z, bool has_m)
	    : alloc(alloc_p),
	      vertex_data(owned_capacity_p == 0
	                      ? nullptr
	                      : alloc.get().AllocateData(owned_capacity_p *
	                                                 (sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0))))),
	      vertex_count(0), owned_capacity(owned_capacity_p), properties(has_z, has_m) {
	}

	// Create an empty non-owning array
	VertexArray(Allocator &alloc_p, bool has_z, bool has_m)
	    : alloc(alloc_p), vertex_data(nullptr), vertex_count(0), owned_capacity(0), properties(has_z, has_m) {
	}

	// Create an empty non-owning array
	static VertexArray CreateEmpty(Allocator &alloc_p, bool has_z, bool has_m) {
		return VertexArray {alloc_p, nullptr, 0, has_z, has_m};
	}

	~VertexArray() {
		if (IsOwning() && vertex_data != nullptr) {
			alloc.get().FreeData(vertex_data, owned_capacity * properties.VertexSize());
		}
	}

	//-------------------------------------------------------------------------
	// Copy (reference the same data but do not own it)
	//-------------------------------------------------------------------------

	VertexArray(const VertexArray &other)
	    : alloc(other.alloc), vertex_data(other.vertex_data), vertex_count(other.vertex_count), owned_capacity(0),
	      properties(other.properties) {
	}

	VertexArray &operator=(const VertexArray &other) {
		if (IsOwning() && vertex_data != nullptr) {
			alloc.get().FreeData(vertex_data, owned_capacity * properties.VertexSize());
		}
		alloc = other.alloc;
		vertex_data = other.vertex_data;
		vertex_count = other.vertex_count;
		owned_capacity = 0;
		properties = other.properties;
		return *this;
	}

	//-------------------------------------------------------------------------
	// Move (take ownership of the data)
	//-------------------------------------------------------------------------

	VertexArray(VertexArray &&other) noexcept
	    : alloc(other.alloc), vertex_data(other.vertex_data), vertex_count(other.vertex_count),
	      owned_capacity(other.owned_capacity), properties(other.properties) {
		other.vertex_data = nullptr;
		other.vertex_count = 0;
		other.owned_capacity = 0;
	}

	VertexArray &operator=(VertexArray &&other) noexcept {
		if (IsOwning() && vertex_data != nullptr) {
			alloc.get().FreeData(vertex_data, owned_capacity * properties.VertexSize());
		}
		alloc = other.alloc;
		vertex_data = other.vertex_data;
		vertex_count = other.vertex_count;
		owned_capacity = other.owned_capacity;
		other.vertex_data = nullptr;
		other.vertex_count = 0;
		other.owned_capacity = 0;
		return *this;
	}

	//-------------------------------------------------------------------------
	// CoW
	//-------------------------------------------------------------------------

	bool IsOwning() const {
		return owned_capacity > 0;
	}

	void MakeOwning() {
		if (IsOwning()) {
			return;
		}

		// Ensure that we at least have a capacity of 1
		auto new_capacity = vertex_count == 0 ? 1 : vertex_count;
		auto new_data = alloc.get().AllocateData(new_capacity * properties.VertexSize());
		memcpy(new_data, vertex_data, vertex_count * properties.VertexSize());
		vertex_data = new_data;
		owned_capacity = new_capacity;
	}

	uint32_t Count() const {
		return vertex_count;
	}

	uint32_t ByteSize() const {
		return vertex_count * properties.VertexSize();
	}

	uint32_t Capacity() const {
		if (IsOwning()) {
			return owned_capacity;
		} else {
			return vertex_count;
		}
	}

	const Properties &GetProperties() const {
		return properties;
	}

	const_data_ptr_t GetData() const {
		return vertex_data;
	}

	//-------------------------------------------------------------------------
	// Set
	//-------------------------------------------------------------------------

	// This requires the vertex type to be the exact same as the stored vertex type
	template <class V>
	void SetTemplatedUnsafe(uint32_t i, const V &v) {
		static_assert(V::IS_VERTEX, "V must be a vertex type");
		D_ASSERT(IsOwning());                    // Only owning arrays can be modified
		D_ASSERT(i < vertex_count);              // Index out of bounds
		D_ASSERT(properties.HasM() == V::HAS_M); // M mismatch
		D_ASSERT(properties.HasZ() == V::HAS_Z); // Z mismatch
		auto offset = i * sizeof(V);
		Store<V>(v, vertex_data + offset);
	}

	// This requires the vertex type to be the exact same as the stored vertex type
	template <class V>
	void SetTemplated(uint32_t i, const V &v) {
		if (!IsOwning()) {
			MakeOwning();
		}
		SetTemplatedUnsafe(i, v);
	}

	// This is safe to call on XYZ, XYM and XYZM arrays
	void SetUnsafe(uint32_t i, const VertexXY &v) {
		D_ASSERT(IsOwning());       // Only owning arrays can be modified
		D_ASSERT(i < vertex_count); // Index out of bounds
		auto offset = i * properties.VertexSize();
		Store<VertexXY>(v, vertex_data + offset);
	}

	// This is safe to call on XYZ, XYM and XYZM arrays
	void Set(uint32_t i, const VertexXY &v) {
		if (!IsOwning()) {
			MakeOwning();
		}
		SetUnsafe(i, v);
	}

	//-------------------------------------------------------------------------
	// Append
	//-------------------------------------------------------------------------

	void AppendUnsafe(const VertexXY &v) {
		D_ASSERT(IsOwning()); // Only owning arrays can be modified
		auto offset = vertex_count * properties.VertexSize();
		Store<VertexXY>(v, vertex_data + offset);
		vertex_count++;
	}

	void Append(const VertexXY &v) {
		if (!IsOwning()) {
			MakeOwning();
		}
		Reserve(vertex_count + 1);
		AppendUnsafe(v);
	}

	//-------------------------------------------------------------------------
	// Reserve
	//-------------------------------------------------------------------------
	void Reserve(uint32_t count) {
        if (count > owned_capacity) {
            if (owned_capacity == 0) {
                // TODO: Optimize this, we can allocate the exact amount of memory we need
                MakeOwning();
            }
            auto vertex_size = properties.VertexSize();
            vertex_data = alloc.get().ReallocateData(vertex_data, owned_capacity * vertex_size, count * vertex_size);
            owned_capacity = count;
        }
	}

	//-------------------------------------------------------------------------
	// Get
	//-------------------------------------------------------------------------

	// This requires the vertex type to be the exact same as the stored vertex type
	template <class V>
	V GetTemplated(uint32_t i) const {
		static_assert(V::IS_VERTEX, "V must be a vertex type");
		D_ASSERT(i < vertex_count);              // Index out of bounds
		D_ASSERT(properties.HasM() == V::HAS_M); // M mismatch
		D_ASSERT(properties.HasZ() == V::HAS_Z); // Z mismatch
		auto offset = i * sizeof(V);
		return Load<V>(vertex_data + offset);
	}

	// This is safe to call on XYZ, XYM and XYZM arrays too
	VertexXY Get(uint32_t i) const {
		D_ASSERT(i < vertex_count); // Index out of bounds
		auto offset = i * properties.VertexSize();
		return Load<VertexXY>(vertex_data + offset);
	}

	//-------------------------------------------------------------------------
	// Length
	//-------------------------------------------------------------------------
	// TODO: Get rid of this
	double Length() const {
		double length = 0;
		if (Count() < 2) {
			return 0.0;
		}
		for (uint32_t i = 0; i < Count() - 1; i++) {
			auto p1 = Get(i);
			auto p2 = Get(i + 1);
			length += std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
		}
		return length;
	}

	bool IsClosed() const {
		if (Count() == 0) {
			return false;
		}
		if (Count() == 1) {
			return true;
		}
		// TODO: Check approximate equality?
		auto start = Get(0);
		auto end = Get(Count() - 1);
		return start.x == end.x && start.y == end.y;
	}

	bool IsEmpty() const {
		return Count() == 0;
	}

	//-------------------------------------------------------------------------
	// Change dimensions
	//-------------------------------------------------------------------------
	// TODO: Test this properly
	void UpdateVertexType(bool has_z, bool has_m) {
		if (properties.HasZ() == has_z && properties.HasM() == has_m) {
			return;
		}
		if (!IsOwning()) {
			MakeOwning();
		}
		auto old_vertex_size = properties.VertexSize();
		properties.SetZ(has_z);
		properties.SetM(has_m);

		auto new_vertex_size = properties.VertexSize();
		// Case 1: The new vertex size is larger than the old vertex size
		if (new_vertex_size > old_vertex_size) {
			vertex_data =
			    alloc.get().ReallocateData(vertex_data, vertex_count * old_vertex_size, vertex_count * new_vertex_size);
			// Loop backwards
			for (int64_t i = vertex_count - 1; i >= 0; i--) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				memmove(vertex_data + new_offset, vertex_data + old_offset, old_vertex_size);
				// zero out the new data
				memset(vertex_data + new_offset + old_vertex_size, 0, new_vertex_size - old_vertex_size);
			}
		}
		// Case 2: The new vertex size is equal to the old vertex size
		else if (new_vertex_size == old_vertex_size) {
			// This only happens when we are replacing z with m or m with z
			// In this case we just need to zero out the third dimension
			for (uint32_t i = 0; i < vertex_count; i++) {
				auto offset = i * new_vertex_size + sizeof(double) * 2;
				memset(vertex_data + offset, 0, sizeof(double));
			}
		}
		// Case 3: The new vertex size is smaller than the old vertex size.
		// In this case we need to allocate new memory and copy the data over to not lose any data
		else {
			auto new_data = alloc.get().AllocateData(vertex_count * new_vertex_size);
			for (uint32_t i = 0; i < vertex_count; i++) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				memcpy(new_data + new_offset, vertex_data + old_offset, new_vertex_size);
			}
			alloc.get().FreeData(vertex_data, vertex_count * old_vertex_size);
			vertex_data = new_data;
		}
	}
};

} // namespace core

} // namespace spatial