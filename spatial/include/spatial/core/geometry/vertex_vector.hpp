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

// A VertexArray represents a copy-on-write array of potentially non-owned vertex data
// A VertexArray will never free its data, even if it owns it as it expects to always be used with a ArenaAllocator
class VertexArray {
	class Properties {
		friend class VertexArray;

	private:
		static constexpr uint16_t HAS_Z = 0x1;
		static constexpr uint16_t HAS_M = 0x2;
		static constexpr uint16_t IS_OWNING = 0x4;

		uint16_t bitmask;
		uint16_t vertex_size;

		void SetHasZ(bool has_z) {
			bitmask = (bitmask & ~HAS_Z) | (has_z ? HAS_Z : 0);
			vertex_size = sizeof(double) * (2 + (has_z ? 1 : 0) + (HasM() ? 1 : 0));
		}
		void SetHasM(bool has_m) {
			bitmask = (bitmask & ~HAS_M) | (has_m ? HAS_M : 0);
			vertex_size = sizeof(double) * (2 + (HasZ() ? 1 : 0) + (has_m ? 1 : 0));
		}
		void SetOwning(bool is_owning) {
			bitmask = (bitmask & ~IS_OWNING) | (is_owning ? IS_OWNING : 0);
		}

		Properties() : bitmask(0), vertex_size(sizeof(double) * 2) {
		}

	public:
		bool HasZ() const {
			return (bitmask & HAS_Z) != 0;
		}
		bool HasM() const {
			return (bitmask & HAS_M) != 0;
		}
		bool IsOwning() const {
			return (bitmask & IS_OWNING) != 0;
		}
		uint32_t VertexSize() const {
			return vertex_size;
		}
		uint32_t Dimensions() const {
			return 2 + (HasZ() ? 1 : 0) + (HasM() ? 1 : 0);
		}
	};

	data_ptr_t vertex_data;
	uint32_t vertex_count;
	Properties properties;

	VertexArray(data_ptr_t data_p, uint32_t count_p, bool has_z, bool has_m, bool is_owning)
	    : vertex_data(data_p), vertex_count(count_p), properties(Properties()) {
		properties.SetHasZ(has_z);
		properties.SetHasM(has_m);
		properties.SetOwning(is_owning);
	}

public:
	// Create an empty vertex vector
	static VertexArray Empty(bool has_z = false, bool has_m = false) {
		return VertexArray(nullptr, 0, has_z, has_m, false);
	}

	// Create a new vertex vector with the given count and properties
	static VertexArray Create(ArenaAllocator &allocator, uint32_t count, bool has_z, bool has_m) {
		auto data = allocator.AllocateAligned(count * sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)));
		return VertexArray(data, count, has_z, has_m, true);
	}

	// Create a new vertex vector by referencing an existing vertex vector without copying the data
	static VertexArray Reference(const VertexArray &other) {
		return VertexArray(other.vertex_data, other.vertex_count, other.properties.HasZ(), other.properties.HasM(),
		                   false);
	}

	// Create a new vertex vector by referencing a subset of an existing vertex vector without copying the data
	static VertexArray Reference(const VertexArray &other, uint32_t ref_offset, uint32_t ref_count) {
		D_ASSERT(ref_offset + ref_count <= other.vertex_count);
		auto offset = ref_offset * other.properties.VertexSize();
		auto data = other.vertex_data + offset;
		return VertexArray(data, ref_count, other.properties.HasZ(), other.properties.HasM(), false);
	}

	// Create a new vertex vector by reference an existing data pointer without copying the data
	static VertexArray Reference(const_data_ptr_t data, uint32_t count, bool has_z, bool has_m) {
		return VertexArray(const_cast<data_ptr_t>(data), count, has_z, has_m, false);
	}

	// Create a new vector by copying the data from another vertex vector
	static VertexArray Copy(ArenaAllocator &allocator, const VertexArray &other) {
		auto data = allocator.AllocateAligned(other.ByteSize());
		memcpy(data, other.vertex_data, other.ByteSize());
		return VertexArray(data, other.vertex_count, other.properties.HasZ(), other.properties.HasM(), true);
	}

	// Create a new vector by copying the data from a data pointer
	static VertexArray Copy(ArenaAllocator &allocator, const_data_ptr_t data, uint32_t count, bool has_z, bool has_m) {
		auto vertex_size = sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));
		auto new_data = allocator.AllocateAligned(count * vertex_size);
		memcpy(new_data, data, count * vertex_size);
		return VertexArray(new_data, count, has_z, has_m, true);
	}

public:
	Properties GetProperties() const {
		return properties;
	}
	uint32_t Count() const {
		return vertex_count;
	}
	data_ptr_t GetData() const {
		return vertex_data;
	}
	bool IsOwning() const {
		return properties.IsOwning();
	}
	uint32_t ByteSize() const {
		return vertex_count * properties.VertexSize();
	}
	bool IsEmpty() const {
		return vertex_count == 0;
	}

	bool IsClosed() const;
	double Length() const;
	string ToString() const;

	// Potentially allocating methods
	void Append(ArenaAllocator &alloc, const VertexArray &other);
	void Resize(ArenaAllocator &alloc, uint32_t new_count);
	void SetVertexType(ArenaAllocator &alloc, bool has_z, bool has_m, double default_z = 0, double default_m = 0);
	void MakeOwning(ArenaAllocator &alloc);

public:
	// Get the vertex at the given index.
	// If the vertex type is not the same as the one stored in the vector, extra dimensions will be set to 0 or dropped.
	template <class V = VertexXY>
	inline V Get(uint32_t i) const = delete;

	inline void Set(uint32_t i, const VertexXY &v) {
		D_ASSERT(i < vertex_count);
		auto offset = i * properties.VertexSize();
		memcpy(vertex_data + offset, &v, sizeof(VertexXY));
	}

	inline void Set(uint32_t i, const VertexXYZ &v) {
		D_ASSERT(i < vertex_count);
		auto has_z = properties.HasZ();
		auto has_m = properties.HasM();
		if (has_z && has_m) {
			auto size = sizeof(VertexXYZM);
			auto offset = i * size;
			VertexXYZM tmp {v.x, v.y, v.z, 0};
			memcpy(vertex_data + offset, &tmp, size);
		} else if (has_m) {
			auto size = sizeof(VertexXYM);
			auto offset = i * size;
			VertexXYM tmp {v.x, v.y, 0};
			// Only copy the X and Y components
			memcpy(vertex_data + offset, &tmp, size);
		} else if (has_z) {
			auto size = sizeof(VertexXYZ);
			auto offset = i * size;
			memcpy(vertex_data + offset, &v, size);
		} else {
			auto size = sizeof(VertexXY);
			auto offset = i * size;
			memcpy(vertex_data + offset, &v, size);
		}
	}

	inline void Set(uint32_t i, const VertexXYM &v) {
		D_ASSERT(i < vertex_count);
		auto has_z = properties.HasZ();
		auto has_m = properties.HasM();
		if (has_z && has_m) {
			auto size = sizeof(VertexXYZM);
			auto offset = i * size;
			VertexXYZM tmp {v.x, v.y, 0, v.m};
			memcpy(vertex_data + offset, &tmp, size);
		} else if (has_m) {
			auto size = sizeof(VertexXYM);
			auto offset = i * size;
			memcpy(vertex_data + offset, &v, size);
		} else if (has_z) {
			auto size = sizeof(VertexXYZ);
			auto offset = i * size;
			VertexXYZ tmp {v.x, v.y, 0};
			// Only copy the X and Y components
			memcpy(vertex_data + offset, &tmp, size);
		} else {
			auto size = sizeof(VertexXY);
			auto offset = i * size;
			memcpy(vertex_data + offset, &v, size);
		}
	}

	inline void Set(uint32_t i, const VertexXYZM &v) {
		D_ASSERT(i < vertex_count);
		auto has_z = properties.HasZ();
		auto has_m = properties.HasM();

		if (!has_z && has_m) {
			// We need to shift the M value
			auto size = sizeof(VertexXYM);
			auto offset = i * size;
			VertexXYM tmp {v.x, v.y, v.m};
			memcpy(vertex_data + offset, &tmp, size);
		} else {
			// All the other cases are the same
			auto size = properties.VertexSize();
			auto offset = i * size;
			memcpy(vertex_data + offset, &v, size);
		}
	}

	inline void Set(uint32_t i, double x, double y) {
		Set(i, VertexXY {x, y});
	}
	inline void Set(uint32_t i, double x, double y, double z) {
		Set(i, VertexXYZ {x, y, z});
	}
	inline void Set(uint32_t i, double x, double y, double z, double m) {
		Set(i, VertexXYZM {x, y, z, m});
	}

	// Get the exact vertex type. This requires the vertex type to be the same as the stored vertex type
	template <class V>
	inline V GetExact(uint32_t i) const {
		static_assert(V::IS_VERTEX, "V must be a vertex type");
		D_ASSERT(i < vertex_count);
		D_ASSERT(properties.HasM() == V::HAS_M);
		D_ASSERT(properties.HasZ() == V::HAS_Z);
		auto offset = i * sizeof(V);
		V result = {0};
		memcpy(&result, vertex_data + offset, sizeof(V));
		return result;
	}

	// Set the exact vertex type. This requires the vertex type to be the same as the stored vertex type
	template <class V>
	inline void SetExact(uint32_t i, const V &v) {
		static_assert(V::IS_VERTEX, "V must be a vertex type");
		D_ASSERT(i < vertex_count);
		D_ASSERT(properties.HasM() == V::HAS_M);
		D_ASSERT(properties.HasZ() == V::HAS_Z);
		auto offset = i * sizeof(V);
		memcpy(vertex_data + offset, &v, sizeof(V));
	}
};

// Specialize for VertexXY
template <>
inline VertexXY VertexArray::Get<VertexXY>(uint32_t i) const {
    D_ASSERT(i < vertex_count);
    auto offset = i * properties.VertexSize();
    VertexXY result = {0};
    // Every vertex type has a x and y component so we can memcpy constant size
    memcpy(&result, vertex_data + offset, sizeof(VertexXY));
    return result;
}

// Specialize for VertexXYZ
template <>
inline VertexXYZ VertexArray::Get<VertexXYZ>(uint32_t i) const {
    D_ASSERT(i < vertex_count);
    auto has_z = properties.HasZ();
    auto has_m = properties.HasM();

    VertexXYZ result = {0};
    if (has_z && has_m) {
        auto size = sizeof(VertexXYZM);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, sizeof(VertexXYZ));
    } else if (has_z) {
        auto size = sizeof(VertexXYZ);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    } else if (has_m) {
        auto size = sizeof(VertexXYM);
        auto offset = i * size;
        // Only copy the X and Y components
        memcpy(&result, vertex_data + offset, sizeof(VertexXY));
    } else {
        auto size = sizeof(VertexXY);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    }
    return result;
}

// Specialize for VertexXYM
template <>
inline VertexXYM VertexArray::Get<VertexXYM>(uint32_t i) const {
    D_ASSERT(i < vertex_count);
    auto has_z = properties.HasZ();
    auto has_m = properties.HasM();

    VertexXYM result = {0};
    if (has_z && has_m) {
        auto size = sizeof(VertexXYZM);
        auto offset = i * size;
        // Copy the X and Y components
        memcpy(&result, vertex_data + offset, sizeof(VertexXY));
        // Copy the M component
        memcpy(&result.m, vertex_data + offset + sizeof(VertexXYZ), sizeof(double));
    } else if (has_z) {
        auto size = sizeof(VertexXYZ);
        auto offset = i * size;
        // Only copy the X and Y components
        memcpy(&result, vertex_data + offset, sizeof(VertexXY));
    } else if (has_m) {
        auto size = sizeof(VertexXYM);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    } else {
        auto size = sizeof(VertexXY);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    }
    return result;
}

// Specialize for VertexXYZM
template <>
inline VertexXYZM VertexArray::Get<VertexXYZM>(uint32_t i) const {
    D_ASSERT(i < vertex_count);
    auto has_z = properties.HasZ();
    auto has_m = properties.HasM();

    VertexXYZM result = {0};
    if (has_z && has_m) {
        auto size = sizeof(VertexXYZM);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    } else if (has_z) {
        auto size = sizeof(VertexXYZ);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    } else if (has_m) {
        auto size = sizeof(VertexXYM);
        auto offset = i * size;
        // Copy the X and Y components
        memcpy(&result, vertex_data + offset, sizeof(VertexXY));
        // Copy the M component
        memcpy(&result.m, vertex_data + offset + sizeof(VertexXY), sizeof(double));
    } else {
        auto size = sizeof(VertexXY);
        auto offset = i * size;
        memcpy(&result, vertex_data + offset, size);
    }
    return result;
}


} // namespace core

} // namespace spatial