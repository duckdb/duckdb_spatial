#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/util/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/geometry/vertex.hpp"

namespace spatial {

namespace core {

class Geometry;

//------------------------------------------------------------------------------
// Geometry
//------------------------------------------------------------------------------

class Geometry {
	friend struct SinglePartGeometry;
	friend struct MultiPartGeometry;
	friend struct CollectionGeometry;

private:
	GeometryType type;
	GeometryProperties properties;
	bool is_readonly;
	uint32_t data_count;
	data_ptr_t data_ptr;

	Geometry(GeometryType type, GeometryProperties props, bool is_readonly, data_ptr_t data, uint32_t count)
	    : type(type), properties(props), is_readonly(is_readonly), data_count(count), data_ptr(data) {
	}

	// TODO: Maybe make these public...
	Geometry &operator[](uint32_t index) {
		D_ASSERT(index < data_count);
		return reinterpret_cast<Geometry *>(data_ptr)[index];
	}
	Geometry *begin() {
		return reinterpret_cast<Geometry *>(data_ptr);
	}
	Geometry *end() {
		return reinterpret_cast<Geometry *>(data_ptr) + data_count;
	}

	const Geometry &operator[](uint32_t index) const {
		D_ASSERT(index < data_count);
		return reinterpret_cast<const Geometry *>(data_ptr)[index];
	}
	const Geometry *begin() const {
		return reinterpret_cast<const Geometry *>(data_ptr);
	}
	const Geometry *end() const {
		return reinterpret_cast<const Geometry *>(data_ptr) + data_count;
	}

public:
	// By default, create a read-only empty point
	Geometry()
	    : type(GeometryType::POINT), properties(false, false), is_readonly(true), data_count(0), data_ptr(nullptr) {
	}

	Geometry(GeometryType type, bool has_z, bool has_m)
	    : type(type), properties(has_z, has_m), is_readonly(true), data_count(0), data_ptr(nullptr) {
	}

	// Copy Constructor
	Geometry(const Geometry &other)
	    : type(other.type), properties(other.properties), is_readonly(true), data_count(other.data_count),
	      data_ptr(other.data_ptr) {
	}

	// Copy Assignment
	Geometry &operator=(const Geometry &other) {
		type = other.type;
		properties = other.properties;
		is_readonly = true;
		data_count = other.data_count;
		data_ptr = other.data_ptr;
		return *this;
	}

	// Move Constructor
	Geometry(Geometry &&other) noexcept
	    : type(other.type), properties(other.properties), is_readonly(other.is_readonly), data_count(other.data_count),
	      data_ptr(other.data_ptr) {
		if (!other.is_readonly) {
			// Take ownership of the data, and make the other object read-only
			other.is_readonly = true;
		}
	}

	// Move Assignment
	Geometry &operator=(Geometry &&other) noexcept {
		type = other.type;
		properties = other.properties;
		is_readonly = other.is_readonly;
		data_count = other.data_count;
		data_ptr = other.data_ptr;
		if (!other.is_readonly) {
			// Take ownership of the data, and make the other object read-only
			other.is_readonly = true;
		}
		return *this;
	}

public:
	GeometryType GetType() const {
		return type;
	}
	GeometryProperties &GetProperties() {
		return properties;
	}
	const GeometryProperties &GetProperties() const {
		return properties;
	}
	const_data_ptr_t GetData() const {
		return data_ptr;
	}
	data_ptr_t GetData() {
		return data_ptr;
	}
	bool IsReadOnly() const {
		return is_readonly;
	}
	uint32_t Count() const {
		return data_count;
	}

	bool IsCollection() const {
		return GeometryTypes::IsCollection(type);
	}
	bool IsMultiPart() const {
		return GeometryTypes::IsMultiPart(type);
	}
	bool IsSinglePart() const {
		return GeometryTypes::IsSinglePart(type);
	}

public:
	// Used for tag dispatching
	struct Tags {
		// Base types
		struct AnyGeometry {};
		struct SinglePartGeometry : public AnyGeometry {};
		struct MultiPartGeometry : public AnyGeometry {};
		struct CollectionGeometry : public MultiPartGeometry {};
		// Concrete types
		struct Point : public SinglePartGeometry {};
		struct LineString : public SinglePartGeometry {};
		struct Polygon : public MultiPartGeometry {};
		struct MultiPoint : public CollectionGeometry {};
		struct MultiLineString : public CollectionGeometry {};
		struct MultiPolygon : public CollectionGeometry {};
		struct GeometryCollection : public CollectionGeometry {};
	};

	template <class T, class... ARGS>
	static auto Match(Geometry &geom, ARGS &&...args)
	    -> decltype(T::Case(std::declval<Tags::Point>(), std::declval<Geometry &>(), std::declval<ARGS>()...)) {
		switch (geom.type) {
		case GeometryType::POINT:
			return T::Case(Tags::Point {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::LINESTRING:
			return T::Case(Tags::LineString {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::POLYGON:
			return T::Case(Tags::Polygon {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOINT:
			return T::Case(Tags::MultiPoint {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::MULTILINESTRING:
			return T::Case(Tags::MultiLineString {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOLYGON:
			return T::Case(Tags::MultiPolygon {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::GEOMETRYCOLLECTION:
			return T::Case(Tags::GeometryCollection {}, geom, std::forward<ARGS>(args)...);
		default:
			throw NotImplementedException("Geometry::Match");
		}
	}

	template <class T, class... ARGS>
	static auto Match(const Geometry &geom, ARGS &&...args)
	    -> decltype(T::Case(std::declval<Tags::Point>(), std::declval<Geometry &>(), std::declval<ARGS>()...)) {
		switch (geom.type) {
		case GeometryType::POINT:
			return T::Case(Tags::Point {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::LINESTRING:
			return T::Case(Tags::LineString {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::POLYGON:
			return T::Case(Tags::Polygon {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOINT:
			return T::Case(Tags::MultiPoint {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::MULTILINESTRING:
			return T::Case(Tags::MultiLineString {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOLYGON:
			return T::Case(Tags::MultiPolygon {}, geom, std::forward<ARGS>(args)...);
		case GeometryType::GEOMETRYCOLLECTION:
			return T::Case(Tags::GeometryCollection {}, geom, std::forward<ARGS>(args)...);
		default:
			throw NotImplementedException("Geometry::Match");
		}
	}

	// TODO: Swap this to only have two create methods,
	// and use mutating methods for Reference/Copy
	static Geometry Create(ArenaAllocator &alloc, GeometryType type, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(GeometryType type, bool has_z, bool has_m);

	static geometry_t Serialize(const Geometry &geom, Vector &result);
	static Geometry Deserialize(ArenaAllocator &arena, const geometry_t &data);

	static bool IsEmpty(const Geometry &geom);
	static uint32_t GetDimension(const Geometry &geom, bool recurse);
	void SetVertexType(ArenaAllocator &alloc, bool has_z, bool has_m, double default_z = 0, double default_m = 0);

	// Iterate over all points in the geometry, recursing into collections
	template <class FUNC>
	static void ExtractPoints(const Geometry &geom, FUNC &&func);

	// Iterate over all lines in the geometry, recursing into collections
	template <class FUNC>
	static void ExtractLines(const Geometry &geom, FUNC &&func);

	// Iterate over all polygons in the geometry, recursing into collections
	template <class FUNC>
	static void ExtractPolygons(const Geometry &geom, FUNC &&func);
};

inline Geometry Geometry::Create(ArenaAllocator &alloc, GeometryType type, uint32_t count, bool has_z, bool has_m) {
	GeometryProperties props(has_z, has_m);
	auto single_part = GeometryTypes::IsSinglePart(type);
	auto elem_size = single_part ? props.VertexSize() : sizeof(Geometry);
	auto geom = Geometry(type, props, false, alloc.AllocateAligned(count * elem_size), count);
	return geom;
}

inline Geometry Geometry::CreateEmpty(GeometryType type, bool has_z, bool has_m) {
	GeometryProperties props(has_z, has_m);
	return Geometry(type, props, false, nullptr, 0);
}

//------------------------------------------------------------------------------
// Inlined Geometry Functions
//------------------------------------------------------------------------------
template <class FUNC>
inline void Geometry::ExtractPoints(const Geometry &geom, FUNC &&func) {
	struct op {
		static void Case(Geometry::Tags::Point, const Geometry &geom, FUNC &&func) {
			func(geom);
		}
		static void Case(Geometry::Tags::MultiPoint, const Geometry &geom, FUNC &&func) {
			for (auto &part : geom) {
				func(part);
			}
		}
		static void Case(Geometry::Tags::GeometryCollection, const Geometry &geom, FUNC &&func) {
			for (auto &part : geom) {
				Match<op>(part, std::forward<FUNC>(func));
			}
		}
		static void Case(Geometry::Tags::AnyGeometry, const Geometry &, FUNC &&) {
		}
	};
	Match<op>(geom, std::forward<FUNC>(func));
}

template <class FUNC>
inline void Geometry::ExtractLines(const Geometry &geom, FUNC &&func) {
	struct op {
		static void Case(Geometry::Tags::LineString, const Geometry &geom, FUNC &&func) {
			func(geom);
		}
		static void Case(Geometry::Tags::MultiLineString, const Geometry &geom, FUNC &&func) {
			for (auto &part : geom) {
				func(part);
			}
		}
		static void Case(Geometry::Tags::GeometryCollection, const Geometry &geom, FUNC &&func) {
			for (auto &part : geom) {
				Match<op>(part, std::forward<FUNC>(func));
			}
		}
		static void Case(Geometry::Tags::AnyGeometry, const Geometry &, FUNC &&) {
		}
	};
	Match<op>(geom, std::forward<FUNC>(func));
}

template <class FUNC>
inline void Geometry::ExtractPolygons(const Geometry &geom, FUNC &&func) {
	struct op {
		static void Case(Geometry::Tags::Polygon, const Geometry &geom, FUNC &&func) {
			func(geom);
		}
		static void Case(Geometry::Tags::MultiPolygon, const Geometry &geom, FUNC &&func) {
			for (auto &part : geom) {
				func(part);
			}
		}
		static void Case(Geometry::Tags::GeometryCollection, const Geometry &geom, FUNC &&func) {
			for (auto &part : geom) {
				Match<op>(part, std::forward<FUNC>(func));
			}
		}
		static void Case(Geometry::Tags::AnyGeometry, const Geometry &, FUNC &&) {
		}
	};
	Match<op>(geom, std::forward<FUNC>(func));
}

inline bool Geometry::IsEmpty(const Geometry &geom) {
	struct op {
		static bool Case(Geometry::Tags::SinglePartGeometry, const Geometry &geom) {
			return geom.data_count == 0;
		}
		static bool Case(Geometry::Tags::MultiPartGeometry, const Geometry &geom) {
			for (const auto &part : geom) {
				if (!Geometry::Match<op>(part)) {
					return false;
				}
			}
			return true;
		}
	};
	return Geometry::Match<op>(geom);
}

inline uint32_t Geometry::GetDimension(const Geometry &geom, bool ignore_empty) {
	if (ignore_empty && Geometry::IsEmpty(geom)) {
		return 0;
	}
	struct op {
		static uint32_t Case(Geometry::Tags::Point, const Geometry &, bool) {
			return 0;
		}
		static uint32_t Case(Geometry::Tags::LineString, const Geometry &, bool) {
			return 1;
		}
		static uint32_t Case(Geometry::Tags::Polygon, const Geometry &, bool) {
			return 2;
		}
		static uint32_t Case(Geometry::Tags::MultiPoint, const Geometry &, bool) {
			return 0;
		}
		static uint32_t Case(Geometry::Tags::MultiLineString, const Geometry &, bool) {
			return 1;
		}
		static uint32_t Case(Geometry::Tags::MultiPolygon, const Geometry &, bool) {
			return 2;
		}
		static uint32_t Case(Geometry::Tags::GeometryCollection, const Geometry &geom, bool ignore_empty) {
			uint32_t max_dimension = 0;
			for (const auto &p : geom) {
				max_dimension = std::max(max_dimension, Geometry::GetDimension(p, ignore_empty));
			}
			return max_dimension;
		}
	};
	return Geometry::Match<op>(geom, ignore_empty);
}

//------------------------------------------------------------------------------
// Iterators
//------------------------------------------------------------------------------
class PartView {
private:
	Geometry *beg_ptr;
	Geometry *end_ptr;

public:
	PartView(Geometry *beg, Geometry *end) : beg_ptr(beg), end_ptr(end) {
	}
	Geometry *begin() {
		return beg_ptr;
	}
	Geometry *end() {
		return end_ptr;
	}
	Geometry &operator[](uint32_t index) {
		return beg_ptr[index];
	}
};

class ConstPartView {
private:
	const Geometry *beg_ptr;
	const Geometry *end_ptr;

public:
	ConstPartView(const Geometry *beg, const Geometry *end) : beg_ptr(beg), end_ptr(end) {
	}
	const Geometry *begin() {
		return beg_ptr;
	}
	const Geometry *end() {
		return end_ptr;
	}
	const Geometry &operator[](uint32_t index) {
		return beg_ptr[index];
	}
};

//------------------------------------------------------------------------------
// Accessors
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// SinglePartGeometry
//------------------------------------------------------------------------------
struct SinglePartGeometry {

	// Turn this geometry into a read-only reference to raw data
	static void ReferenceData(Geometry &geom, const_data_ptr_t data, uint32_t count, bool has_z, bool has_m) {
		geom.data_count = count;
		geom.data_ptr = const_cast<data_ptr_t>(data);
		geom.is_readonly = true;
		geom.properties.SetZ(has_z);
		geom.properties.SetM(has_m);
	}

	// Turn this geometry into a read-only reference to another geometry, starting at the specified index
	static void ReferenceData(Geometry &geom, const Geometry &other, uint32_t offset, uint32_t count) {
		D_ASSERT(GeometryTypes::IsSinglePart(other.GetType()));
		D_ASSERT(offset + count <= other.data_count);
		auto vertex_size = other.properties.VertexSize();
		auto has_z = other.properties.HasZ();
		auto has_m = other.properties.HasM();
		ReferenceData(geom, other.data_ptr + offset * vertex_size, count, has_z, has_m);
	}

	static void ReferenceData(Geometry &geom, const_data_ptr_t data, uint32_t count) {
		ReferenceData(geom, data, count, geom.properties.HasZ(), geom.properties.HasM());
	}

	// Turn this geometry into a owning copy of raw data
	static void CopyData(Geometry &geom, ArenaAllocator &alloc, const_data_ptr_t data, uint32_t count, bool has_z,
	                     bool has_m) {
		auto old_vertex_size = geom.properties.VertexSize();
		geom.properties.SetZ(has_z);
		geom.properties.SetM(has_m);
		auto new_vertex_size = geom.properties.VertexSize();
		if (geom.is_readonly) {
			geom.data_ptr = alloc.AllocateAligned(count * new_vertex_size);
		} else if (geom.data_count != count) {
			geom.data_ptr =
			    alloc.ReallocateAligned(geom.data_ptr, geom.data_count * old_vertex_size, count * new_vertex_size);
		}
		memcpy(geom.data_ptr, data, count * new_vertex_size);
		geom.data_count = count;
		geom.is_readonly = false;
	}

	// Turn this geometry into a owning copy of another geometry, starting at the specified index
	static void CopyData(Geometry &geom, ArenaAllocator &alloc, const Geometry &other, uint32_t offset,
	                     uint32_t count) {
		D_ASSERT(GeometryTypes::IsSinglePart(other.GetType()));
		D_ASSERT(offset + count <= other.data_count);
		auto vertex_size = geom.properties.VertexSize();
		auto has_z = other.properties.HasZ();
		auto has_m = other.properties.HasM();
		CopyData(geom, alloc, other.data_ptr + offset * vertex_size, count, has_z, has_m);
	}

	static void CopyData(Geometry &geom, ArenaAllocator &alloc, const_data_ptr_t data, uint32_t count) {
		CopyData(geom, alloc, data, count, geom.properties.HasZ(), geom.properties.HasM());
	}

	// Resize the geometry, truncating or extending with zeroed vertices as needed
	static void Resize(Geometry &geom, ArenaAllocator &alloc, uint32_t new_count);

	// Append the data from another geometry
	static void Append(Geometry &geom, ArenaAllocator &alloc, const Geometry &other);

	// Append the data from multiple other geometries
	static void Append(Geometry &geom, ArenaAllocator &alloc, const Geometry *others, uint32_t others_count);

	// Force the geometry to have a specific vertex type, resizing or shrinking the data as needed
	static void SetVertexType(Geometry &geom, ArenaAllocator &alloc, bool has_z, bool has_m, double default_z = 0,
	                          double default_m = 0);

	// If this geometry is read-only, make it mutable by copying the data
	static void MakeMutable(Geometry &geom, ArenaAllocator &alloc);

	// Print this geometry as a string, starting at the specified index and printing the specified number of vertices
	// (useful for debugging)
	static string ToString(const Geometry &geom, uint32_t start = 0, uint32_t count = 0);

	// Check if the geometry is closed (first and last vertex are the same)
	// A geometry with 1 vertex is considered closed, 0 vertices are considered open
	static bool IsClosed(const Geometry &geom);
	static bool IsEmpty(const Geometry &geom);

	// Return the planar length of the geometry
	static double Length(const Geometry &geom);

	static VertexXY GetVertex(const Geometry &geom, uint32_t index);
	static void SetVertex(Geometry &geom, uint32_t index, const VertexXY &vertex);

	template <class V>
	static V GetVertex(const Geometry &geom, uint32_t index);

	template <class V>
	static void SetVertex(Geometry &geom, uint32_t index, const V &vertex);

	static uint32_t VertexCount(const Geometry &geom);
	static uint32_t VertexSize(const Geometry &geom);
	static uint32_t ByteSize(const Geometry &geom);
};

inline VertexXY SinglePartGeometry::GetVertex(const Geometry &geom, uint32_t index) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	D_ASSERT(index < geom.data_count);
	return Load<VertexXY>(geom.GetData() + index * geom.GetProperties().VertexSize());
}

inline void SinglePartGeometry::SetVertex(Geometry &geom, uint32_t index, const VertexXY &vertex) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	D_ASSERT(index < geom.data_count);
	Store(vertex, geom.GetData() + index * geom.GetProperties().VertexSize());
}

template <class V>
inline V SinglePartGeometry::GetVertex(const Geometry &geom, uint32_t index) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	D_ASSERT(V::HAS_Z == geom.GetProperties().HasZ());
	D_ASSERT(V::HAS_M == geom.GetProperties().HasM());
	D_ASSERT(index < geom.data_count);
	return Load<V>(geom.GetData() + index * sizeof(V));
}

template <class V>
inline void SinglePartGeometry::SetVertex(Geometry &geom, uint32_t index, const V &vertex) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	D_ASSERT(V::HAS_Z == geom.GetProperties().HasZ());
	D_ASSERT(V::HAS_M == geom.GetProperties().HasM());
	D_ASSERT(index < geom.data_count);
	Store(vertex, geom.GetData() + index * sizeof(V));
}

inline uint32_t SinglePartGeometry::VertexCount(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	return geom.data_count;
}

inline uint32_t SinglePartGeometry::VertexSize(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	return geom.GetProperties().VertexSize();
}

inline uint32_t SinglePartGeometry::ByteSize(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	return geom.data_count * geom.GetProperties().VertexSize();
}

inline bool SinglePartGeometry::IsEmpty(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.GetType()));
	return geom.data_count == 0;
}

//------------------------------------------------------------------------------
// MultiPartGeometry
//------------------------------------------------------------------------------
struct MultiPartGeometry {

	// static void Resize(Geometry &geom, ArenaAllocator &alloc, uint32_t new_count);

	static uint32_t PartCount(const Geometry &geom);
	static Geometry &Part(Geometry &geom, uint32_t index);
	static const Geometry &Part(const Geometry &geom, uint32_t index);
	static PartView Parts(Geometry &geom);
	static ConstPartView Parts(const Geometry &geom);

	static bool IsEmpty(const Geometry &geom) {
		D_ASSERT(GeometryTypes::IsMultiPart(geom.GetType()));
		for (uint32_t i = 0; i < geom.data_count; i++) {
			if (!Geometry::IsEmpty(Part(geom, i))) {
				return false;
			}
		}
		return true;
	}
};

inline uint32_t MultiPartGeometry::PartCount(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsMultiPart(geom.GetType()));
	return geom.data_count;
}

inline Geometry &MultiPartGeometry::Part(Geometry &geom, uint32_t index) {
	D_ASSERT(GeometryTypes::IsMultiPart(geom.GetType()));
	D_ASSERT(index < geom.data_count);
	return *reinterpret_cast<Geometry *>(geom.GetData() + index * sizeof(Geometry));
}

inline const Geometry &MultiPartGeometry::Part(const Geometry &geom, uint32_t index) {
	D_ASSERT(GeometryTypes::IsMultiPart(geom.GetType()));
	D_ASSERT(index < geom.data_count);
	return *reinterpret_cast<const Geometry *>(geom.GetData() + index * sizeof(Geometry));
}

inline PartView MultiPartGeometry::Parts(Geometry &geom) {
	D_ASSERT(GeometryTypes::IsMultiPart(geom.GetType()));
	auto ptr = reinterpret_cast<Geometry *>(geom.GetData());
	return {ptr, ptr + geom.data_count};
}

inline ConstPartView MultiPartGeometry::Parts(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsMultiPart(geom.GetType()));
	auto ptr = reinterpret_cast<const Geometry *>(geom.GetData());
	return {ptr, ptr + geom.data_count};
}

//------------------------------------------------------------------------------
// CollectionGeometry
//------------------------------------------------------------------------------
struct CollectionGeometry : public MultiPartGeometry {
protected:
	static Geometry Create(ArenaAllocator &alloc, GeometryType type, vector<Geometry> &items, bool has_z, bool has_m) {
		D_ASSERT(GeometryTypes::IsCollection(type));
		auto collection = Geometry::Create(alloc, type, items.size(), has_z, has_m);
		for (uint32_t i = 0; i < items.size(); i++) {
			CollectionGeometry::Part(collection, i) = std::move(items[i]);
		}
		return collection;
	}
};

//------------------------------------------------------------------------------
// Point
//------------------------------------------------------------------------------
struct Point : public SinglePartGeometry {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);

	template <class V>
	static Geometry CreateFromVertex(ArenaAllocator &alloc, const V &vertex);

	static Geometry CreateFromCopy(ArenaAllocator &alloc, const_data_ptr_t data, uint32_t count, bool has_z,
	                               bool has_m) {
		auto point = Point::Create(alloc, 1, has_z, has_m);
		SinglePartGeometry::CopyData(point, alloc, data, count, has_z, has_m);
		return point;
	}

	// Methods
	template <class V = VertexXY>
	static V GetVertex(const Geometry &geom);

	template <class V = VertexXY>
	static void SetVertex(Geometry &geom, const V &vertex);

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::POINT;
};

inline Geometry Point::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	return Geometry::Create(alloc, TYPE, count, has_z, has_m);
}

inline Geometry Point::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

template <class V>
inline Geometry Point::CreateFromVertex(ArenaAllocator &alloc, const V &vertex) {
	auto point = Create(alloc, 1, V::HAS_Z, V::HAS_M);
	Point::SetVertex(point, vertex);
	return point;
}

template <class V>
inline V Point::GetVertex(const Geometry &geom) {
	D_ASSERT(geom.GetType() == TYPE);
	D_ASSERT(geom.Count() == 1);
	D_ASSERT(geom.GetProperties().HasZ() == V::HAS_Z);
	D_ASSERT(geom.GetProperties().HasM() == V::HAS_M);
	return SinglePartGeometry::GetVertex<V>(geom, 0);
}

template <class V>
void Point::SetVertex(Geometry &geom, const V &vertex) {
	D_ASSERT(geom.GetType() == TYPE);
	D_ASSERT(geom.Count() == 1);
	D_ASSERT(geom.GetProperties().HasZ() == V::HAS_Z);
	D_ASSERT(geom.GetProperties().HasM() == V::HAS_M);
	SinglePartGeometry::SetVertex(geom, 0, vertex);
}

//------------------------------------------------------------------------------
// LineString
//------------------------------------------------------------------------------
struct LineString : public SinglePartGeometry {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);

	static Geometry CreateFromCopy(ArenaAllocator &alloc, const_data_ptr_t data, uint32_t count, bool has_z,
	                               bool has_m) {
		auto line = LineString::Create(alloc, 1, has_z, has_m);
		SinglePartGeometry::CopyData(line, alloc, data, count, has_z, has_m);
		return line;
	}

	// TODO: Wrap
	// Create a new LineString referencing a slice of the this linestring
	static Geometry GetSliceAsReference(const Geometry &geom, uint32_t start, uint32_t count) {
		auto line = LineString::CreateEmpty(geom.GetProperties().HasZ(), geom.GetProperties().HasM());
		SinglePartGeometry::ReferenceData(line, geom, start, count);
		return line;
	}

	// TODO: Wrap
	// Create a new LineString referencing a single point in the this linestring
	static Geometry GetPointAsReference(const Geometry &geom, uint32_t index) {
		auto count = index >= geom.Count() ? 0 : 1;
		auto point = Point::CreateEmpty(geom.GetProperties().HasZ(), geom.GetProperties().HasM());
		SinglePartGeometry::ReferenceData(point, geom, index, count);
		return point;
	}

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::LINESTRING;
};

inline Geometry LineString::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	return Geometry::Create(alloc, TYPE, count, has_z, has_m);
}

inline Geometry LineString::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

//------------------------------------------------------------------------------
// LinearRing (special case of LineString)
//------------------------------------------------------------------------------
struct LinearRing : public LineString {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);

	// Methods
	static bool IsClosed(const Geometry &geom);

	// Constants
	// TODO: We dont have a LinearRing type, so we use LineString for now
	static const constexpr GeometryType TYPE = GeometryType::LINESTRING;
};

inline Geometry LinearRing::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	return LineString::Create(alloc, count, has_z, has_m);
}

inline Geometry LinearRing::CreateEmpty(bool has_z, bool has_m) {
	return LineString::CreateEmpty(has_z, has_m);
}

inline bool LinearRing::IsClosed(const Geometry &geom) {
	D_ASSERT(geom.GetType() == TYPE);
	// The difference between LineString is that a empty LinearRing is considered closed
	if (LinearRing::IsEmpty(geom)) {
		return true;
	}
	return LineString::IsClosed(geom);
}

//------------------------------------------------------------------------------
// Polygon
//------------------------------------------------------------------------------
struct Polygon : public MultiPartGeometry {
	// Constructors
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);
	static Geometry CreateFromBox(ArenaAllocator &alloc, double minx, double miny, double maxx, double maxy);

	// Methods
	static const Geometry &ExteriorRing(const Geometry &geom);
	static Geometry &ExteriorRing(Geometry &geom);

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::POLYGON;
};

inline Geometry Polygon::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	auto geom = Geometry::Create(alloc, TYPE, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		// Placement new
		new (&Polygon::Part(geom, i)) Geometry(GeometryType::LINESTRING, has_z, has_m);
	}
	return geom;
}

inline Geometry Polygon::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

inline Geometry Polygon::CreateFromBox(ArenaAllocator &alloc, double minx, double miny, double maxx, double maxy) {
	auto polygon = Polygon::Create(alloc, 1, false, false);
	auto &ring = Polygon::Part(polygon, 0);
	LineString::Resize(ring, alloc, 5);
	LineString::SetVertex(ring, 0, {minx, miny});
	LineString::SetVertex(ring, 1, {minx, maxy});
	LineString::SetVertex(ring, 2, {maxx, maxy});
	LineString::SetVertex(ring, 3, {maxx, miny});
	LineString::SetVertex(ring, 4, {minx, miny});
	return polygon;
}

inline Geometry &Polygon::ExteriorRing(Geometry &geom) {
	D_ASSERT(geom.GetType() == TYPE);
	D_ASSERT(Polygon::PartCount(geom) > 0);
	return Polygon::Part(geom, 0);
}

inline const Geometry &Polygon::ExteriorRing(const Geometry &geom) {
	D_ASSERT(geom.GetType() == TYPE);
	D_ASSERT(Polygon::PartCount(geom) > 0);
	return Polygon::Part(geom, 0);
}

//------------------------------------------------------------------------------
// MultiPoint
//------------------------------------------------------------------------------
struct MultiPoint : public CollectionGeometry {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);
	static Geometry Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m);

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::MULTIPOINT;
};

inline Geometry MultiPoint::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	auto geom = Geometry::Create(alloc, TYPE, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		// Placement new
		new (&MultiPoint::Part(geom, i)) Geometry(GeometryType::POINT, has_z, has_m);
	}
	return geom;
}

inline Geometry MultiPoint::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

inline Geometry MultiPoint::Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m) {
	return CollectionGeometry::Create(alloc, TYPE, items, has_z, has_m);
}

//------------------------------------------------------------------------------
// MultiLineString
//------------------------------------------------------------------------------
struct MultiLineString : public CollectionGeometry {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);
	static Geometry Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m);

	static bool IsClosed(const Geometry &geom);

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::MULTILINESTRING;
};

inline Geometry MultiLineString::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	auto geom = Geometry::Create(alloc, TYPE, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		// Placement new
		new (&MultiLineString::Part(geom, i)) Geometry(GeometryType::LINESTRING, has_z, has_m);
	}
	return geom;
}

inline Geometry MultiLineString::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

inline Geometry MultiLineString::Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m) {
	return CollectionGeometry::Create(alloc, TYPE, items, has_z, has_m);
}

inline bool MultiLineString::IsClosed(const Geometry &geom) {
	if (MultiLineString::PartCount(geom) == 0) {
		return false;
	}
	for (auto &part : MultiLineString::Parts(geom)) {
		if (!LineString::IsClosed(part)) {
			return false;
		}
	}
	return true;
}

//------------------------------------------------------------------------------
// MultiPolygon
//------------------------------------------------------------------------------
struct MultiPolygon : public CollectionGeometry {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);
	static Geometry Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m);

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::MULTIPOLYGON;
};

inline Geometry MultiPolygon::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	auto geom = Geometry::Create(alloc, TYPE, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		// Placement new
		new (&MultiPolygon::Part(geom, i)) Geometry(GeometryType::POLYGON, has_z, has_m);
	}
	return geom;
}

inline Geometry MultiPolygon::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

inline Geometry MultiPolygon::Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m) {
	return CollectionGeometry::Create(alloc, TYPE, items, has_z, has_m);
}

//------------------------------------------------------------------------------
// GeometryCollection
//------------------------------------------------------------------------------
struct GeometryCollection : public CollectionGeometry {
	static Geometry Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
	static Geometry CreateEmpty(bool has_z, bool has_m);
	static Geometry Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m);

	// Constants
	static const constexpr GeometryType TYPE = GeometryType::GEOMETRYCOLLECTION;
};

inline Geometry GeometryCollection::Create(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m) {
	auto geom = Geometry::Create(alloc, TYPE, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		// Placement new
		new (&GeometryCollection::Part(geom, i)) Geometry(GeometryType::GEOMETRYCOLLECTION, has_z, has_m);
	}
	return geom;
}

inline Geometry GeometryCollection::CreateEmpty(bool has_z, bool has_m) {
	return Geometry::CreateEmpty(TYPE, has_z, has_m);
}

inline Geometry GeometryCollection::Create(ArenaAllocator &alloc, vector<Geometry> &items, bool has_z, bool has_m) {
	return CollectionGeometry::Create(alloc, TYPE, items, has_z, has_m);
}

//------------------------------------------------------------------------------
// Assertions
//------------------------------------------------------------------------------

static_assert(std::is_standard_layout<Geometry>::value, "Geometry must be standard layout");

} // namespace core

} // namespace spatial