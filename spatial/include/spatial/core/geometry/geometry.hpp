#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

namespace spatial {

namespace core {

// A serialized geometry
class geometry_t {
private:
	string_t data;

public:
	geometry_t() = default;
	// NOLINTNEXTLINE
	explicit geometry_t(string_t data) : data(data) {
	}

	// NOLINTNEXTLINE
	operator string_t() const {
		return data;
	}

	GeometryType GetType() const {
		return Load<GeometryType>(const_data_ptr_cast(data.GetPrefix()));
	}
	GeometryProperties GetProperties() const {
		return Load<GeometryProperties>(const_data_ptr_cast(data.GetPrefix() + 1));
	}
	uint16_t GetHash() const {
		return Load<uint16_t>(const_data_ptr_cast(data.GetPrefix() + 2));
	}
};

static_assert(sizeof(geometry_t) == sizeof(string_t), "geometry_t should be the same size as string_t");

//------------------------------------------------------------------------------
// Geometry Objects
//------------------------------------------------------------------------------

template <class T>
class IteratorPair {
	T *begin_ptr;
	T *end_ptr;

public:
	IteratorPair(T *begin_ptr, T *end_ptr) : begin_ptr(begin_ptr), end_ptr(end_ptr) {
	}

	T *begin() {
		return begin_ptr;
	}

	T *end() {
		return end_ptr;
	}
};

template <class T>
class ConstIteratorPair {
	const T *begin_ptr;
	const T *end_ptr;

public:
	ConstIteratorPair(const T *begin_ptr, const T *end_ptr) : begin_ptr(begin_ptr), end_ptr(end_ptr) {
	}

	const T *begin() {
		return begin_ptr;
	}

	const T *end() {
		return end_ptr;
	}
};

struct Utils {
	static string format_coord(double d);
	static string format_coord(double x, double y);
	static string format_coord(double x, double y, double z);
	static string format_coord(double x, double y, double z, double m);

	static inline float DoubleToFloatDown(double d) {
		if (d > static_cast<double>(std::numeric_limits<float>::max())) {
			return std::numeric_limits<float>::max();
		}
		if (d <= static_cast<double>(std::numeric_limits<float>::lowest())) {
			return std::numeric_limits<float>::lowest();
		}

		auto f = static_cast<float>(d);
		if (static_cast<double>(f) <= d) {
			return f;
		}
		return std::nextafter(f, std::numeric_limits<float>::lowest());
	}

	static inline float DoubleToFloatUp(double d) {
		if (d >= static_cast<double>(std::numeric_limits<float>::max())) {
			return std::numeric_limits<float>::max();
		}
		if (d < static_cast<double>(std::numeric_limits<float>::lowest())) {
			return std::numeric_limits<float>::lowest();
		}

		auto f = static_cast<float>(d);
		if (static_cast<double>(f) >= d) {
			return f;
		}
		return std::nextafter(f, std::numeric_limits<float>::max());
	}
};

struct BoundingBox {
	double minx = std::numeric_limits<double>::max();
	double miny = std::numeric_limits<double>::max();
	double maxx = std::numeric_limits<double>::lowest();
	double maxy = std::numeric_limits<double>::lowest();

	bool Intersects(const BoundingBox &other) const {
		return !(minx > other.maxx || maxx < other.minx || miny > other.maxy || maxy < other.miny);
	}
};

// Custom STL Allocator
// No realloc though :(
template <class T>
struct DuckDBAllocator {
	using value_type = T;

	DuckDBAllocator(Allocator &allocator) : allocator(allocator) {
	}
	DuckDBAllocator(const DuckDBAllocator &other) : allocator(other.allocator) {
	}
	template <class U>
	DuckDBAllocator(const DuckDBAllocator<U> &other) : allocator(other.allocator) {
	}

	// Required
	T *allocate(std::size_t n) {
		return reinterpret_cast<T *>(allocator.get().AllocateData(n * sizeof(T)));
	}
	// Required
	void deallocate(T *p, std::size_t n) {
		allocator.get().FreeData(reinterpret_cast<data_ptr_t>(p), n * sizeof(T));
	}

	bool operator==(const DuckDBAllocator &other) const {
		return true;
	}

	bool operator!=(const DuckDBAllocator &other) const {
		return false;
	}

private:
	template <class U>
	friend struct DuckDBAllocator;

	reference<Allocator> allocator;
};

class Geometry;

class Point {
private:
	VertexArray vertices;

public:
	static constexpr GeometryType TYPE = GeometryType::POINT;
	explicit Point(Allocator &allocator) : vertices(allocator, false, false) {
	}
	explicit Point(Allocator &allocator, bool has_z, bool has_m) : vertices(allocator, has_z, has_m) {
	}
	explicit Point(VertexArray &&vertices) : vertices(std::move(vertices)) {
	}

	// NOLINTNEXTLINE
	operator Geometry() const;

	string ToString() const;

	VertexArray &Vertices() {
		return vertices;
	}

	const VertexArray &Vertices() const {
		return vertices;
	}

	bool IsEmpty() const {
		return vertices.IsEmpty();
	}

	uint32_t Dimension() const {
		return 0;
	}
};

class LineString {
private:
	VertexArray vertices;

public:
	static constexpr GeometryType TYPE = GeometryType::LINESTRING;
	explicit LineString(Allocator &allocator) : vertices(allocator, false, false) {
	}
	explicit LineString(Allocator &allocator, bool has_z, bool has_m) : vertices(allocator, has_z, has_m) {
	}
	explicit LineString(VertexArray &&vertices) : vertices(std::move(vertices)) {
	}
	// NOLINTNEXTLINE
	operator Geometry() const;

	string ToString() const;

	VertexArray &Vertices() {
		return vertices;
	}

	const VertexArray &Vertices() const {
		return vertices;
	}

	bool IsEmpty() const {
		return vertices.IsEmpty();
	}

	uint32_t Dimension() const {
		return 1;
	}
};

class Polygon {
private:
	std::vector<VertexArray, DuckDBAllocator<VertexArray>> rings;

public:
	static constexpr GeometryType TYPE = GeometryType::POLYGON;
	explicit Polygon(Allocator &allocator) : rings(allocator) {
	}
	explicit Polygon(Allocator &allocator, uint32_t num_rings, bool has_z, bool has_m) : rings(allocator) {
		rings.reserve(num_rings);
		for (uint32_t i = 0; i < num_rings; i++) {
			rings.emplace_back(allocator, has_z, has_m);
		}
	}

	string ToString() const;

	// Implicitly convert to Geometry
	// NOLINTNEXTLINE
	operator Geometry() const;

	// Collection Methods
	VertexArray &operator[](uint32_t index) {
		D_ASSERT(index < rings.size());
		return rings[index];
	}
	const VertexArray &operator[](uint32_t index) const {
		D_ASSERT(index < rings.size());
		return rings[index];
	}
	VertexArray *begin() {
		return rings.data();
	}
	VertexArray *end() {
		return rings.data() + rings.size();
	}
	const VertexArray *begin() const {
		return rings.data();
	}
	const VertexArray *end() const {
		return rings.data() + rings.size();
	}

	uint32_t RingCount() const {
		return rings.size();
	}

	bool IsEmpty() const {
		for (const auto &ring : rings) {
			if (!ring.IsEmpty()) {
				return false;
			}
		}
		return true;
	}

	uint32_t Dimension() const {
		return 2;
	}
};

template <class T>
class MultiGeometry {
	std::vector<T, DuckDBAllocator<T>> items;

protected:
	const std::vector<T, DuckDBAllocator<T>> &Items() const {
		return items;
	}

public:
	explicit MultiGeometry(Allocator &allocator, uint32_t count) : items(allocator) {
		items.reserve(count);
		for (uint32_t i = 0; i < count; i++) {
			items.emplace_back(allocator);
		}
	}

	T &operator[](uint32_t index) {
		D_ASSERT(index < items.size());
		return items[index];
	}
	const T &operator[](uint32_t index) const {
		D_ASSERT(index < items.size());
		return items[index];
	}
	T *begin() {
		return items.data();
	}
	T *end() {
		return items.data() + items.size();
	}
	const T *begin() const {
		return items.data();
	}
	const T *end() const {
		return items.data() + items.size();
	}

	uint32_t ItemCount() const {
		return items.size();
	}

	bool IsEmpty() const {
		for (const auto &item : items) {
			if (!item.IsEmpty()) {
				return false;
			}
		}
		return true;
	}
};
class GeometryCollection : public MultiGeometry<Geometry> {
public:
	static constexpr GeometryType TYPE = GeometryType::GEOMETRYCOLLECTION;
	GeometryCollection(Allocator &allocator, uint32_t count) : MultiGeometry<Geometry>(allocator, count) {
	}

	string ToString() const;
	operator Geometry() const;

	template <class AGG, class RESULT_TYPE>
	RESULT_TYPE Aggregate(AGG agg, RESULT_TYPE zero) const;

	uint32_t Dimension() const;
};

class MultiPoint : public MultiGeometry<Point> {
public:
	static constexpr GeometryType TYPE = GeometryType::MULTIPOINT;
	MultiPoint(Allocator &allocator, uint32_t count) : MultiGeometry<Point>(allocator, count) {
	}

	string ToString() const;
	operator Geometry() const;

	uint32_t Dimension() const {
		return 0;
	}
};

class MultiLineString : public MultiGeometry<LineString> {
public:
	static constexpr GeometryType TYPE = GeometryType::MULTILINESTRING;
	MultiLineString(Allocator &allocator, uint32_t count) : MultiGeometry<LineString>(allocator, count) {
	}

	string ToString() const;
	operator Geometry() const;

	uint32_t Dimension() const {
		return 1;
	}
};

class MultiPolygon : public MultiGeometry<Polygon> {
public:
	static constexpr GeometryType TYPE = GeometryType::MULTIPOLYGON;
	MultiPolygon(Allocator &allocator, uint32_t count) : MultiGeometry<Polygon>(allocator, count) {
	}

	string ToString() const;
	operator Geometry() const;

	uint32_t Dimension() const {
		return 2;
	}
};

class Geometry {
private:
	GeometryType type;
	union {
		Point point;
		LineString linestring;
		Polygon polygon;
		MultiPoint multipoint;
		MultiLineString multilinestring;
		MultiPolygon multipolygon;
		GeometryCollection collection;
	};

public:
	explicit Geometry(Allocator &allocator) {
		type = GeometryType::POINT;
		new (&point) Point(allocator);
	}

	explicit Geometry(const Point &point) : type(GeometryType::POINT), point(point) {
	}
	explicit Geometry(const LineString &linestring) : type(GeometryType::LINESTRING), linestring(linestring) {
	}
	explicit Geometry(const Polygon &polygon) : type(GeometryType::POLYGON), polygon(polygon) {
	}
	explicit Geometry(const MultiPoint &multipoint) : type(GeometryType::MULTIPOINT), multipoint(multipoint) {
	}
	explicit Geometry(const MultiLineString &multilinestring)
	    : type(GeometryType::MULTILINESTRING), multilinestring(multilinestring) {
	}
	explicit Geometry(const MultiPolygon &multipolygon) : type(GeometryType::MULTIPOLYGON), multipolygon(multipolygon) {
	}
	explicit Geometry(const GeometryCollection &collection)
	    : type(GeometryType::GEOMETRYCOLLECTION), collection(collection) {
	}

	explicit Geometry(Point &&point) : type(GeometryType::POINT), point(std::move(point)) {
	}
	explicit Geometry(LineString &&linestring) : type(GeometryType::LINESTRING), linestring(std::move(linestring)) {
	}
	explicit Geometry(Polygon &&polygon) : type(GeometryType::POLYGON), polygon(std::move(polygon)) {
	}
	explicit Geometry(MultiPoint &&multipoint) : type(GeometryType::MULTIPOINT), multipoint(std::move(multipoint)) {
	}
	explicit Geometry(MultiLineString &&multilinestring)
	    : type(GeometryType::MULTILINESTRING), multilinestring(std::move(multilinestring)) {
	}
	explicit Geometry(MultiPolygon &&multipolygon)
	    : type(GeometryType::MULTIPOLYGON), multipolygon(std::move(multipolygon)) {
	}
	explicit Geometry(GeometryCollection &&collection)
	    : type(GeometryType::GEOMETRYCOLLECTION), collection(std::move(collection)) {
	}

	GeometryType Type() const {
		return type;
	}

	bool IsCollection() const;
	string ToString() const;

	// Apply a functor to the contained geometry
	template <class F, class... ARGS>
	auto Dispatch(ARGS... args) const
	    -> decltype(F::Apply(std::declval<const Point &>(), std::forward<ARGS>(args)...)) {
		switch (type) {
		case GeometryType::POINT:
			return F::Apply(const_cast<const Point &>(point), std::forward<ARGS>(args)...);
		case GeometryType::LINESTRING:
			return F::Apply(const_cast<const LineString &>(linestring), std::forward<ARGS>(args)...);
		case GeometryType::POLYGON:
			return F::Apply(const_cast<const Polygon &>(polygon), std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOINT:
			return F::Apply(const_cast<const MultiPoint &>(multipoint), std::forward<ARGS>(args)...);
		case GeometryType::MULTILINESTRING:
			return F::Apply(const_cast<const MultiLineString &>(multilinestring), std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOLYGON:
			return F::Apply(const_cast<const MultiPolygon &>(multipolygon), std::forward<ARGS>(args)...);
		case GeometryType::GEOMETRYCOLLECTION:
			return F::Apply(const_cast<const GeometryCollection &>(collection), std::forward<ARGS>(args)...);
		default:
			throw NotImplementedException("Geometry::Dispatch()");
		}
	}

	bool IsEmpty() const {
		if (type == GeometryType::POINT) {
			return point.IsEmpty();
		} else if (type == GeometryType::LINESTRING) {
			return linestring.IsEmpty();
		} else if (type == GeometryType::POLYGON) {
			return polygon.IsEmpty();
		} else if (type == GeometryType::MULTIPOINT) {
			return multipoint.IsEmpty();
		} else if (type == GeometryType::MULTILINESTRING) {
			return multilinestring.IsEmpty();
		} else if (type == GeometryType::MULTIPOLYGON) {
			return multipolygon.IsEmpty();
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			return collection.IsEmpty();
		}
		return false;
	}

	uint32_t Dimension() const {
		if (type == GeometryType::POINT) {
			return point.Dimension();
		} else if (type == GeometryType::LINESTRING) {
			return linestring.Dimension();
		} else if (type == GeometryType::POLYGON) {
			return polygon.Dimension();
		} else if (type == GeometryType::MULTIPOINT) {
			return multipoint.Dimension();
		} else if (type == GeometryType::MULTILINESTRING) {
			return multilinestring.Dimension();
		} else if (type == GeometryType::MULTIPOLYGON) {
			return multipolygon.Dimension();
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			return collection.Dimension();
		}
		return 0;
	}

	// Accessor
	template <class T>
	T &As() {
		D_ASSERT(type == T::TYPE);
		if (T::TYPE == GeometryType::POINT) {
			return reinterpret_cast<T &>(point);
		} else if (T::TYPE == GeometryType::LINESTRING) {
			return reinterpret_cast<T &>(linestring);
		} else if (T::TYPE == GeometryType::POLYGON) {
			return reinterpret_cast<T &>(polygon);
		} else if (T::TYPE == GeometryType::MULTIPOINT) {
			return reinterpret_cast<T &>(multipoint);
		} else if (T::TYPE == GeometryType::MULTILINESTRING) {
			return reinterpret_cast<T &>(multilinestring);
		} else if (T::TYPE == GeometryType::MULTIPOLYGON) {
			return reinterpret_cast<T &>(multipolygon);
		} else if (T::TYPE == GeometryType::GEOMETRYCOLLECTION) {
			return reinterpret_cast<T &>(collection);
		}
	}

	template <class T>
	const T &As() const {
		D_ASSERT(type == T::TYPE);
		if (T::TYPE == GeometryType::POINT) {
			return reinterpret_cast<const T &>(point);
		} else if (T::TYPE == GeometryType::LINESTRING) {
			return reinterpret_cast<const T &>(linestring);
		} else if (T::TYPE == GeometryType::POLYGON) {
			return reinterpret_cast<const T &>(polygon);
		} else if (T::TYPE == GeometryType::MULTIPOINT) {
			return reinterpret_cast<const T &>(multipoint);
		} else if (T::TYPE == GeometryType::MULTILINESTRING) {
			return reinterpret_cast<const T &>(multilinestring);
		} else if (T::TYPE == GeometryType::MULTIPOLYGON) {
			return reinterpret_cast<const T &>(multipolygon);
		} else if (T::TYPE == GeometryType::GEOMETRYCOLLECTION) {
			return reinterpret_cast<const T &>(collection);
		}
	}

	// Copy constructor and assignment operator

	Geometry(const Geometry &other) : type(other.type) {
		if (type == GeometryType::POINT) {
			new (&point) Point(other.point);
		} else if (type == GeometryType::LINESTRING) {
			new (&linestring) LineString(other.linestring);
		} else if (type == GeometryType::POLYGON) {
			new (&polygon) Polygon(other.polygon);
		} else if (type == GeometryType::MULTIPOINT) {
			new (&multipoint) MultiPoint(other.multipoint);
		} else if (type == GeometryType::MULTILINESTRING) {
			new (&multilinestring) MultiLineString(other.multilinestring);
		} else if (type == GeometryType::MULTIPOLYGON) {
			new (&multipolygon) MultiPolygon(other.multipolygon);
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			new (&collection) GeometryCollection(other.collection);
		}
	}

	Geometry &operator=(const Geometry &other) {
		if (this == &other) {
			return *this;
		}
		if (type == GeometryType::POINT) {
			point.~Point();
		} else if (type == GeometryType::LINESTRING) {
			linestring.~LineString();
		} else if (type == GeometryType::POLYGON) {
			polygon.~Polygon();
		} else if (type == GeometryType::MULTIPOINT) {
			multipoint.~MultiPoint();
		} else if (type == GeometryType::MULTILINESTRING) {
			multilinestring.~MultiLineString();
		} else if (type == GeometryType::MULTIPOLYGON) {
			multipolygon.~MultiPolygon();
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			collection.~GeometryCollection();
		}
		type = other.type;
		if (type == GeometryType::POINT) {
			new (&point) Point(other.point);
		} else if (type == GeometryType::LINESTRING) {
			new (&linestring) LineString(other.linestring);
		} else if (type == GeometryType::POLYGON) {
			new (&polygon) Polygon(other.polygon);
		} else if (type == GeometryType::MULTIPOINT) {
			new (&multipoint) MultiPoint(other.multipoint);
		} else if (type == GeometryType::MULTILINESTRING) {
			new (&multilinestring) MultiLineString(other.multilinestring);
		} else if (type == GeometryType::MULTIPOLYGON) {
			new (&multipolygon) MultiPolygon(other.multipolygon);
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			new (&collection) GeometryCollection(other.collection);
		}
		return *this;
	}

	// Move constructor and assignment operator

	Geometry(Geometry &&other) noexcept : type(other.type) {
		if (type == GeometryType::POINT) {
			new (&point) Point(std::move(other.point));
		} else if (type == GeometryType::LINESTRING) {
			new (&linestring) LineString(std::move(other.linestring));
		} else if (type == GeometryType::POLYGON) {
			new (&polygon) Polygon(std::move(other.polygon));
		} else if (type == GeometryType::MULTIPOINT) {
			new (&multipoint) MultiPoint(std::move(other.multipoint));
		} else if (type == GeometryType::MULTILINESTRING) {
			new (&multilinestring) MultiLineString(std::move(other.multilinestring));
		} else if (type == GeometryType::MULTIPOLYGON) {
			new (&multipolygon) MultiPolygon(std::move(other.multipolygon));
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			new (&collection) GeometryCollection(std::move(other.collection));
		}
	}

	Geometry &operator=(Geometry &&other) noexcept {
		if (this == &other) {
			return *this;
		}
		if (type == GeometryType::POINT) {
			point.~Point();
		} else if (type == GeometryType::LINESTRING) {
			linestring.~LineString();
		} else if (type == GeometryType::POLYGON) {
			polygon.~Polygon();
		} else if (type == GeometryType::MULTIPOINT) {
			multipoint.~MultiPoint();
		} else if (type == GeometryType::MULTILINESTRING) {
			multilinestring.~MultiLineString();
		} else if (type == GeometryType::MULTIPOLYGON) {
			multipolygon.~MultiPolygon();
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			collection.~GeometryCollection();
		}
		type = other.type;
		if (type == GeometryType::POINT) {
			new (&point) Point(std::move(other.point));
		} else if (type == GeometryType::LINESTRING) {
			new (&linestring) LineString(std::move(other.linestring));
		} else if (type == GeometryType::POLYGON) {
			new (&polygon) Polygon(std::move(other.polygon));
		} else if (type == GeometryType::MULTIPOINT) {
			new (&multipoint) MultiPoint(std::move(other.multipoint));
		} else if (type == GeometryType::MULTILINESTRING) {
			new (&multilinestring) MultiLineString(std::move(other.multilinestring));
		} else if (type == GeometryType::MULTIPOLYGON) {
			new (&multipolygon) MultiPolygon(std::move(other.multipolygon));
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			new (&collection) GeometryCollection(std::move(other.collection));
		}
		return *this;
	}

	// Destructor
	~Geometry() {
		if (type == GeometryType::POINT) {
			point.~Point();
		} else if (type == GeometryType::LINESTRING) {
			linestring.~LineString();
		} else if (type == GeometryType::POLYGON) {
			polygon.~Polygon();
		} else if (type == GeometryType::MULTIPOINT) {
			multipoint.~MultiPoint();
		} else if (type == GeometryType::MULTILINESTRING) {
			multilinestring.~MultiLineString();
		} else if (type == GeometryType::MULTIPOLYGON) {
			multipolygon.~MultiPolygon();
		} else if (type == GeometryType::GEOMETRYCOLLECTION) {
			collection.~GeometryCollection();
		}
	}
};

template <class AGG, class RESULT_TYPE>
RESULT_TYPE GeometryCollection::Aggregate(AGG agg, RESULT_TYPE zero) const {
	RESULT_TYPE result = zero;
	for (auto &item : Items()) {
		if (item.Type() == GeometryType::GEOMETRYCOLLECTION) {
			result = item.As<GeometryCollection>().Aggregate(agg, result);
		} else {
			result = agg(item, result);
		}
	}
	return result;
}

} // namespace core

} // namespace spatial