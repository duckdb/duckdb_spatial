#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Geometry Objects
//------------------------------------------------------------------------------

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
	double minz = std::numeric_limits<double>::max();
	double maxz = std::numeric_limits<double>::lowest();
	double minm = std::numeric_limits<double>::max();
	double maxm = std::numeric_limits<double>::lowest();

	bool Intersects(const BoundingBox &other) const {
		return !(minx > other.maxx || maxx < other.minx || miny > other.maxy || maxy < other.miny);
	}
};

class Geometry;

class Point {
private:
	VertexArray vertices;

public:
	static constexpr GeometryType TYPE = GeometryType::POINT;

	explicit Point(ArenaAllocator &alloc, double x, double y) : vertices(VertexArray::Create(alloc, 1, false, false)) {
		vertices.Set(0, x, y);
	}
	explicit Point(ArenaAllocator &alloc, double x, double y, double z)
	    : vertices(VertexArray::Create(alloc, 1, true, false)) {
		vertices.Set(0, x, y, z);
	}

	explicit Point(bool has_z, bool has_m) : vertices(VertexArray::Empty(has_z, has_m)) {
	}
	explicit Point(VertexArray vertices) : vertices(vertices) {
	}

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

	Point DeepCopy(ArenaAllocator &alloc) const {
		Point copy(*this);
		copy.vertices.MakeOwning(alloc);
		return copy;
	}
};

class LineString {
private:
	VertexArray vertices;

public:
	static constexpr GeometryType TYPE = GeometryType::LINESTRING;
	LineString(bool has_z, bool has_m) : vertices(VertexArray::Empty(has_z, has_m)) {
	}
	LineString(ArenaAllocator &arena, uint32_t size, bool has_z, bool has_m)
	    : vertices(VertexArray::Create(arena, size, has_z, has_m)) {
	}
	explicit LineString(VertexArray vertices) : vertices(vertices) {
	}

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

	// Make this linestring owning by copying all the vertices
	LineString DeepCopy(ArenaAllocator &alloc) const {
		LineString copy(*this);
		copy.vertices.MakeOwning(alloc);
		return copy;
	}
};

class Polygon {
private:
	uint32_t ring_count;
	VertexArray *rings;

public:
	static constexpr GeometryType TYPE = GeometryType::POLYGON;
	explicit Polygon(bool has_z, bool has_m) : ring_count(0), rings(nullptr) {
	}
	explicit Polygon(ArenaAllocator &alloc, uint32_t ring_count_p, bool has_z, bool has_m)
	    : ring_count(ring_count_p),
	      rings(reinterpret_cast<VertexArray *>(alloc.AllocateAligned(ring_count * sizeof(VertexArray)))) {
		for (uint32_t i = 0; i < ring_count; i++) {
			new (&rings[i]) VertexArray(VertexArray::Empty(has_z, has_m));
		}
	}
	explicit Polygon(ArenaAllocator &alloc, uint32_t ring_count_p, uint32_t *ring_sizes_p, bool has_z, bool has_m)
	    : ring_count(ring_count_p),
	      rings(reinterpret_cast<VertexArray *>(alloc.AllocateAligned(ring_count * sizeof(VertexArray)))) {
		for (uint32_t i = 0; i < ring_count; i++) {
			new (&rings[i]) VertexArray(VertexArray::Create(alloc, ring_sizes_p[i], has_z, has_m));
		}
	}

	// Make this polygon owning by copying all the vertices
	Polygon DeepCopy(ArenaAllocator &alloc) const {
		Polygon copy(*this);
		copy.rings = reinterpret_cast<VertexArray *>(alloc.AllocateAligned(copy.RingCount() * sizeof(VertexArray)));
		for (uint32_t i = 0; i < copy.RingCount(); i++) {
			new (&copy.rings[i]) VertexArray(VertexArray::Copy(alloc, rings[i]));
		}
		return copy;
	}

	string ToString() const;

	// Collection Methods
	VertexArray &operator[](uint32_t index) {
		D_ASSERT(index < ring_count);
		return rings[index];
	}
	const VertexArray &operator[](uint32_t index) const {
		D_ASSERT(index < ring_count);
		return rings[index];
	}
	VertexArray *begin() {
		return rings;
	}
	VertexArray *end() {
		return rings + ring_count;
	}
	const VertexArray *begin() const {
		return rings;
	}
	const VertexArray *end() const {
		return rings + ring_count;
	}

	uint32_t RingCount() const {
		return ring_count;
	}

	bool IsEmpty() const {
		for (const auto &ring : *this) {
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
private:
	uint32_t item_count;
	T *items;

public:
	MultiGeometry(bool has_z, bool has_m) : item_count(0), items(nullptr) {
	}
	MultiGeometry(ArenaAllocator &allocator, uint32_t item_count_p, bool has_z, bool has_m)
	    : item_count(item_count_p), items(reinterpret_cast<T *>(allocator.AllocateAligned(item_count * sizeof(T)))) {
		for (uint32_t i = 0; i < item_count; i++) {
			new (&items[i]) T(has_z, has_m);
		}
	}

	T &operator[](uint32_t index) {
		D_ASSERT(index < item_count);
		return items[index];
	}
	const T &operator[](uint32_t index) const {
		D_ASSERT(index < item_count);
		return items[index];
	}
	T *begin() {
		return items;
	}
	T *end() {
		return items + item_count;
	}
	const T *begin() const {
		return items;
	}
	const T *end() const {
		return items + item_count;
	}

	uint32_t ItemCount() const {
		return item_count;
	}

	bool IsEmpty() const {
		for (const auto &item : *this) {
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
	GeometryCollection(bool has_z, bool has_m) : MultiGeometry<Geometry>(has_z, has_m) {
	}
	GeometryCollection(ArenaAllocator &allocator, uint32_t count, bool has_z, bool has_m)
	    : MultiGeometry<Geometry>(allocator, count, has_z, has_m) {
	}

	string ToString() const;
	template <class AGG, class RESULT_TYPE>
	RESULT_TYPE Aggregate(AGG agg, RESULT_TYPE zero) const;
	uint32_t Dimension() const;
	GeometryCollection DeepCopy(ArenaAllocator &alloc) const;
};

class MultiPoint : public MultiGeometry<Point> {
public:
	static constexpr GeometryType TYPE = GeometryType::MULTIPOINT;
	MultiPoint(bool has_z, bool has_m) : MultiGeometry<Point>(has_z, has_m) {
	}
	MultiPoint(ArenaAllocator &allocator, uint32_t count, bool has_z, bool has_m)
	    : MultiGeometry<Point>(allocator, count, has_z, has_m) {
	}

	string ToString() const;
	uint32_t Dimension() const {
		return 0;
	}
	MultiPoint DeepCopy(ArenaAllocator &alloc) const {
		MultiPoint copy(*this);
		for (auto &item : copy) {
			item = item.DeepCopy(alloc);
		}
		return copy;
	}
};

class MultiLineString : public MultiGeometry<LineString> {
public:
	static constexpr GeometryType TYPE = GeometryType::MULTILINESTRING;
	MultiLineString(bool has_z, bool has_m) : MultiGeometry<LineString>(has_z, has_m) {
	}
	MultiLineString(ArenaAllocator &allocator, uint32_t count, bool has_z, bool has_m)
	    : MultiGeometry<LineString>(allocator, count, has_z, has_m) {
	}

	string ToString() const;

	uint32_t Dimension() const {
		return 1;
	}

	MultiLineString DeepCopy(ArenaAllocator &alloc) const {
		MultiLineString copy(*this);
		for (auto &item : copy) {
			item = item.DeepCopy(alloc);
		}
		return copy;
	}
};

class MultiPolygon : public MultiGeometry<Polygon> {
public:
	static constexpr GeometryType TYPE = GeometryType::MULTIPOLYGON;
	MultiPolygon(bool has_z, bool has_m) : MultiGeometry<Polygon>(has_z, has_m) {
	}
	MultiPolygon(ArenaAllocator &allocator, uint32_t count, bool has_z, bool has_m)
	    : MultiGeometry<Polygon>(allocator, count, has_z, has_m) {
	}

	string ToString() const;

	uint32_t Dimension() const {
		return 2;
	}

	MultiPolygon DeepCopy(ArenaAllocator &alloc) const {
		MultiPolygon copy(*this);
		for (auto &item : copy) {
			item = item.DeepCopy(alloc);
		}
		return copy;
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
	explicit Geometry(bool has_z, bool has_m) {
		type = GeometryType::POINT;
		new (&point) Point(has_z, has_m);
	}

	Geometry(const Point &point) : type(GeometryType::POINT), point(point) {
	}
	Geometry(const LineString &linestring) : type(GeometryType::LINESTRING), linestring(linestring) {
	}
	Geometry(const Polygon &polygon) : type(GeometryType::POLYGON), polygon(polygon) {
	}
	Geometry(const MultiPoint &multipoint) : type(GeometryType::MULTIPOINT), multipoint(multipoint) {
	}
	Geometry(const MultiLineString &multilinestring)
	    : type(GeometryType::MULTILINESTRING), multilinestring(multilinestring) {
	}
	Geometry(const MultiPolygon &multipolygon) : type(GeometryType::MULTIPOLYGON), multipolygon(multipolygon) {
	}
	Geometry(const GeometryCollection &collection) : type(GeometryType::GEOMETRYCOLLECTION), collection(collection) {
	}
	Geometry(Point &&point) : type(GeometryType::POINT), point(std::move(point)) {
	}
	Geometry(LineString &&linestring) : type(GeometryType::LINESTRING), linestring(std::move(linestring)) {
	}
	Geometry(Polygon &&polygon) : type(GeometryType::POLYGON), polygon(std::move(polygon)) {
	}
	Geometry(MultiPoint &&multipoint) : type(GeometryType::MULTIPOINT), multipoint(std::move(multipoint)) {
	}
	Geometry(MultiLineString &&multilinestring)
	    : type(GeometryType::MULTILINESTRING), multilinestring(std::move(multilinestring)) {
	}
	Geometry(MultiPolygon &&multipolygon) : type(GeometryType::MULTIPOLYGON), multipolygon(std::move(multipolygon)) {
	}
	Geometry(GeometryCollection &&collection)
	    : type(GeometryType::GEOMETRYCOLLECTION), collection(std::move(collection)) {
	}

	GeometryType Type() const {
		return type;
	}

	bool IsCollection() const;
	string ToString() const;

	// Apply a functor to the contained geometry
	template <class F, class... ARGS>
	auto Dispatch(ARGS &&...args) const
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

	// Apply a functor to the contained geometry
	template <class F, class... ARGS>
	auto Dispatch(ARGS &&...args) -> decltype(F::Apply(std::declval<Point &>(), std::forward<ARGS>(args)...)) {
		switch (type) {
		case GeometryType::POINT:
			return F::Apply(static_cast<Point &>(point), std::forward<ARGS>(args)...);
		case GeometryType::LINESTRING:
			return F::Apply(static_cast<LineString &>(linestring), std::forward<ARGS>(args)...);
		case GeometryType::POLYGON:
			return F::Apply(static_cast<Polygon &>(polygon), std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOINT:
			return F::Apply(static_cast<MultiPoint &>(multipoint), std::forward<ARGS>(args)...);
		case GeometryType::MULTILINESTRING:
			return F::Apply(static_cast<MultiLineString &>(multilinestring), std::forward<ARGS>(args)...);
		case GeometryType::MULTIPOLYGON:
			return F::Apply(static_cast<MultiPolygon &>(multipolygon), std::forward<ARGS>(args)...);
		case GeometryType::GEOMETRYCOLLECTION:
			return F::Apply(static_cast<GeometryCollection &>(collection), std::forward<ARGS>(args)...);
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

	Geometry DeepCopy(ArenaAllocator &alloc) const {
		switch (type) {
		case GeometryType::POINT:
			return Geometry(point.DeepCopy(alloc));
		case GeometryType::LINESTRING:
			return Geometry(linestring.DeepCopy(alloc));
		case GeometryType::POLYGON:
			return Geometry(polygon.DeepCopy(alloc));
		case GeometryType::MULTIPOINT:
			return Geometry(multipoint.DeepCopy(alloc));
		case GeometryType::MULTILINESTRING:
			return Geometry(multilinestring.DeepCopy(alloc));
		case GeometryType::MULTIPOLYGON:
			return Geometry(multipolygon.DeepCopy(alloc));
		case GeometryType::GEOMETRYCOLLECTION:
			return Geometry(collection.DeepCopy(alloc));
		default:
			throw NotImplementedException("Geometry::DeepCopy()");
		}
	}

	Geometry &SetVertexType(ArenaAllocator &alloc, bool has_z, bool has_m) {
		switch (type) {
		case GeometryType::POINT:
			point.Vertices().SetVertexType(alloc, has_z, has_m);
			break;
		case GeometryType::LINESTRING:
			linestring.Vertices().SetVertexType(alloc, has_z, has_m);
			break;
		case GeometryType::POLYGON:
			for (auto &ring : polygon) {
				ring.SetVertexType(alloc, has_z, has_m);
			}
			break;
		case GeometryType::MULTIPOINT:
			for (auto &point : multipoint) {
				point.Vertices().SetVertexType(alloc, has_z, has_m);
			}
			break;
		case GeometryType::MULTILINESTRING:
			for (auto &line : multilinestring) {
				line.Vertices().SetVertexType(alloc, has_z, has_m);
			}
			break;
		case GeometryType::MULTIPOLYGON:
			for (auto &polygon : multipolygon) {
				for (auto &ring : polygon) {
					ring.SetVertexType(alloc, has_z, has_m);
				}
			}
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			for (auto &geom : collection) {
				geom.SetVertexType(alloc, has_z, has_m);
			}
			break;
		default:
			break;
		}
		return *this;
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
	for (auto &item : *this) {
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