#pragma once
#include "geo/common.hpp"

#include "geos_c.h"

namespace geo {

namespace geos {

template <class T>
struct GeosDeleter {
	GEOSContextHandle_t ctx;
	void operator()(T *ptr) const = delete;
};

template <>
struct GeosDeleter<GEOSGeometry> {
	GEOSContextHandle_t ctx;
	void operator()(GEOSGeometry *ptr) const {
		GEOSGeom_destroy_r(ctx, ptr);
	}
};

template <>
struct GeosDeleter<GEOSPreparedGeometry> {
	GEOSContextHandle_t ctx;
	void operator()(GEOSPreparedGeometry *ptr) const {
		GEOSPreparedGeom_destroy_r(ctx, ptr);
	}
};

template <>
struct GeosDeleter<GEOSWKBReader_t> {
	GEOSContextHandle_t ctx;
	void operator()(GEOSWKBReader_t *ptr) const {
		GEOSWKBReader_destroy_r(ctx, ptr);
	}
};

template <class T>
unique_ptr<T, GeosDeleter<T>> make_unique_geos(GEOSContextHandle_t ctx, T *ptr) {
	return unique_ptr<T, GeosDeleter<T>>(ptr, GeosDeleter<T> {ctx});
}

struct GeometryPtr {
private:
	GEOSContextHandle_t ctx;
	GEOSGeometry *ptr;

public:
	explicit GeometryPtr(GEOSContextHandle_t ctx, GEOSGeometry *ptr) : ctx(ctx), ptr(ptr) {
	}
	GeometryPtr(const GeometryPtr &) = delete;
	GeometryPtr &operator=(const GeometryPtr &) = delete;
	GeometryPtr(GeometryPtr &&other) noexcept : ctx(other.ctx), ptr(other.ptr) {
		std::swap(other.ptr, ptr);
		ctx = other.ctx;
	}

	GeometryPtr &operator=(GeometryPtr &&other) noexcept {
		std::swap(other.ptr, ptr);
		ctx = other.ctx;
		return *this;
	}

	~GeometryPtr() {
		if (ptr) {
			GEOSGeom_destroy_r(ctx, ptr);
		}
	}

	inline GEOSGeometry *get() const {
		return ptr;
	}

	// Accessors
	double Area() const;
	double Length() const;
	double GetX();
	double GetY();
	bool IsEmpty() const;
	bool IsSimple() const;
	bool IsValid() const;
	bool IsRing() const;
	bool IsClosed() const;

	// Constructs
	GeometryPtr Simplify(double tolerance) const;
	GeometryPtr SimplifyPreserveTopology(double tolerance) const;
	GeometryPtr Buffer(double distance, int n_quadrant_segments) const;
	GeometryPtr Boundary() const;
	GeometryPtr Centroid() const;
	GeometryPtr ConvexHull() const;
	GeometryPtr Envelope() const;
	GeometryPtr Intersection(const GeometryPtr &other) const;

	// Mutations
	void Normalize() const;

	// Predicates
	bool Contains(const GeometryPtr &other) const;
	bool Covers(const GeometryPtr &other) const;
	bool CoveredBy(const GeometryPtr &other) const;
	bool Crosses(const GeometryPtr &other) const;
	bool Disjoint(const GeometryPtr &other) const;
	bool Equals(const GeometryPtr &other) const;
	bool Intersects(const GeometryPtr &other) const;
	bool Overlaps(const GeometryPtr &other) const;
	bool Touches(const GeometryPtr &other) const;
	bool Within(const GeometryPtr &other) const;
};

struct WKBReader {
	GEOSContextHandle_t ctx;
	GEOSWKBReader_t *reader;

	explicit WKBReader(GEOSContextHandle_t ctx) : ctx(ctx) {
		reader = GEOSWKBReader_create_r(ctx);
	}

	GeometryPtr Read(const unsigned char *wkb, size_t size) const {
		auto geom = GEOSWKBReader_read_r(ctx, reader, wkb, size);
		if (!geom) {
			throw InvalidInputException("Could not read WKB");
		}
		return GeometryPtr(ctx, geom);
	}

	GeometryPtr Read(string_t &wkb) const {
		return Read((const unsigned char *)wkb.GetDataUnsafe(), wkb.GetSize());
	}

	~WKBReader() {
		GEOSWKBReader_destroy_r(ctx, reader);
	}
};

struct WKBWriter {
	GEOSContextHandle_t ctx;
	GEOSWKBWriter_t *writer;

	explicit WKBWriter(GEOSContextHandle_t ctx) : ctx(ctx) {
		writer = GEOSWKBWriter_create_r(ctx);
	}

	void Write(const GeometryPtr &geom, std::ostream &stream) const {
		size_t size = 0;
		auto wkb = GEOSWKBWriter_write_r(ctx, writer, geom.get(), &size);
		if (!wkb) {
			throw InvalidInputException("Could not write WKB");
		}
		stream.write((const char *)wkb, (long)size);
		GEOSFree_r(ctx, wkb);
	}

	string_t Write(const GeometryPtr &geom, Vector &vec) const {
		std::stringstream buf;
		Write(geom, buf);
		return StringVector::AddStringOrBlob(vec, buf.str());
	}

	~WKBWriter() {
		GEOSWKBWriter_destroy_r(ctx, writer);
	}
};

struct WKTReader {
	GEOSContextHandle_t ctx;
	GEOSWKTReader_t *reader;

	explicit WKTReader(GEOSContextHandle_t ctx) : ctx(ctx) {
		reader = GEOSWKTReader_create_r(ctx);
	}

	GeometryPtr Read(string_t &wkt) const {
		auto str = wkt.GetString();
		auto geom = GEOSWKTReader_read_r(ctx, reader, str.c_str());
		if (!geom) {
			throw InvalidInputException("Could not read WKT");
		}
		return GeometryPtr(ctx, geom);
	}
};

struct WKTWriter {
	GEOSContextHandle_t ctx;
	GEOSWKTWriter_t *writer;

	explicit WKTWriter(GEOSContextHandle_t ctx) : ctx(ctx) {
		writer = GEOSWKTWriter_create_r(ctx);
	}

	void SetTrim(bool trim) const {
		GEOSWKTWriter_setTrim_r(ctx, writer, trim ? 1 : 0);
	}

	void Write(const GeometryPtr &geom, std::ostream &stream) const {
		auto wkt = GEOSWKTWriter_write_r(ctx, writer, geom.get());
		if (!wkt) {
			throw InvalidInputException("Could not write WKT");
		}
		stream << wkt;
		GEOSFree_r(ctx, wkt);
	}

	string_t Write(const GeometryPtr &geom, Vector &vec) const {
		auto wkt = GEOSWKTWriter_write_r(ctx, writer, geom.get());
		if (!wkt) {
			throw InvalidInputException("Could not write WKT");
		}
		auto str = StringVector::AddStringOrBlob(vec, wkt);
		GEOSFree_r(ctx, wkt);
		return str;
	}
};

struct GeosContextWrapper {
private:
	GEOSContextHandle_t ctx;

public:
	GeosContextWrapper() {
		ctx = GEOS_init_r();
		// TODO: Set handlers
		/*
		GEOSContext_setNoticeHandler_r(ctx, NoticeHandler);
		GEOSContext_setErrorHandler_r(ctx, ErrorHandler);
		GEOSContext_setNoticeMessageHandler_r(ctx, ExceptionHandler, (void*)nullptr);
		GEOSContext_setErrorMessageHandler_r(ctx, ExceptionHandler, (void*)nullptr);
		 */
	}
	~GeosContextWrapper() {
		GEOS_finish_r(ctx);
	}

	WKBReader CreateWKBReader() const {
		return WKBReader(ctx);
	}

	WKBWriter CreateWKBWriter() const {
		return WKBWriter(ctx);
	}

	WKTWriter CreateWKTWriter() const {
		return WKTWriter(ctx);
	}

	WKTReader CreateWKTReader() const {
		return WKTReader(ctx);
	}
};

} // namespace geos

} // namespace geo