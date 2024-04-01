#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "geos_c.h"

namespace spatial {

namespace geos {

using namespace core;

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

template <>
struct GeosDeleter<const GEOSPreparedGeometry> {
	GEOSContextHandle_t ctx;
	void operator()(const GEOSPreparedGeometry *ptr) const {
		GEOSPreparedGeom_destroy_r(ctx, ptr);
	}
};

template <class T>
unique_ptr<T, GeosDeleter<T>> make_uniq_geos(GEOSContextHandle_t ctx, T *ptr) {
	return unique_ptr<T, GeosDeleter<T>>(ptr, GeosDeleter<T> {ctx});
}

using GeometryPtr = unique_ptr<GEOSGeometry, GeosDeleter<GEOSGeometry>>;

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
		return make_uniq_geos(ctx, geom);
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

	~WKTReader() {
		GEOSWKTReader_destroy_r(ctx, reader);
	}

	GeometryPtr Read(string_t &wkt) const {
		auto str = wkt.GetString();
		auto geom = GEOSWKTReader_read_r(ctx, reader, str.c_str());
		if (!geom) {
			return nullptr;
		}
		return make_uniq_geos(ctx, geom);
	}
};

struct WKTWriter {
	GEOSContextHandle_t ctx;
	GEOSWKTWriter_t *writer;

	explicit WKTWriter(GEOSContextHandle_t ctx) : ctx(ctx) {
		writer = GEOSWKTWriter_create_r(ctx);
	}

	~WKTWriter() {
		GEOSWKTWriter_destroy_r(ctx, writer);
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
		GEOSContext_setErrorMessageHandler_r(ctx, ErrorHandler, (void *)nullptr);
	}
	~GeosContextWrapper() {
		GEOS_finish_r(ctx);
	}

	static void ErrorHandler(const char *message, void *userdata) {
		throw InvalidInputException(message);
	}

	inline const GEOSContextHandle_t &GetCtx() {
		return ctx;
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

	unique_ptr<GEOSGeometry, GeosDeleter<GEOSGeometry>> Deserialize(const geometry_t &blob);
	geometry_t Serialize(Vector &result, const unique_ptr<GEOSGeometry, GeosDeleter<GEOSGeometry>> &geom);
};

GEOSGeometry *DeserializeGEOSGeometry(const geometry_t &blob, GEOSContextHandle_t ctx);
geometry_t SerializeGEOSGeometry(Vector &result, const GEOSGeometry *geom, GEOSContextHandle_t ctx);

} // namespace geos

} // namespace spatial