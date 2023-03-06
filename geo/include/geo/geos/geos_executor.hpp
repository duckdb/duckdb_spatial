#pragma once

#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

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

template<class T>
unique_ptr<T, GeosDeleter<T>> make_unique_geos(GEOSContextHandle_t ctx, T *ptr) {
	return unique_ptr<T, GeosDeleter<T>>(ptr, GeosDeleter<T>{ctx});
}

struct GeometryPtr {
	GEOSContextHandle_t ctx;
	GEOSGeometry *ptr;

	GeometryPtr(GEOSContextHandle_t ctx, GEOSGeometry *ptr) : ctx(ctx), ptr(ptr) {}

	GeometryPtr( const GeometryPtr& ) = delete; // non construction-copyable
	GeometryPtr& operator=( const GeometryPtr& ) = delete; // non copyable

	GeometryPtr( GeometryPtr&& other )  noexcept : ctx(other.ctx), ptr(other.ptr)  {
		std::swap(other.ptr, ptr);
		ctx = other.ctx;
	}

	GeometryPtr& operator=( GeometryPtr&& other ) noexcept {
		std::swap(other.ptr, ptr);
		ctx = other.ctx;
		return *this;
	}


	~GeometryPtr() {
		if (ptr) {
			GEOSGeom_destroy_r(ctx, ptr);
		}
	}

	double Area() const {
		double area = 0;
		return GEOSArea_r(ctx, ptr, &area) ? area : 0;
	}

	double Length() const {
		double length = 0;
		return GEOSLength_r(ctx, ptr, &length) ? length : 0;
	}

	bool IsEmpty() const {
		return GEOSisEmpty_r(ctx, ptr);
	}

	bool IsSimple() const {
		return GEOSisSimple_r(ctx, ptr);
	}

	bool IsValid() const {
		return GEOSisValid_r(ctx, ptr);
	}

	bool IsRing() const {
		return GEOSisRing_r(ctx, ptr);
	}

	bool IsClosed() const {
		return GEOSisClosed_r(ctx, ptr);
	}

	void Simplify(double tolerance) {
		auto new_geom = GEOSSimplify_r(ctx, ptr, tolerance);
		if (new_geom) {
			GEOSGeom_destroy_r(ctx, ptr);
			ptr = new_geom;
		}
	}

	void SimplifyPreserveTopology(double tolerance) {
		auto new_geom = GEOSTopologyPreserveSimplify_r(ctx, ptr, tolerance);
		if (new_geom) {
			GEOSGeom_destroy_r(ctx, ptr);
			ptr = new_geom;
		}
	}

	double GetX() const {
		double x = 0;
		return GEOSGeomGetX_r(ctx, ptr, &x) ? x : throw InvalidInputException("Could not get X coordinate");
	}

	double GetY() const {
		double y = 0;
		return GEOSGeomGetY_r(ctx, ptr, &y) ? y : throw InvalidInputException("Could not get Y coordinate");
	}

	bool Overlaps(const GeometryPtr &other) const {
		auto res = GEOSOverlaps_r(ctx, ptr, other.ptr);
		switch (res) {
		case 2:
			throw InvalidInputException("GEOS error: overlaps");
		case 1:
			return true;
		default:
			return false;
		}
	}
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
		return {ctx, geom};
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
		auto wkb = GEOSWKBWriter_write_r(ctx, writer, geom.ptr, &size);
		if (!wkb) {
			throw InvalidInputException("Could not write WKB");
		}
		stream.write((const char *)wkb, (long)size);
		GEOSFree_r(ctx, wkb);
	}

	~WKBWriter() {
		GEOSWKBWriter_destroy_r(ctx, writer);
	}
};

struct GeosContextWrapper {
	GEOSContextHandle_t ctx;
	GeosContextWrapper() {
		ctx = GEOS_init_r();

		// TODO: We should throw an exception here
		/*
		GEOSContext_setErrorHandler_r(ctx, [](const char *fmt, ...) {
		    va_list ap;
		    va_start(ap, fmt);
		    vfprintf(stderr, fmt, ap);
		    va_end(ap);
		});
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
};

struct GEOSExecutor {
	template <class FUNC>
	static inline void ExecuteUnary(Vector &input, Vector &result, idx_t count, FUNC func) {

		// TODO: Error handler in GeosContextWrapper
		auto ctx = GeosContextWrapper();
		auto reader = ctx.CreateWKBReader();
		auto writer = ctx.CreateWKBWriter();

		UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input_wkb_blob) {
			auto ptr = (const unsigned char *)(input_wkb_blob.GetDataUnsafe());
			auto size = input_wkb_blob.GetSize();

			auto geom = reader.Read(ptr, size);

			auto result_geom = func(geom);
			std::stringstream buf;
			writer.Write(result_geom, buf);

			return StringVector::AddStringOrBlob(result, buf.str());
		});
	}

	// FUNC<T>(GEOMETRY) -> T
	template <class RESULT_TYPE, class FUNC>
	static inline void ExecuteUnaryToScalar(Vector &input, Vector &result, idx_t count, FUNC func) {

		auto ctx = GeosContextWrapper();
		auto reader = ctx.CreateWKBReader();

		UnaryExecutor::Execute<string_t, RESULT_TYPE>(input, result, count, [&](string_t input_wkb_blob) {
			auto ptr = (const unsigned char *)(input_wkb_blob.GetDataUnsafe());
			auto size = input_wkb_blob.GetSize();
			auto geom = reader.Read(ptr, size);
			return func(geom);
		});
	}

	// FUNC(GEOMETRY, GEOMETRY) -> GEOMETRY
	template <class FUNC>
	static inline void ExecuteBinary(Vector &left, Vector &right, Vector &result, idx_t count, FUNC func) {

		auto ctx = GeosContextWrapper();
		auto reader = ctx.CreateWKBReader();
		auto writer = ctx.CreateWKBWriter();

		BinaryExecutor::Execute<string_t, string_t, string_t>(
		    left, right, result, count, [&](string_t left_wkb, string_t right_wkb) {
			    auto left_ptr = (const unsigned char *)(left_wkb.GetDataUnsafe());
			    auto left_size = left_wkb.GetSize();
			    auto left_geom = reader.Read(left_ptr, left_size);

			    auto right_ptr = (const unsigned char *)(right_wkb.GetDataUnsafe());
			    auto right_size = right_wkb.GetSize();
			    auto right_geom = reader.Read(right_ptr, right_size);

			    auto result_geom = func(left_geom, right_geom);
			    std::stringstream buf;
			    writer.Write(*result_geom, buf);
			    return StringVector::AddStringOrBlob(result, buf.str());
		    });
	}

	template <class RESULT_TYPE, class FUNC>
	static inline void ExecuteBinaryToScalar(Vector &left, Vector &right, Vector &result, idx_t count, FUNC func) {

		auto ctx = GeosContextWrapper();
		auto reader = ctx.CreateWKBReader();

		BinaryExecutor::Execute<string_t, string_t, RESULT_TYPE>(
		    left, right, result, count, [&](string_t left_wkb, string_t right_wkb) {
			    auto left_ptr = (const unsigned char *)(left_wkb.GetDataUnsafe());
			    auto left_size = left_wkb.GetSize();
			    auto left_geom = reader.Read(left_ptr, left_size);

			    auto right_ptr = (const unsigned char *)(right_wkb.GetDataUnsafe());
			    auto right_size = right_wkb.GetSize();
			    auto right_geom = reader.Read(right_ptr, right_size);

			    return func(left_geom, right_geom);
		    });
	}
};

}

}