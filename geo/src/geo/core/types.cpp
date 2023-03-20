
#include "geo/core/types.hpp"

#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "geo/common.hpp"
#include "geo/core/geometry/geometry_factory.hpp"

namespace geo {

namespace core {

LogicalType GeoTypes::POINT_2D = LogicalType::STRUCT({
    {"x", LogicalType::DOUBLE},
    {"y", LogicalType::DOUBLE}
});

LogicalType GeoTypes::POINT_3D = LogicalType::STRUCT({
    {"x", LogicalType::DOUBLE},
    {"y", LogicalType::DOUBLE},
    {"z", LogicalType::DOUBLE}
});

LogicalType GeoTypes::POINT_4D = LogicalType::STRUCT({
    {"x", LogicalType::DOUBLE},
    {"y", LogicalType::DOUBLE},
    {"z", LogicalType::DOUBLE},
    {"m", LogicalType::DOUBLE}
});

LogicalType GeoTypes::BOX_2D = LogicalType::STRUCT({
    {"min_x", LogicalType::DOUBLE},
	{"min_y", LogicalType::DOUBLE},
	{"max_x", LogicalType::DOUBLE},
	{"max_y", LogicalType::DOUBLE}
});

LogicalType GeoTypes::LINESTRING_2D = LogicalType::LIST(GeoTypes::POINT_2D);

LogicalType GeoTypes::POLYGON_2D = LogicalType::LIST(GeoTypes::LINESTRING_2D);

LogicalType GeoTypes::GEOMETRY = LogicalType::BLOB;

LogicalType GeoTypes::WKB_BLOB = LogicalType::BLOB;

static void AddType(Catalog &catalog, ClientContext &context, LogicalType &type, const string &name) {
	auto type_info = CreateTypeInfo(name, type);
	type_info.temporary = true;
	type_info.internal = true;
	type.SetAlias(name);
	auto type_entry = (TypeCatalogEntry *)catalog.CreateType(context, &type_info);
	LogicalType::SetCatalog(type, type_entry);
}

// Casts
static bool ToPoint2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &children = StructVector::GetEntries(source);
	auto &x_child = children[0];
	auto &y_child = children[1];

	auto &result_children = StructVector::GetEntries(result);
	auto &result_x_child = result_children[0];
	auto &result_y_child = result_children[1];
	result_x_child->Reference(*x_child);
	result_y_child->Reference(*y_child);

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

static bool Point2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using GEOMETRY_TYPE = PrimitiveType<string_t>;

	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);

	GenericExecutor::ExecuteUnary<POINT_TYPE, GEOMETRY_TYPE>(source, result, count, [&](POINT_TYPE &point) {
		// Don't bother resetting the allocator, points take up a fixed amount of space anyway
		auto geom = ctx.CreatePoint(point.a_val, point.b_val);
		return ctx.Serialize(result, Geometry(std::move(geom)));
	});
	return true;
}

static bool LineString2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);

	auto &coord_vec = ListVector::GetEntry(source);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &line) {
		auto geom = ctx.CreateLineString(line.length);
		for (idx_t i = 0; i < line.length; i++) {
			auto x = x_data[line.offset + i];
			auto y = y_data[line.offset + i];
			geom.points[i].x = x;
			geom.points[i].y = y;
		}
		return ctx.Serialize(result, Geometry(std::move(geom)));
	});
	return true;
}

static bool Polygon2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);

	auto polygon_entries = ListVector::GetData(source);
	auto &ring_vec = ListVector::GetEntry(source);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &poly) {
		auto geom = ctx.CreatePolygon(poly.length);

		for (idx_t i = 0; i < poly.length; i++) {
			auto ring = ring_entries[poly.offset + i];
			auto ring_array = ctx.AllocateVertexVector(ring.length);
			for (idx_t j = 0; j < ring.length; j++) {
				auto x = x_data[ring.offset + j];
				auto y = y_data[ring.offset + j];
				ring_array[j].x = x;
				ring_array[j].y = y;
			}
			geom.rings[i] = std::move(ring_array);
		}
		return ctx.Serialize(result, Geometry(geom));
	});
	return true;
}

static bool Box2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);

	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using GEOMETRY_TYPE = PrimitiveType<string_t>;
	uint32_t capacity = 4;
	GenericExecutor::ExecuteUnary<BOX_TYPE, GEOMETRY_TYPE>(source, result, count, [&](BOX_TYPE &box) {
		// Don't bother resetting the allocator, boxes take up a fixed amount of space anyway
		// Since BOX is not a standard geometry type, we serialize them as polygons
		auto geom = ctx.CreatePolygon(1, &capacity);
		geom.rings[0].data[0].x = box.a_val;
		geom.rings[0].data[0].y = box.b_val;
		geom.rings[0].data[1].x = box.c_val;
		geom.rings[0].data[1].y = box.b_val;
		geom.rings[0].data[2].x = box.c_val;
		geom.rings[0].data[2].y = box.d_val;
		geom.rings[0].data[3].x = box.a_val;
		geom.rings[0].data[3].y = box.d_val;
		geom.rings[0].count = 4;
		return ctx.Serialize(result, Geometry(geom));
	});
	return true;
}

static bool WKBToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t input) {
		auto geometry = ctx.FromWKB(input.GetDataUnsafe(), input.GetSize());
		return ctx.Serialize(result, geometry);
	});
	return true;
}


void GeoTypes::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	// POINT_2D
	AddType(catalog, context, GeoTypes::POINT_2D, "POINT_2D");

	// POINT_3D
	AddType(catalog, context, GeoTypes::POINT_3D, "POINT_3D");

	// POINT_4D
	AddType(catalog, context, GeoTypes::POINT_4D, "POINT_4D");

	// LineString2D
	AddType(catalog, context, GeoTypes::LINESTRING_2D, "LINESTRING_2D");

	// Polygon2D
	AddType(catalog, context, GeoTypes::POLYGON_2D, "POLYGON_2D");

	// Box2D
	AddType(catalog, context, GeoTypes::BOX_2D, "BOX_2D");

    // GEOMETRY
	AddType(catalog, context, GeoTypes::GEOMETRY, "GEOMETRY");

	// WKB_BLOB
	AddType(catalog, context, GeoTypes::WKB_BLOB, "WKB_BLOB");

	casts.RegisterCastFunction(GeoTypes::WKB_BLOB, LogicalType::BLOB, DefaultCasts::ReinterpretCast);
	casts.RegisterCastFunction(GeoTypes::LINESTRING_2D, GeoTypes::GEOMETRY, LineString2DToGeometryCast, 1);
	casts.RegisterCastFunction(GeoTypes::LINESTRING_2D, GeoTypes::GEOMETRY, LineString2DToGeometryCast, 1);
	casts.RegisterCastFunction(GeoTypes::POINT_4D, GeoTypes::POINT_2D, ToPoint2DCast, 1);
	casts.RegisterCastFunction(GeoTypes::POINT_3D, GeoTypes::POINT_2D, ToPoint2DCast, 1);
	casts.RegisterCastFunction(GeoTypes::POINT_2D, GeoTypes::GEOMETRY, Point2DToGeometryCast, 1);
	casts.RegisterCastFunction(GeoTypes::POLYGON_2D, GeoTypes::GEOMETRY, Polygon2DToGeometryCast, 1);
	casts.RegisterCastFunction(GeoTypes::BOX_2D, GeoTypes::GEOMETRY, Box2DToGeometryCast, 1);

	// TODO: remove this implicit cast once we have more functions for the geometry type itself
	casts.RegisterCastFunction(GeoTypes::WKB_BLOB, GeoTypes::GEOMETRY, WKBToGeometryCast, 1);
}

} // namespace core

} // namespace geo