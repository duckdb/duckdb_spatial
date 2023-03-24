#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions/cast.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_factory.hpp"
#include "geo/core/functions/common.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"


namespace geo {

namespace core {

//------------------------------------------------------------------------------
// Point2D -> Geometry
//------------------------------------------------------------------------------
static bool Point2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using GEOMETRY_TYPE = PrimitiveType<string_t>;

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	GenericExecutor::ExecuteUnary<POINT_TYPE, GEOMETRY_TYPE>(source, result, count, [&](POINT_TYPE &point) {
		// Don't bother resetting the allocator, points take up a fixed amount of space anyway
		auto geom = lstate.factory.CreatePoint(point.a_val, point.b_val);
		return lstate.factory.Serialize(result, Geometry(geom));
	});
	return true;
}

//------------------------------------------------------------------------------
// Geometry -> Point2D
//------------------------------------------------------------------------------
static bool GeometryToPoint2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using GEOMETRY_TYPE = PrimitiveType<string_t>;

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	GenericExecutor::ExecuteUnary<GEOMETRY_TYPE, POINT_TYPE>(source, result, count, [&](GEOMETRY_TYPE &geometry) {
		auto geom = lstate.factory.Deserialize(geometry.val);
		if (geom.Type() != GeometryType::POINT) {
			throw CastException("Cannot cast non-point GEOMETRY to POINT_2D");
		}
		auto &point = geom.GetPoint();
		if(point.IsEmpty()) {
			throw CastException("Cannot cast empty point GEOMETRY to POINT_2D");
		}
		auto &vertex = point.GetVertex();
		return POINT_TYPE { vertex.x, vertex.y };
	});
	return true;
}


//------------------------------------------------------------------------------
// LineString2D -> Geometry
//------------------------------------------------------------------------------
static bool LineString2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	auto &coord_vec = ListVector::GetEntry(source);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &line) {
		auto geom = lstate.factory.CreateLineString(line.length);
		for (idx_t i = 0; i < line.length; i++) {
			auto x = x_data[line.offset + i];
			auto y = y_data[line.offset + i];
			geom.points[i].x = x;
			geom.points[i].y = y;
		}
		return lstate.factory.Serialize(result, Geometry(geom));
	});
	return true;
}

//------------------------------------------------------------------------------
// Geometry -> LineString2D
//------------------------------------------------------------------------------
static bool GeometryToLineString2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);


	auto &coord_vec = ListVector::GetEntry(result);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	idx_t total_coords = 0;
	UnaryExecutor::Execute<string_t, list_entry_t>(source, result, count, [&](string_t &geom) {
		auto geometry = lstate.factory.Deserialize(geom);
		if (geometry.Type() != GeometryType::LINESTRING) {
			throw CastException("Cannot cast non-linestring GEOMETRY to LINESTRING_2D");
		}

		auto &line = geometry.GetLineString();
		auto line_size = line.Count();

		auto entry = list_entry_t(total_coords, line_size);
		total_coords += line_size;
		ListVector::Reserve(result, total_coords);

		for (idx_t i = 0; i < line_size; i++) {
			x_data[entry.offset + i] = line.points[i].x;
			y_data[entry.offset + i] = line.points[i].y;
		}
		return entry;
	});
	ListVector::SetListSize(result, total_coords);
	return true;
}


//------------------------------------------------------------------------------
// Polygon2D -> Geometry
//------------------------------------------------------------------------------
static bool Polygon2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	auto &ring_vec = ListVector::GetEntry(source);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &poly) {
		auto geom = lstate.factory.CreatePolygon(poly.length);

		for (idx_t i = 0; i < poly.length; i++) {
			auto ring = ring_entries[poly.offset + i];
			auto ring_array = lstate.factory.AllocateVertexVector(ring.length);
			for (idx_t j = 0; j < ring.length; j++) {
				auto x = x_data[ring.offset + j];
				auto y = y_data[ring.offset + j];
				ring_array[j].x = x;
				ring_array[j].y = y;
			}
			geom.rings[i] = ring_array;
		}
		return lstate.factory.Serialize(result, Geometry(geom));
	});
	return true;
}

//------------------------------------------------------------------------------
// Geometry -> Polygon2D
//------------------------------------------------------------------------------
static bool GeometryToPolygon2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);

	auto poly_entries = ListVector::GetData(result);
	auto &ring_vec = ListVector::GetEntry(result);

	idx_t total_rings = 0;
	idx_t total_coords = 0;

	UnaryExecutor::Execute<string_t, list_entry_t>(source, result, count, [&](string_t &geom) {
		auto geometry = lstate.factory.Deserialize(geom);
		if (geometry.Type() != GeometryType::POLYGON) {
			throw CastException("Cannot cast non-linestring GEOMETRY to POLYGON_2D");
		}

		auto &poly = geometry.GetPolygon();
		auto poly_size = poly.num_rings;
		auto poly_entry = list_entry_t(total_rings, poly_size);

		ListVector::Reserve(result, total_rings + poly_size);

		for (idx_t ring_idx = 0; ring_idx < poly_size; ring_idx++) {
			auto ring = poly.rings[ring_idx];
			auto ring_size = ring.Count();
			auto ring_entry = list_entry_t(total_coords, ring_size);

			ListVector::Reserve(ring_vec, total_coords + ring_size);

			auto ring_entries = ListVector::GetData(ring_vec);
			auto &coord_vec = ListVector::GetEntry(ring_vec);
			auto &coord_vec_children = StructVector::GetEntries(coord_vec);
			auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
			auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

			ring_entries[total_rings + ring_idx] = ring_entry;

			for (idx_t j = 0; j < ring_size; j++) {
				x_data[ring_entry.offset + j] = ring.data[j].x;
				y_data[ring_entry.offset + j] = ring.data[j].y;
			}
			total_coords += ring_size;
		}
		total_rings += poly_size;

		return poly_entry;
	});

	ListVector::SetListSize(result, total_rings);
	ListVector::SetListSize(ring_vec, total_coords);

	return true;
}


//------------------------------------------------------------------------------
// BOX_2D -> Geometry
//------------------------------------------------------------------------------
// Since BOX is a non-standard geometry type, we serialize it as a polygon
static bool Box2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);


	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using GEOMETRY_TYPE = PrimitiveType<string_t>;
	uint32_t capacity = 4;
	GenericExecutor::ExecuteUnary<BOX_TYPE, GEOMETRY_TYPE>(source, result, count, [&](BOX_TYPE &box) {
		// Don't bother resetting the allocator, boxes take up a fixed amount of space anyway
		auto geom = lstate.factory.CreatePolygon(1, &capacity);
		geom.rings[0].data[0].x = box.a_val;
		geom.rings[0].data[0].y = box.b_val;
		geom.rings[0].data[1].x = box.c_val;
		geom.rings[0].data[1].y = box.b_val;
		geom.rings[0].data[2].x = box.c_val;
		geom.rings[0].data[2].y = box.d_val;
		geom.rings[0].data[3].x = box.a_val;
		geom.rings[0].data[3].y = box.d_val;
		geom.rings[0].count = 4;
		return lstate.factory.Serialize(result, Geometry(geom));
	});
	return true;
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreCastFunctions::RegisterGeometryCasts(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	casts.RegisterCastFunction(GeoTypes::GEOMETRY(), GeoTypes::LINESTRING_2D(),
	                           BoundCastInfo(GeometryToLineString2DCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
	casts.RegisterCastFunction(GeoTypes::LINESTRING_2D(), GeoTypes::GEOMETRY(),
	                           BoundCastInfo(LineString2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	casts.RegisterCastFunction(GeoTypes::POINT_2D(), GeoTypes::GEOMETRY(),
	                           BoundCastInfo(Point2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
	casts.RegisterCastFunction(GeoTypes::GEOMETRY(), GeoTypes::POINT_2D(),
	                           BoundCastInfo(GeometryToPoint2DCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	casts.RegisterCastFunction(GeoTypes::POLYGON_2D(), GeoTypes::GEOMETRY(),
	                           BoundCastInfo(Polygon2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
	casts.RegisterCastFunction(GeoTypes::GEOMETRY(), GeoTypes::POLYGON_2D(),
	                           BoundCastInfo(GeometryToPolygon2DCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	casts.RegisterCastFunction(GeoTypes::BOX_2D(), GeoTypes::GEOMETRY(),
	                           BoundCastInfo(Box2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
}

} // namespace core

} // namespace geo