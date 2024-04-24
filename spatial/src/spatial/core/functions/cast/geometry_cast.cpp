#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/functions/common.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Point2D -> Geometry
//------------------------------------------------------------------------------
static bool Point2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	auto &arena = lstate.arena;

	GenericExecutor::ExecuteUnary<POINT_TYPE, GEOMETRY_TYPE>(source, result, count, [&](POINT_TYPE &point) {
		auto geom = Point::CreateFromVertex(arena, VertexXY {point.a_val, point.b_val});
		return Geometry::Serialize(geom, result);
	});
	return true;
}

//------------------------------------------------------------------------------
// Geometry -> Point2D
//------------------------------------------------------------------------------
static bool GeometryToPoint2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	auto &arena = lstate.arena;

	GenericExecutor::ExecuteUnary<GEOMETRY_TYPE, POINT_TYPE>(source, result, count, [&](GEOMETRY_TYPE &geometry) {
		auto geom = Geometry::Deserialize(arena, geometry.val);
		if (geom.GetType() != GeometryType::POINT) {
			throw ConversionException("Cannot cast non-point GEOMETRY to POINT_2D");
		}
		if (Point::IsEmpty(geom)) {
			// TODO: Maybe make this return NULL instead
			throw ConversionException("Cannot cast empty point GEOMETRY to POINT_2D");
		}
		auto vertex = Point::GetVertex(geom);
		return POINT_TYPE {vertex.x, vertex.y};
	});
	return true;
}

//------------------------------------------------------------------------------
// LineString2D -> Geometry
//------------------------------------------------------------------------------
static bool LineString2DToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	auto &arena = lstate.arena;

	auto &coord_vec = ListVector::GetEntry(source);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, geometry_t>(source, result, count, [&](list_entry_t &line) {
		auto geom = LineString::Create(arena, line.length, false, false);
		for (idx_t i = 0; i < line.length; i++) {
			auto x = x_data[line.offset + i];
			auto y = y_data[line.offset + i];
			LineString::SetVertex<VertexXY>(geom, i, VertexXY {x, y});
		}
		return Geometry::Serialize(geom, result);
	});
	return true;
}

//------------------------------------------------------------------------------
// Geometry -> LineString2D
//------------------------------------------------------------------------------
static bool GeometryToLineString2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	auto &arena = lstate.arena;

	auto &coord_vec = ListVector::GetEntry(result);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	idx_t total_coords = 0;
	UnaryExecutor::Execute<geometry_t, list_entry_t>(source, result, count, [&](geometry_t &geom) {
		if (geom.GetType() != GeometryType::LINESTRING) {
			throw ConversionException("Cannot cast non-linestring GEOMETRY to LINESTRING_2D");
		}

		auto line = Geometry::Deserialize(arena, geom);
		auto line_size = LineString::VertexCount(line);

		auto entry = list_entry_t(total_coords, line_size);
		total_coords += line_size;
		ListVector::Reserve(result, total_coords);

		for (idx_t i = 0; i < line_size; i++) {
			auto vertex = LineString::GetVertex(line, i);
			x_data[entry.offset + i] = vertex.x;
			y_data[entry.offset + i] = vertex.y;
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
	auto &arena = lstate.arena;

	auto &ring_vec = ListVector::GetEntry(source);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, geometry_t>(source, result, count, [&](list_entry_t &poly) {
		auto geom = Polygon::Create(arena, poly.length, false, false);

		for (idx_t i = 0; i < poly.length; i++) {
			auto ring = ring_entries[poly.offset + i];
			auto &ring_array = Polygon::Part(geom, i);
			LineString::Resize(ring_array, arena, ring.length);
			for (idx_t j = 0; j < ring.length; j++) {
				auto x = x_data[ring.offset + j];
				auto y = y_data[ring.offset + j];
				LineString::SetVertex<VertexXY>(ring_array, j, VertexXY {x, y});
			}
		}
		return Geometry::Serialize(geom, result);
	});
	return true;
}

//------------------------------------------------------------------------------
// Geometry -> Polygon2D
//------------------------------------------------------------------------------
static bool GeometryToPolygon2DCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	auto &arena = lstate.arena;

	auto &ring_vec = ListVector::GetEntry(result);

	idx_t total_rings = 0;
	idx_t total_coords = 0;

	UnaryExecutor::Execute<geometry_t, list_entry_t>(source, result, count, [&](geometry_t &geom) {
		if (geom.GetType() != GeometryType::POLYGON) {
			throw ConversionException("Cannot cast non-polygon GEOMETRY to POLYGON_2D");
		}
		auto poly = Geometry::Deserialize(arena, geom);
		auto poly_size = Polygon::PartCount(poly);
		auto poly_entry = list_entry_t(total_rings, poly_size);

		ListVector::Reserve(result, total_rings + poly_size);

		for (idx_t ring_idx = 0; ring_idx < poly_size; ring_idx++) {
			auto ring = Polygon::Part(poly, ring_idx);
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
				auto vert = LineString::GetVertex(ring, j);
				x_data[ring_entry.offset + j] = vert.x;
				y_data[ring_entry.offset + j] = vert.y;
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
	auto &arena = lstate.arena;
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;
	GenericExecutor::ExecuteUnary<BOX_TYPE, GEOMETRY_TYPE>(source, result, count, [&](BOX_TYPE &box) {
		auto minx = box.a_val;
		auto miny = box.b_val;
		auto maxx = box.c_val;
		auto maxy = box.d_val;
		auto polygon = Polygon::CreateFromBox(arena, minx, miny, maxx, maxy);
		return Geometry::Serialize(polygon, result);
	});
	return true;
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreCastFunctions::RegisterGeometryCasts(DatabaseInstance &db) {
	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::GEOMETRY(), GeoTypes::LINESTRING_2D(),
	    BoundCastInfo(GeometryToLineString2DCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::LINESTRING_2D(), GeoTypes::GEOMETRY(),
	    BoundCastInfo(LineString2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::GEOMETRY(), GeoTypes::POINT_2D(),
	    BoundCastInfo(GeometryToPoint2DCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::POINT_2D(), GeoTypes::GEOMETRY(),
	    BoundCastInfo(Point2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::GEOMETRY(), GeoTypes::POLYGON_2D(),
	    BoundCastInfo(GeometryToPolygon2DCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::POLYGON_2D(), GeoTypes::GEOMETRY(),
	    BoundCastInfo(Polygon2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);

	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::BOX_2D(), GeoTypes::GEOMETRY(),
	    BoundCastInfo(Box2DToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
}

} // namespace core

} // namespace spatial