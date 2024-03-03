#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"

#include "yyjson.h"

#include "spatial/core/geometry/geometry_processor.hpp"

namespace spatial {

namespace core {

using namespace duckdb_yyjson_spatial;

class JSONAllocator {
	// Stolen from the JSON extension :)
public:
	explicit JSONAllocator(ArenaAllocator &allocator)
	    : allocator(allocator), yyjson_allocator({Allocate, Reallocate, Free, &allocator}) {
	}

	inline yyjson_alc *GetYYJSONAllocator() {
		return &yyjson_allocator;
	}

	void Reset() {
		allocator.Reset();
	}

private:
	static inline void *Allocate(void *ctx, size_t size) {
		auto alloc = (ArenaAllocator *)ctx;
		return alloc->AllocateAligned(size);
	}

	static inline void *Reallocate(void *ctx, void *ptr, size_t old_size, size_t size) {
		auto alloc = (ArenaAllocator *)ctx;
		return alloc->ReallocateAligned((data_ptr_t)ptr, old_size, size);
	}

	static inline void Free(void *ctx, void *ptr) {
		// NOP because ArenaAllocator can't free
	}

private:
	ArenaAllocator &allocator;
	yyjson_alc yyjson_allocator;
};

//------------------------------------------------------------------------------
// GEOMETRY -> GEOJSON Fragment
//------------------------------------------------------------------------------

static void VerticesToGeoJSON(const VertexVector &vertices, yyjson_mut_doc *doc, yyjson_mut_val *arr) {
	// TODO: If the vertexvector is empty, do we null, add an empty array or a pair of NaN?
	for (uint32_t i = 0; i < vertices.count; i++) {
		auto vertex = vertices.Get(i);
		auto coord = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_real(doc, coord, vertex.x);
		yyjson_mut_arr_add_real(doc, coord, vertex.y);
		yyjson_mut_arr_append(arr, coord);
	}
}

static void ToGeoJSON(const Point &point, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "Point");

	auto coords = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

	if (!point.IsEmpty()) {
		auto vertex = point.GetVertex();
		yyjson_mut_arr_add_real(doc, coords, vertex.x);
		yyjson_mut_arr_add_real(doc, coords, vertex.y);
	}
}

static void ToGeoJSON(const LineString &line, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "LineString");

	auto coords = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
	VerticesToGeoJSON(line.Vertices(), doc, coords);
}

static void ToGeoJSON(const Polygon &poly, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "Polygon");

	auto coords = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
	for (auto &ring : poly.Rings()) {
		auto ring_coords = yyjson_mut_arr(doc);
		VerticesToGeoJSON(ring, doc, ring_coords);
		yyjson_mut_arr_append(coords, ring_coords);
	}
}

static void ToGeoJSON(const MultiPoint &mpoint, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "MultiPoint");

	auto coords = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
	for (auto &point : mpoint) {
		VerticesToGeoJSON(point.Vertices(), doc, coords);
	}
}

static void ToGeoJSON(const MultiLineString &mline, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "MultiLineString");

	auto coords = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

	for (auto &line : mline) {
		auto line_coords = yyjson_mut_arr(doc);
		VerticesToGeoJSON(line.Vertices(), doc, line_coords);
		yyjson_mut_arr_append(coords, line_coords);
	}
}

static void ToGeoJSON(const MultiPolygon &mpoly, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "MultiPolygon");

	auto coords = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

	for (auto &poly : mpoly) {
		auto poly_coords = yyjson_mut_arr(doc);
		for (auto &ring : poly.Rings()) {
			auto ring_coords = yyjson_mut_arr(doc);
			VerticesToGeoJSON(ring, doc, ring_coords);
			yyjson_mut_arr_append(poly_coords, ring_coords);
		}
		yyjson_mut_arr_append(coords, poly_coords);
	}
}

static void ToGeoJSON(const Geometry &geom, yyjson_mut_doc *doc, yyjson_mut_val *obj);
static void ToGeoJSON(const GeometryCollection &collection, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	yyjson_mut_obj_add_str(doc, obj, "type", "GeometryCollection");
	auto arr = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, obj, "geometries", arr);

	for (auto &geom : collection) {
		auto geom_obj = yyjson_mut_obj(doc);
		ToGeoJSON(geom, doc, geom_obj);
		yyjson_mut_arr_append(arr, geom_obj);
	}
}

static void ToGeoJSON(const Geometry &geom, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
	switch (geom.Type()) {
	case GeometryType::POINT:
		ToGeoJSON(geom.GetPoint(), doc, obj);
		break;
	case GeometryType::LINESTRING:
		ToGeoJSON(geom.GetLineString(), doc, obj);
		break;
	case GeometryType::POLYGON:
		ToGeoJSON(geom.GetPolygon(), doc, obj);
		break;
	case GeometryType::MULTIPOINT:
		ToGeoJSON(geom.GetMultiPoint(), doc, obj);
		break;
	case GeometryType::MULTILINESTRING:
		ToGeoJSON(geom.GetMultiLineString(), doc, obj);
		break;
	case GeometryType::MULTIPOLYGON:
		ToGeoJSON(geom.GetMultiPolygon(), doc, obj);
		break;
	case GeometryType::GEOMETRYCOLLECTION:
		ToGeoJSON(geom.GetGeometryCollection(), doc, obj);
		break;
	default: {
		throw NotImplementedException(
		    StringUtil::Format("Geometry type %d not supported", static_cast<int>(geom.Type())));
	}
	}
}

static void GeometryToGeoJSONFragmentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	JSONAllocator json_allocator(lstate.factory.allocator);

	UnaryExecutor::Execute<geometry_t, string_t>(input, result, count, [&](geometry_t input) {
		auto geometry = lstate.factory.Deserialize(input);

		auto doc = yyjson_mut_doc_new(json_allocator.GetYYJSONAllocator());
		auto obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, obj);

		ToGeoJSON(geometry, doc, obj);

		size_t json_size = 0;
		// TODO: YYJSON_WRITE_PRETTY
		auto json_data = yyjson_mut_write(doc, 0, &json_size);
		auto json_str = StringVector::AddString(result, json_data, json_size);
		return json_str;
	});
}

//------------------------------------------------------------------------------
// GEOJSON Fragment -> GEOMETRY
//------------------------------------------------------------------------------
static Point PointFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory, const string_t &raw) {
	auto len = yyjson_arr_size(coord_array);
	if (len == 0) {
		// empty point
		return factory.CreateEmptyPoint();
	}
	if (len < 2) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array of at least length 2: %s",
		                            raw.GetString());
	}
	auto x_val = yyjson_arr_get_first(coord_array);
	if (!yyjson_is_num(x_val)) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array of numbers: %s", raw.GetString());
	}
	auto y_val = len == 2 ? yyjson_arr_get_last(coord_array) : yyjson_arr_get(coord_array, 1);
	if (!yyjson_is_num(y_val)) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array of numbers: %s", raw.GetString());
	}
	auto x = yyjson_get_num(x_val);
	auto y = yyjson_get_num(y_val);
	return factory.CreatePoint(x, y);
}

static VertexVector VerticesFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory, const string_t &raw) {
	auto len = yyjson_arr_size(coord_array);
	if (len == 0) {
		// Empty
		return factory.AllocateVertexVector(0);
	} else {
		VertexVector vertices = factory.AllocateVertexVector(len);
		for (idx_t i = 0; i < len; i++) {
			auto coord_val = yyjson_arr_get(coord_array, i);
			if (!yyjson_is_arr(coord_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			auto coord_len = yyjson_arr_size(coord_val);
			if (coord_len < 2) {
				throw InvalidInputException(
				    "GeoJSON input coordinates field is not an array of arrays of length >= 2: %s", raw.GetString());
			}
			auto x_val = yyjson_arr_get_first(coord_val);
			if (!yyjson_is_num(x_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays of numbers: %s",
				                            raw.GetString());
			}
			auto y_val = coord_len == 2 ? yyjson_arr_get_last(coord_val) : yyjson_arr_get(coord_val, 1);
			if (!yyjson_is_num(y_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays of numbers: %s",
				                            raw.GetString());
			}
			auto x = yyjson_get_num(x_val);
			auto y = yyjson_get_num(y_val);
			vertices.Add(Vertex(x, y));
		}
		return vertices;
	}
}

static LineString LineStringFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory, const string_t &raw) {
	return LineString(VerticesFromGeoJSON(coord_array, factory, raw));
}

static Polygon PolygonFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory, const string_t &raw) {
	auto num_rings = yyjson_arr_size(coord_array);
	if (num_rings == 0) {
		// Empty
		return factory.CreateEmptyPolygon();
	} else {
		// Polygon
		auto polygon = factory.CreatePolygon(num_rings);
		size_t idx, max;
		yyjson_val *ring_val;
		yyjson_arr_foreach(coord_array, idx, max, ring_val) {
			if (!yyjson_is_arr(ring_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			polygon.Ring(idx) = VerticesFromGeoJSON(ring_val, factory, raw);
		}

		return polygon;
	}
}

static MultiPoint MultiPointFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory, const string_t &raw) {
	auto num_points = yyjson_arr_size(coord_array);
	if (num_points == 0) {
		// Empty
		return factory.CreateEmptyMultiPoint();
	} else {
		// MultiPoint
		auto multi_point = factory.CreateMultiPoint(num_points);
		size_t idx, max;
		yyjson_val *point_val;
		yyjson_arr_foreach(coord_array, idx, max, point_val) {
			if (!yyjson_is_arr(point_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			if (yyjson_arr_size(point_val) < 2) {
				throw InvalidInputException(
				    "GeoJSON input coordinates field is not an array of arrays of length >= 2: %s", raw.GetString());
			}
			multi_point[idx] = PointFromGeoJSON(point_val, factory, raw);
		}
		return multi_point;
	}
}

static MultiLineString MultiLineStringFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory,
                                                  const string_t &raw) {
	auto num_linestrings = yyjson_arr_size(coord_array);
	if (num_linestrings == 0) {
		// Empty
		return factory.CreateEmptyMultiLineString();
	} else {
		// MultiLineString
		auto multi_linestring = factory.CreateMultiLineString(num_linestrings);
		size_t idx, max;
		yyjson_val *linestring_val;
		yyjson_arr_foreach(coord_array, idx, max, linestring_val) {
			if (!yyjson_is_arr(linestring_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			multi_linestring[idx] = LineStringFromGeoJSON(linestring_val, factory, raw);
		}

		return multi_linestring;
	}
}

static MultiPolygon MultiPolygonFromGeoJSON(yyjson_val *coord_array, GeometryFactory &factory, const string_t &raw) {
	auto num_polygons = yyjson_arr_size(coord_array);
	if (num_polygons == 0) {
		// Empty
		return factory.CreateEmptyMultiPolygon();
	} else {
		// MultiPolygon
		auto multi_polygon = factory.CreateMultiPolygon(num_polygons);
		size_t idx, max;
		yyjson_val *polygon_val;
		yyjson_arr_foreach(coord_array, idx, max, polygon_val) {
			if (!yyjson_is_arr(polygon_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			multi_polygon[idx] = PolygonFromGeoJSON(polygon_val, factory, raw);
		}

		return multi_polygon;
	}
}

static Geometry FromGeoJSON(yyjson_val *root, GeometryFactory &factory, const string_t &raw);

static GeometryCollection GeometryCollectionFromGeoJSON(yyjson_val *root, GeometryFactory &factory,
                                                        const string_t &raw) {
	auto geometries_val = yyjson_obj_get(root, "geometries");
	if (!geometries_val) {
		throw InvalidInputException("GeoJSON input does not have a geometries field: %s", raw.GetString());
	}
	if (!yyjson_is_arr(geometries_val)) {
		throw InvalidInputException("GeoJSON input geometries field is not an array: %s", raw.GetString());
	}
	auto num_geometries = yyjson_arr_size(geometries_val);
	if (num_geometries == 0) {
		// Empty
		return factory.CreateEmptyGeometryCollection();
	} else {
		// GeometryCollection
		auto geometry_collection = factory.CreateGeometryCollection(num_geometries);
		size_t idx, max;
		yyjson_val *geometry_val;
		yyjson_arr_foreach(geometries_val, idx, max, geometry_val) {
			geometry_collection[idx] = FromGeoJSON(geometry_val, factory, raw);
		}

		return geometry_collection;
	}
}

static Geometry FromGeoJSON(yyjson_val *root, GeometryFactory &factory, const string_t &raw) {
	auto type_val = yyjson_obj_get(root, "type");
	if (!type_val) {
		throw InvalidInputException("GeoJSON input does not have a type field: %s", raw.GetString());
	}
	auto type_str = yyjson_get_str(type_val);
	if (!type_str) {
		throw InvalidInputException("GeoJSON input type field is not a string: %s", raw.GetString());
	}

	if (StringUtil::Equals(type_str, "GeometryCollection")) {
		return GeometryCollectionFromGeoJSON(root, factory, raw);
	}

	// Get the coordinates
	auto coord_array = yyjson_obj_get(root, "coordinates");
	if (!coord_array) {
		throw InvalidInputException("GeoJSON input does not have a coordinates field: %s", raw.GetString());
	}
	if (!yyjson_is_arr(coord_array)) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array: %s", raw.GetString());
	}

	if (StringUtil::Equals(type_str, "Point")) {
		return PointFromGeoJSON(coord_array, factory, raw);
	} else if (StringUtil::Equals(type_str, "LineString")) {
		return LineStringFromGeoJSON(coord_array, factory, raw);
	} else if (StringUtil::Equals(type_str, "Polygon")) {
		return PolygonFromGeoJSON(coord_array, factory, raw);
	} else if (StringUtil::Equals(type_str, "MultiPoint")) {
		return MultiPointFromGeoJSON(coord_array, factory, raw);
	} else if (StringUtil::Equals(type_str, "MultiLineString")) {
		return MultiLineStringFromGeoJSON(coord_array, factory, raw);
	} else if (StringUtil::Equals(type_str, "MultiPolygon")) {
		return MultiPolygonFromGeoJSON(coord_array, factory, raw);
	} else {
		throw InvalidInputException("GeoJSON input has invalid type field: %s", raw.GetString());
	}
}

static void GeoJSONFragmentToGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	JSONAllocator json_allocator(lstate.factory.allocator);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		yyjson_read_err err;
		auto doc = yyjson_read_opts(const_cast<char *>(input.GetDataUnsafe()), input.GetSize(),
		                            YYJSON_READ_ALLOW_TRAILING_COMMAS | YYJSON_READ_ALLOW_COMMENTS,
		                            json_allocator.GetYYJSONAllocator(), &err);

		if (err.code) {
			throw InvalidInputException("Could not parse GeoJSON input: %s, (%s)", err.msg, input.GetString());
		}

		auto root = yyjson_doc_get_root(doc);
		if (!yyjson_is_obj(root)) {
			throw InvalidInputException("Could not parse GeoJSON input: %s, (%s)", err.msg, input.GetString());
		} else {
			auto geom = FromGeoJSON(root, lstate.factory, input);
			return lstate.factory.Serialize(result, geom);
		}
	});
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsGeoJSON(DatabaseInstance &db) {
	ScalarFunctionSet to_geojson("ST_AsGeoJSON");
	to_geojson.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::VARCHAR,
	                                      GeometryToGeoJSONFragmentFunction, nullptr, nullptr, nullptr,
	                                      GeometryFunctionLocalState::Init));
	ExtensionUtil::RegisterFunction(db, to_geojson);

	ScalarFunctionSet from_geojson("ST_GeomFromGeoJSON");
	from_geojson.AddFunction(ScalarFunction({LogicalType::VARCHAR}, GeoTypes::GEOMETRY(),
	                                        GeoJSONFragmentToGeometryFunction, nullptr, nullptr, nullptr,
	                                        GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, from_geojson);
}

} // namespace core

} // namespace spatial