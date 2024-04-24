#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
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

static void VerticesToGeoJSON(const Geometry &vertices, yyjson_mut_doc *doc, yyjson_mut_val *arr) {
	// TODO: If the vertexvector is empty, do we null, add an empty array or a pair of NaN?
	auto haz_z = vertices.GetProperties().HasZ();
	auto has_m = vertices.GetProperties().HasM();
	// GeoJSON does not support M values, so we ignore them
	if (haz_z && has_m) {
		for (uint32_t i = 0; i < vertices.Count(); i++) {
			auto coord = yyjson_mut_arr(doc);
			auto vert = SinglePartGeometry::GetVertex<VertexXYZM>(vertices, i);
			yyjson_mut_arr_add_real(doc, coord, vert.x);
			yyjson_mut_arr_add_real(doc, coord, vert.y);
			yyjson_mut_arr_add_real(doc, coord, vert.z);
			yyjson_mut_arr_append(arr, coord);
		}
	} else if (haz_z) {
		for (uint32_t i = 0; i < vertices.Count(); i++) {
			auto coord = yyjson_mut_arr(doc);
			auto vert = SinglePartGeometry::GetVertex<VertexXYZ>(vertices, i);
			yyjson_mut_arr_add_real(doc, coord, vert.x);
			yyjson_mut_arr_add_real(doc, coord, vert.y);
			yyjson_mut_arr_add_real(doc, coord, vert.z);
			yyjson_mut_arr_append(arr, coord);
		}
	} else if (has_m) {
		for (uint32_t i = 0; i < vertices.Count(); i++) {
			auto coord = yyjson_mut_arr(doc);
			auto vert = SinglePartGeometry::GetVertex<VertexXYM>(vertices, i);
			yyjson_mut_arr_add_real(doc, coord, vert.x);
			yyjson_mut_arr_add_real(doc, coord, vert.y);
			yyjson_mut_arr_append(arr, coord);
		}
	} else {
		for (uint32_t i = 0; i < vertices.Count(); i++) {
			auto coord = yyjson_mut_arr(doc);
			auto vert = SinglePartGeometry::GetVertex<VertexXY>(vertices, i);
			yyjson_mut_arr_add_real(doc, coord, vert.x);
			yyjson_mut_arr_add_real(doc, coord, vert.y);
			yyjson_mut_arr_append(arr, coord);
		}
	}
}

struct ToGeoJSONFunctor {

	// Point
	static void Case(Geometry::Tags::Point, const Geometry &point, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "Point");

		auto coords = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
		if (!Point::IsEmpty(point)) {
			auto has_z = point.GetProperties().HasZ();
			auto has_m = point.GetProperties().HasM();
			if (has_z && has_m) {
				auto vert = Point::GetVertex<VertexXYZM>(point);
				yyjson_mut_arr_add_real(doc, coords, vert.x);
				yyjson_mut_arr_add_real(doc, coords, vert.y);
				yyjson_mut_arr_add_real(doc, coords, vert.z);
			} else if (has_z) {
				auto vert = Point::GetVertex<VertexXYZ>(point);
				yyjson_mut_arr_add_real(doc, coords, vert.x);
				yyjson_mut_arr_add_real(doc, coords, vert.y);
				yyjson_mut_arr_add_real(doc, coords, vert.z);
			} else if (has_m) {
				auto vert = Point::GetVertex<VertexXYM>(point);
				yyjson_mut_arr_add_real(doc, coords, vert.x);
				yyjson_mut_arr_add_real(doc, coords, vert.y);
			} else {
				auto vert = Point::GetVertex<VertexXY>(point);
				yyjson_mut_arr_add_real(doc, coords, vert.x);
				yyjson_mut_arr_add_real(doc, coords, vert.y);
			}
		}
	}

	// LineString
	static void Case(Geometry::Tags::LineString, const Geometry &line, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "LineString");

		auto coords = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
		VerticesToGeoJSON(line, doc, coords);
	}

	// Polygon
	static void Case(Geometry::Tags::Polygon, const Geometry &poly, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "Polygon");

		auto coords = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
		for (uint32_t i = 0; i < Polygon::PartCount(poly); i++) {
			auto &ring = Polygon::Part(poly, i);
			auto ring_coords = yyjson_mut_arr(doc);
			VerticesToGeoJSON(ring, doc, ring_coords);
			yyjson_mut_arr_append(coords, ring_coords);
		}
	}

	// MultiPoint
	static void Case(Geometry::Tags::MultiPoint, const Geometry &mpoint, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "MultiPoint");

		auto coords = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
		for (uint32_t i = 0; i < MultiPoint::PartCount(mpoint); i++) {
			auto &point = MultiPoint::Part(mpoint, i);
			VerticesToGeoJSON(point, doc, coords);
		}
	}

	// MultiLineString
	static void Case(Geometry::Tags::MultiLineString, const Geometry &mline, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "MultiLineString");

		auto coords = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

		for (uint32_t i = 0; i < MultiLineString::PartCount(mline); i++) {
			auto &line = MultiLineString::Part(mline, i);
			auto line_coords = yyjson_mut_arr(doc);
			VerticesToGeoJSON(line, doc, line_coords);
			yyjson_mut_arr_append(coords, line_coords);
		}
	}

	// MultiPolygon
	static void Case(Geometry::Tags::MultiPolygon, const Geometry &mpoly, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "MultiPolygon");

		auto coords = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

		for (uint32_t i = 0; i < MultiPolygon::PartCount(mpoly); i++) {
			auto &poly = MultiPolygon::Part(mpoly, i);
			auto poly_coords = yyjson_mut_arr(doc);
			for (uint32_t j = 0; j < Polygon::PartCount(poly); j++) {
				auto &ring = Polygon::Part(poly, j);
				auto ring_coords = yyjson_mut_arr(doc);
				VerticesToGeoJSON(ring, doc, ring_coords);
				yyjson_mut_arr_append(poly_coords, ring_coords);
			}
			yyjson_mut_arr_append(coords, poly_coords);
		}
	}

	// GeometryCollection
	static void Case(Geometry::Tags::GeometryCollection, const Geometry &collection, yyjson_mut_doc *doc,
	                 yyjson_mut_val *obj) {
		yyjson_mut_obj_add_str(doc, obj, "type", "GeometryCollection");
		auto arr = yyjson_mut_arr(doc);
		yyjson_mut_obj_add_val(doc, obj, "geometries", arr);

		for (uint32_t i = 0; i < GeometryCollection::PartCount(collection); i++) {
			auto &geom = GeometryCollection::Part(collection, i);
			auto geom_obj = yyjson_mut_obj(doc);
			Geometry::Match<ToGeoJSONFunctor>(geom, doc, geom_obj);
			yyjson_mut_arr_append(arr, geom_obj);
		}
	}
};

static void GeometryToGeoJSONFragmentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	JSONAllocator json_allocator(lstate.arena);

	UnaryExecutor::Execute<geometry_t, string_t>(input, result, count, [&](geometry_t input) {
		auto geom = Geometry::Deserialize(lstate.arena, input);

		auto doc = yyjson_mut_doc_new(json_allocator.GetYYJSONAllocator());
		auto obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, obj);

		Geometry::Match<ToGeoJSONFunctor>(geom, doc, obj);

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

static Geometry PointFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw, bool &has_z) {
	auto len = yyjson_arr_size(coord_array);
	if (len == 0) {
		// empty point
		return Point::CreateEmpty(has_z, false);
	}
	if (len < 2) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array of at least length 2: %s",
		                            raw.GetString());
	}
	auto x_val = yyjson_arr_get_first(coord_array);
	if (!yyjson_is_num(x_val)) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array of numbers: %s", raw.GetString());
	}
	auto y_val = yyjson_arr_get(coord_array, 1);
	if (!yyjson_is_num(y_val)) {
		throw InvalidInputException("GeoJSON input coordinates field is not an array of numbers: %s", raw.GetString());
	}

	auto x = yyjson_get_num(x_val);
	auto y = yyjson_get_num(y_val);

	auto geom_has_z = len > 2;
	if (geom_has_z) {
		has_z = true;
		auto z_val = yyjson_arr_get(coord_array, 2);
		if (!yyjson_is_num(z_val)) {
			throw InvalidInputException("GeoJSON input coordinates field is not an array of numbers: %s",
			                            raw.GetString());
		}
		auto z = yyjson_get_num(z_val);
		return Point::CreateFromVertex(arena, VertexXYZ {x, y, z});
	} else {
		return Point::CreateFromVertex(arena, VertexXY {x, y});
	}
}

static Geometry VerticesFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw, bool &has_z) {
	auto len = yyjson_arr_size(coord_array);
	if (len == 0) {
		// Empty
		return LineString::CreateEmpty(false, false);
	} else {
		// Sniff the coordinates to see if we have Z
		bool has_any_z = false;
		size_t idx, max;
		yyjson_val *coord;
		yyjson_arr_foreach(coord_array, idx, max, coord) {
			if (!yyjson_is_arr(coord)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			auto coord_len = yyjson_arr_size(coord);
			if (coord_len > 2) {
				has_any_z = true;
			} else if (coord_len < 2) {
				throw InvalidInputException(
				    "GeoJSON input coordinates field is not an array of arrays of length >= 2: %s", raw.GetString());
			}
		}

		if (has_any_z) {
			has_z = true;
		}

		auto vertices = LineString::Create(arena, len, has_any_z, false);

		yyjson_arr_foreach(coord_array, idx, max, coord) {
			auto coord_len = yyjson_arr_size(coord);
			auto x_val = yyjson_arr_get_first(coord);
			if (!yyjson_is_num(x_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays of numbers: %s",
				                            raw.GetString());
			}
			auto y_val = yyjson_arr_get(coord, 1);
			if (!yyjson_is_num(y_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays of numbers: %s",
				                            raw.GetString());
			}
			auto x = yyjson_get_num(x_val);
			auto y = yyjson_get_num(y_val);
			auto z = 0.0;

			if (coord_len > 2) {
				auto z_val = yyjson_arr_get(coord, 2);
				if (!yyjson_is_num(z_val)) {
					throw InvalidInputException(
					    "GeoJSON input coordinates field is not an array of arrays of numbers: %s", raw.GetString());
				}
				z = yyjson_get_num(z_val);
			}
			if (has_any_z) {
				LineString::SetVertex<VertexXYZ>(vertices, idx, {x, y, z});
			} else {
				LineString::SetVertex<VertexXY>(vertices, idx, {x, y});
			}
		}
		return vertices;
	}
}

static Geometry LineStringFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw,
                                      bool &has_z) {
	return VerticesFromGeoJSON(coord_array, arena, raw, has_z);
}

static Geometry PolygonFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw, bool &has_z) {
	auto num_rings = yyjson_arr_size(coord_array);
	if (num_rings == 0) {
		// Empty
		return Polygon::CreateEmpty(has_z, false);
	} else {
		// Polygon
		auto polygon = Polygon::Create(arena, num_rings, has_z, false);
		size_t idx, max;
		yyjson_val *ring_val;
		yyjson_arr_foreach(coord_array, idx, max, ring_val) {
			if (!yyjson_is_arr(ring_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			Polygon::Part(polygon, idx) = VerticesFromGeoJSON(ring_val, arena, raw, has_z);
		}

		return polygon;
	}
}

static Geometry MultiPointFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw,
                                      bool &has_z) {
	auto num_points = yyjson_arr_size(coord_array);
	if (num_points == 0) {
		// Empty
		return MultiPoint::CreateEmpty(has_z, false);
	} else {
		// MultiPoint
		auto multi_point = MultiPoint::Create(arena, num_points, has_z, false);
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
			MultiPoint::Part(multi_point, idx) = PointFromGeoJSON(point_val, arena, raw, has_z);
		}
		return multi_point;
	}
}

static Geometry MultiLineStringFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw,
                                           bool &has_z) {
	auto num_linestrings = yyjson_arr_size(coord_array);
	if (num_linestrings == 0) {
		// Empty
		return MultiLineString::CreateEmpty(has_z, false);
	} else {
		// MultiLineString
		auto multi_linestring = MultiLineString::Create(arena, num_linestrings, has_z, false);
		size_t idx, max;
		yyjson_val *linestring_val;
		yyjson_arr_foreach(coord_array, idx, max, linestring_val) {
			if (!yyjson_is_arr(linestring_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			MultiLineString::Part(multi_linestring, idx) = LineStringFromGeoJSON(linestring_val, arena, raw, has_z);
		}

		return multi_linestring;
	}
}

static Geometry MultiPolygonFromGeoJSON(yyjson_val *coord_array, ArenaAllocator &arena, const string_t &raw,
                                        bool &has_z) {
	auto num_polygons = yyjson_arr_size(coord_array);
	if (num_polygons == 0) {
		// Empty
		return MultiPolygon::CreateEmpty(has_z, false);
	} else {
		// MultiPolygon
		auto multi_polygon = MultiPolygon::Create(arena, num_polygons, has_z, false);
		size_t idx, max;
		yyjson_val *polygon_val;
		yyjson_arr_foreach(coord_array, idx, max, polygon_val) {
			if (!yyjson_is_arr(polygon_val)) {
				throw InvalidInputException("GeoJSON input coordinates field is not an array of arrays: %s",
				                            raw.GetString());
			}
			MultiPolygon::Part(multi_polygon, idx) = PolygonFromGeoJSON(polygon_val, arena, raw, has_z);
		}

		return multi_polygon;
	}
}

static Geometry FromGeoJSON(yyjson_val *root, ArenaAllocator &arena, const string_t &raw, bool &has_z);

static Geometry GeometryCollectionFromGeoJSON(yyjson_val *root, ArenaAllocator &arena, const string_t &raw,
                                              bool &has_z) {
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
		return GeometryCollection::CreateEmpty(has_z, false);
	} else {
		// GeometryCollection
		auto geometry_collection = GeometryCollection::Create(arena, num_geometries, has_z, false);
		size_t idx, max;
		yyjson_val *geometry_val;
		yyjson_arr_foreach(geometries_val, idx, max, geometry_val) {
			GeometryCollection::Part(geometry_collection, idx) = FromGeoJSON(geometry_val, arena, raw, has_z);
		}

		return geometry_collection;
	}
}

static Geometry FromGeoJSON(yyjson_val *root, ArenaAllocator &arena, const string_t &raw, bool &has_z) {
	auto type_val = yyjson_obj_get(root, "type");
	if (!type_val) {
		throw InvalidInputException("GeoJSON input does not have a type field: %s", raw.GetString());
	}
	auto type_str = yyjson_get_str(type_val);
	if (!type_str) {
		throw InvalidInputException("GeoJSON input type field is not a string: %s", raw.GetString());
	}

	if (StringUtil::Equals(type_str, "GeometryCollection")) {
		return GeometryCollectionFromGeoJSON(root, arena, raw, has_z);
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
		return PointFromGeoJSON(coord_array, arena, raw, has_z);
	} else if (StringUtil::Equals(type_str, "LineString")) {
		return LineStringFromGeoJSON(coord_array, arena, raw, has_z);
	} else if (StringUtil::Equals(type_str, "Polygon")) {
		return PolygonFromGeoJSON(coord_array, arena, raw, has_z);
	} else if (StringUtil::Equals(type_str, "MultiPoint")) {
		return MultiPointFromGeoJSON(coord_array, arena, raw, has_z);
	} else if (StringUtil::Equals(type_str, "MultiLineString")) {
		return MultiLineStringFromGeoJSON(coord_array, arena, raw, has_z);
	} else if (StringUtil::Equals(type_str, "MultiPolygon")) {
		return MultiPolygonFromGeoJSON(coord_array, arena, raw, has_z);
	} else {
		throw InvalidInputException("GeoJSON input has invalid type field: %s", raw.GetString());
	}
}

static void GeoJSONFragmentToGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	JSONAllocator json_allocator(lstate.arena);

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
			bool has_z = false;
			auto geom = FromGeoJSON(root, lstate.arena, input, has_z);
			if (has_z) {
				// Ensure the geometries has consistent Z values
				geom.SetVertexType(lstate.arena, has_z, false);
			}
			return Geometry::Serialize(geom, result);
		}
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

// AsGeoJSON
static constexpr const char *AS_DOC_DESCRIPTION = R"(
    Returns the geometry as a GeoJSON fragment

    This does not return a complete GeoJSON document, only the geometry fragment. To construct a complete GeoJSON document or feature, look into using the DuckDB JSON extension in conjunction with this function.
)";

static constexpr const char *AS_DOC_EXAMPLE = R"(
select ST_AsGeoJSON('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
----
{"type":"Polygon","coordinates":[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[1.0,0.0],[0.0,0.0]]]}
)";

// FromGeoJSON
static constexpr const char *FROM_DOC_DESCRIPTION = R"(
    Deserializes a GEOMETRY from a GeoJSON fragment.
)";

static constexpr const char *FROM_DOC_EXAMPLE = R"(

)";

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsGeoJSON(DatabaseInstance &db) {
	ScalarFunctionSet to_geojson("ST_AsGeoJSON");
	to_geojson.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::JSON(),
	                                      GeometryToGeoJSONFragmentFunction, nullptr, nullptr, nullptr,
	                                      GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, to_geojson);
	DocUtil::AddDocumentation(db, "ST_AsGeoJSON", AS_DOC_DESCRIPTION, AS_DOC_EXAMPLE, DOC_TAGS);

	ScalarFunctionSet from_geojson("ST_GeomFromGeoJSON");
	from_geojson.AddFunction(ScalarFunction({LogicalType::VARCHAR}, GeoTypes::GEOMETRY(),
	                                        GeoJSONFragmentToGeometryFunction, nullptr, nullptr, nullptr,
	                                        GeometryFunctionLocalState::Init));

	from_geojson.AddFunction(ScalarFunction({LogicalType::JSON()}, GeoTypes::GEOMETRY(),
	                                        GeoJSONFragmentToGeometryFunction, nullptr, nullptr, nullptr,
	                                        GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, from_geojson);
	DocUtil::AddDocumentation(db, "ST_GeomFromGeoJSON", FROM_DOC_DESCRIPTION, FROM_DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial