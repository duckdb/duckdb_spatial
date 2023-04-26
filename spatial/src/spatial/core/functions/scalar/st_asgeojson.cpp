#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"

#include "yyjson.h"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Operations
//------------------------------------------------------------------------------
static void VerticesToGeoJSON(const VertexVector &vertices, yyjson_mut_doc* doc, yyjson_mut_val* arr) {
    // TODO: If the vertexvector is empty, do we null, add an empty array or a pair of NaN?
    for (uint32_t i = 0; i < vertices.count; i++) {
        auto &vertex = vertices[i];
        auto coord = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_real(doc, coord, vertex.x);
        yyjson_mut_arr_add_real(doc, coord, vertex.y);
        yyjson_mut_arr_append(arr, coord);
    }
}

static void ToGeoJSON(const Point &point, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "Point");

    auto coords = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

    if(!point.IsEmpty()) {
        auto &vertex = point.GetVertex();
        yyjson_mut_arr_add_real(doc, coords, vertex.x);
        yyjson_mut_arr_add_real(doc, coords, vertex.y);
    }
}

static void ToGeoJSON(const LineString &line, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "LineString");

    auto coords = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
    VerticesToGeoJSON(line.points, doc, coords);
}

static void ToGeoJSON(const Polygon &poly, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "Polygon");

    auto coords = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
    for (uint32_t i = 0; i < poly.num_rings; i++) {
        auto &ring = poly.rings[i];
        auto ring_coords = yyjson_mut_arr(doc);
        VerticesToGeoJSON(ring, doc, ring_coords);
        yyjson_mut_arr_append(coords, ring_coords);
    }
}

static void ToGeoJSON(const MultiPoint &mpoint, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "MultiPoint");

    auto coords = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);
    for (uint32_t i = 0; i < mpoint.Count(); i++) {
        auto &point = mpoint.points[i];
        VerticesToGeoJSON(point.data, doc, coords);
    }
}

static void ToGeoJSON(const MultiLineString &mline, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "MultiLineString");
    
    auto coords = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

    for (uint32_t i = 0; i < mline.Count(); i++) {
        auto &line = mline.linestrings[i];
        auto line_coords = yyjson_mut_arr(doc);
        VerticesToGeoJSON(line.points, doc, line_coords);
        yyjson_mut_arr_append(coords, line_coords);
    }
}

static void ToGeoJSON(const MultiPolygon &mpoly, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "MultiPolygon");

    auto coords = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "coordinates", coords);

    for (uint32_t i = 0; i < mpoly.Count(); i++) {
        auto &poly = mpoly.polygons[i];
        auto poly_coords = yyjson_mut_arr(doc);
        for (uint32_t j = 0; j < poly.num_rings; j++) {
            auto &ring = poly.rings[j];
            auto ring_coords = yyjson_mut_arr(doc);
            VerticesToGeoJSON(ring, doc, ring_coords);
            yyjson_mut_arr_append(poly_coords, ring_coords);
        }
        yyjson_mut_arr_append(coords, poly_coords);
    }
}

static void ToGeoJSON(const Geometry &geom, yyjson_mut_doc* doc, yyjson_mut_val* obj);
static void ToGeoJSON(const GeometryCollection &collection, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    yyjson_mut_obj_add_str(doc, obj, "type", "GeometryCollection");
    auto arr = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, obj, "geometries", arr);

    for (idx_t i = 0; i < collection.num_geometries; i++) {
        auto &geom = collection.geometries[i];
        auto geom_obj = yyjson_mut_obj(doc);
        ToGeoJSON(geom, doc, geom_obj);
        yyjson_mut_arr_append(arr, geom_obj);
    }
}

static void ToGeoJSON(const Geometry &geom, yyjson_mut_doc* doc, yyjson_mut_val* obj) {
    switch (geom.Type()) {
        case GeometryType::POINT: ToGeoJSON(geom.GetPoint(), doc, obj); break;
        case GeometryType::LINESTRING: ToGeoJSON(geom.GetLineString(), doc, obj); break;
        case GeometryType::POLYGON: ToGeoJSON(geom.GetPolygon(), doc, obj);  break;
        case GeometryType::MULTIPOINT: ToGeoJSON(geom.GetMultiPoint(), doc, obj);  break;
        case GeometryType::MULTILINESTRING: ToGeoJSON(geom.GetMultiLineString(), doc, obj);  break;
        case GeometryType::MULTIPOLYGON: ToGeoJSON(geom.GetMultiPolygon(), doc, obj);  break;
        case GeometryType::GEOMETRYCOLLECTION: ToGeoJSON(geom.GetGeometryCollection(), doc, obj);  break;
        default: {
            throw NotImplementedException("Geometry type not supported");
        }
    }
}

//------------------------------------------------------------------------------
// GEOMETRY -> GEOJSON Fragment
//------------------------------------------------------------------------------
static void GeometryToGeoJSONFragmentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);

        auto doc = yyjson_mut_doc_new(nullptr);
        auto obj = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, obj);

        ToGeoJSON(geometry, doc, obj);

        size_t json_size = 0;
        // TODO: YYJSON_WRITE_PRETTY
        auto json_data = yyjson_mut_write(doc, 0, &json_size);
        auto json_str = StringVector::AddString(result, json_data, json_size);

        free((void*)json_data);
        yyjson_mut_doc_free(doc);
        return json_str;
	});
}

//------------------------------------------------------------------------------
// GEOMETRY -> GEOJSON Feature
//------------------------------------------------------------------------------
struct GeoJSONFeatureBindData : public FunctionData { 
	child_list_t<LogicalType> properties;
    GeoJSONFeatureBindData(child_list_t<LogicalType> properties) 
        : FunctionData(), properties(std::move(properties)) {
    }
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<GeoJSONFeatureBindData>(properties);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static unique_ptr<FunctionData> GeometryToGeoJSONFeatureBind(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
    if(arguments.size() != 2) {
        throw BinderException("GeometryToGeoJSONFeature takes exactly two arguments");
    }
    child_list_t<LogicalType> properties;
    if(arguments[1]->return_type.id() == LogicalTypeId::STRUCT) {
        properties = StructType::GetChildTypes(arguments[1]->return_type);  
    } else if(arguments[1]->return_type.id() == LogicalTypeId::SQLNULL) {
        // do nothing, properties is empty
    } else {
        throw BinderException("GeometryToGeoJSONFeature takes a struct as second argument");
    }
    return make_unique<GeoJSONFeatureBindData>(std::move(properties));
}


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

static void GeometryToGeoJSONFeatureFunction(DataChunk &args, ExpressionState &state, Vector &result) {

    auto count = args.size();
    auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
    auto &func_expr = (BoundFunctionExpression&)state.expr;
    auto &bind_data = (GeoJSONFeatureBindData&)*func_expr.bind_info;

    auto &geom_vec = args.data[0];
    auto &prop_vec = args.data[1];
    geom_vec.Flatten(count);
    prop_vec.Flatten(count);

    bool has_properties = !bind_data.properties.empty();
    
    auto json_docs = (yyjson_mut_doc**)lstate.factory.allocator.AllocateAligned(count * sizeof(yyjson_mut_doc*));
    auto json_objs = (yyjson_mut_val**)lstate.factory.allocator.AllocateAligned(count * sizeof(yyjson_mut_val*));
    auto json_props = (yyjson_mut_val**)lstate.factory.allocator.AllocateAligned(count * sizeof(yyjson_mut_val*));

    JSONAllocator json_allocator(lstate.factory.allocator);
    
    for(idx_t row_idx = 0; row_idx < count; row_idx++) {
        auto geom_blob = FlatVector::GetData<string_t>(geom_vec)[row_idx];
        auto geom = lstate.factory.Deserialize(geom_blob);

        auto doc = yyjson_mut_doc_new(json_allocator.GetYYJSONAllocator());
        auto obj = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, obj);
        json_docs[row_idx] = doc;
        json_objs[row_idx] = obj;


        yyjson_mut_obj_add_str(doc, obj, "type", "Feature");
        auto geom_obj = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_val(doc, obj, "geometry", geom_obj);
        ToGeoJSON(geom, doc, geom_obj);

        if(has_properties) {
            auto prop_obj = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_val(doc, obj, "properties", prop_obj);
            json_props[row_idx] = prop_obj;
        } else {
            yyjson_mut_obj_add_null(doc, obj, "properties");
            json_props[row_idx] = nullptr;
        }
    }
    
    child_list_t<LogicalType> schema;
    if(has_properties) {
        auto &prop_vecs = StructVector::GetEntries(prop_vec);
        schema = StructType::GetChildTypes(prop_vec.GetType());
        for(idx_t property_idx = 0; property_idx < prop_vecs.size(); property_idx++) {
            auto &prop_vec = prop_vecs[property_idx];
            auto &prop_name = schema[property_idx].first;
            auto &prop_type = schema[property_idx].second;

            for(idx_t row_idx = 0; row_idx < count; row_idx++) {
                auto &prop_obj = json_props[row_idx];
                yyjson_mut_obj_add_int(json_docs[row_idx], prop_obj, prop_name.c_str(), FlatVector::GetData<int64_t>(*prop_vec)[row_idx]);
            }
        }
    }

    for(idx_t row_idx = 0; row_idx < count; row_idx++) {
        size_t json_size = 0;
        // TODO: YYJSON_WRITE_PRETTY
        auto json_data = yyjson_mut_write(json_docs[row_idx], 0, &json_size);
        FlatVector::GetData<string_t>(result)[row_idx] = StringVector::AddString(result, json_data, json_size);
    }

    if(count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsGeoJSON(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

    ScalarFunctionSet geojson("ST_AsGeoJSON");
    geojson.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::VARCHAR, GeometryToGeoJSONFragmentFunction, 
        nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));
    geojson.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalTypeId::ANY}, LogicalType::VARCHAR, GeometryToGeoJSONFeatureFunction, 
        GeometryToGeoJSONFeatureBind, nullptr, nullptr, GeometryFunctionLocalState::Init, LogicalTypeId::INVALID, FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));

	CreateScalarFunctionInfo info(geojson);
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace core

} // namespace spatial