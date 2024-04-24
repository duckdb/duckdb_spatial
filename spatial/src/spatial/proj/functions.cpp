#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/proj/functions.hpp"
#include "spatial/proj/module.hpp"

#include "proj.h"

namespace spatial {

namespace proj {

using namespace core;

struct ProjFunctionLocalState : public FunctionLocalState {

	PJ_CONTEXT *proj_ctx;
	ArenaAllocator arena;

	explicit ProjFunctionLocalState(ClientContext &context)
	    : proj_ctx(ProjModule::GetThreadProjContext()), arena(BufferAllocator::Get(context)) {
	}

	~ProjFunctionLocalState() override {
		proj_context_destroy(proj_ctx);
	}

	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		auto result = make_uniq<ProjFunctionLocalState>(state.GetContext());
		return std::move(result);
	}

	static ProjFunctionLocalState &ResetAndGet(ExpressionState &state) {
		auto &local_state = (ProjFunctionLocalState &)*ExecuteFunctionState::GetFunctionState(state);
		local_state.arena.Reset();
		return local_state;
	}
};

struct TransformFunctionData : FunctionData {

	// Whether or not to always return XY coordinates, even when the CRS has a different axis order.
	bool conventional_gis_order = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<TransformFunctionData>();
		result->conventional_gis_order = conventional_gis_order;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &data = other.Cast<TransformFunctionData>();
		return conventional_gis_order == data.conventional_gis_order;
	}
};

static unique_ptr<FunctionData> TransformBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {

	auto result = make_uniq<TransformFunctionData>();
	if (arguments.size() == 4) {
		// Ensure the "always_xy" parameter is a constant
		auto &arg = arguments[3];
		if (arg->HasParameter()) {
			throw InvalidInputException("The 'always_xy' parameter must be a constant");
		}
		if (!arg->IsFoldable()) {
			throw InvalidInputException("The 'always_xy' parameter must be a constant");
		}
		result->conventional_gis_order = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
	}
	return std::move(result);
}

static void Box2DTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using PROJ_TYPE = PrimitiveType<string_t>;

	auto count = args.size();
	auto &box = args.data[0];
	auto &proj_from = args.data[1];
	auto &proj_to = args.data[2];

	auto &local_state = ProjFunctionLocalState::ResetAndGet(state);
	auto &proj_ctx = local_state.proj_ctx;
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<TransformFunctionData>();

	if (proj_from.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    proj_to.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(proj_from) &&
	    !ConstantVector::IsNull(proj_to)) {
		// Special case: both projections are constant, so we can create the projection once and reuse it
		auto from_str = ConstantVector::GetData<PROJ_TYPE>(proj_from)[0].val.GetString();
		auto to_str = ConstantVector::GetData<PROJ_TYPE>(proj_to)[0].val.GetString();

		auto crs = proj_create_crs_to_crs(proj_ctx, from_str.c_str(), to_str.c_str(), nullptr);
		if (!crs) {
			throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
		}

		if (info.conventional_gis_order) {
			auto normalized_crs = proj_normalize_for_visualization(proj_ctx, crs);
			if (normalized_crs) {
				proj_destroy(crs);
				crs = normalized_crs;
			}
			// otherwise fall back to the original CRS
		}

		GenericExecutor::ExecuteUnary<BOX_TYPE, BOX_TYPE>(box, result, count, [&](BOX_TYPE box_in) {
			BOX_TYPE box_out;
			int densify_pts = 0;
			proj_trans_bounds(proj_ctx, crs, PJ_FWD, box_in.a_val, box_in.b_val, box_in.c_val, box_in.d_val,
			                  &box_out.a_val, &box_out.b_val, &box_out.c_val, &box_out.d_val, densify_pts);
			return box_out;
		});

		proj_destroy(crs);
	} else {
		GenericExecutor::ExecuteTernary<BOX_TYPE, PROJ_TYPE, PROJ_TYPE, BOX_TYPE>(
		    box, proj_from, proj_to, result, count, [&](BOX_TYPE box_in, PROJ_TYPE proj_from, PROJ_TYPE proj_to) {
			    auto from_str = proj_from.val.GetString();
			    auto to_str = proj_to.val.GetString();

			    auto crs = proj_create_crs_to_crs(nullptr, from_str.c_str(), to_str.c_str(), nullptr);
			    if (!crs) {
				    throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
			    }

			    if (info.conventional_gis_order) {
				    auto normalized_crs = proj_normalize_for_visualization(proj_ctx, crs);
				    if (normalized_crs) {
					    proj_destroy(crs);
					    crs = normalized_crs;
				    }
				    // otherwise fall back to the original CRS
			    }

			    // TODO: this may be interesting to use, but at that point we can only return a BOX_TYPE
			    int densify_pts = 0;
			    BOX_TYPE box_out;
			    proj_trans_bounds(proj_ctx, crs, PJ_FWD, box_in.a_val, box_in.b_val, box_in.c_val, box_in.d_val,
			                      &box_out.a_val, &box_out.b_val, &box_out.c_val, &box_out.d_val, densify_pts);

			    proj_destroy(crs);

			    return box_out;
		    });
	}
}

static void Point2DTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using PROJ_TYPE = PrimitiveType<string_t>;

	auto count = args.size();
	auto &point = args.data[0];
	auto &proj_from = args.data[1];
	auto &proj_to = args.data[2];

	auto &local_state = ProjFunctionLocalState::ResetAndGet(state);
	auto &proj_ctx = local_state.proj_ctx;
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<TransformFunctionData>();

	if (proj_from.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    proj_to.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(proj_from) &&
	    !ConstantVector::IsNull(proj_to)) {
		// Special case: both projections are constant, so we can create the projection once and reuse it
		auto from_str = ConstantVector::GetData<PROJ_TYPE>(proj_from)[0].val.GetString();
		auto to_str = ConstantVector::GetData<PROJ_TYPE>(proj_to)[0].val.GetString();

		auto crs = proj_create_crs_to_crs(proj_ctx, from_str.c_str(), to_str.c_str(), nullptr);
		if (!crs) {
			throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
		}

		if (info.conventional_gis_order) {
			auto normalized_crs = proj_normalize_for_visualization(proj_ctx, crs);
			if (normalized_crs) {
				proj_destroy(crs);
				crs = normalized_crs;
			}
			// otherwise fall back to the original CRS
		}

		GenericExecutor::ExecuteUnary<POINT_TYPE, POINT_TYPE>(point, result, count, [&](POINT_TYPE point_in) {
			POINT_TYPE point_out;
			auto transformed = proj_trans(crs, PJ_FWD, proj_coord(point_in.a_val, point_in.b_val, 0, 0)).xy;
			point_out.a_val = transformed.x;
			point_out.b_val = transformed.y;
			return point_out;
		});
		proj_destroy(crs);

	} else {
		GenericExecutor::ExecuteTernary<POINT_TYPE, PROJ_TYPE, PROJ_TYPE, POINT_TYPE>(
		    point, proj_from, proj_to, result, count, [&](POINT_TYPE point_in, PROJ_TYPE proj_from, PROJ_TYPE proj_to) {
			    auto from_str = proj_from.val.GetString();
			    auto to_str = proj_to.val.GetString();

			    auto crs = proj_create_crs_to_crs(proj_ctx, from_str.c_str(), to_str.c_str(), nullptr);
			    if (!crs) {
				    throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
			    }

			    if (info.conventional_gis_order) {
				    auto normalized_crs = proj_normalize_for_visualization(proj_ctx, crs);
				    if (normalized_crs) {
					    proj_destroy(crs);
					    crs = normalized_crs;
				    }
				    // otherwise fall back to the original CRS
			    }

			    POINT_TYPE point_out;
			    auto transformed = proj_trans(crs, PJ_FWD, proj_coord(point_in.a_val, point_in.b_val, 0, 0)).xy;
			    point_out.a_val = transformed.x;
			    point_out.b_val = transformed.y;

			    proj_destroy(crs);

			    return point_out;
		    });
	}
}

struct TransformOp {
	static void Case(Geometry::Tags::SinglePartGeometry, Geometry &geom, PJ *crs, ArenaAllocator &arena) {
		SinglePartGeometry::MakeMutable(geom, arena);
		for (uint32_t i = 0; i < geom.Count(); i++) {
			auto vertex = SinglePartGeometry::GetVertex(geom, i);
			auto transformed = proj_trans(crs, PJ_FWD, proj_coord(vertex.x, vertex.y, 0, 0)).xy;
			// we own the array, so we can use SetUnsafe
			SinglePartGeometry::SetVertex(geom, i, {transformed.x, transformed.y});
		}
	}
	static void Case(Geometry::Tags::MultiPartGeometry, Geometry &geom, PJ *crs, ArenaAllocator &arena) {
		for (auto &part : MultiPartGeometry::Parts(geom)) {
			Geometry::Match<TransformOp>(part, crs, arena);
		}
	}
};

struct ProjCRSDelete {
	void operator()(PJ *crs) {
		proj_destroy(crs);
	}
};

using ProjCRS = unique_ptr<PJ, ProjCRSDelete>;

static void GeometryTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &geom_vec = args.data[0];
	auto &proj_from_vec = args.data[1];
	auto &proj_to_vec = args.data[2];

	auto &local_state = ProjFunctionLocalState::ResetAndGet(state);

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<TransformFunctionData>();

	auto &proj_ctx = local_state.proj_ctx;
	auto &arena = local_state.arena;

	if (proj_from_vec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    proj_to_vec.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(proj_from_vec) &&
	    !ConstantVector::IsNull(proj_to_vec)) {
		// Special case: both projections are constant (very common)
		// we can create the projection once and reuse it for all geometries

		// TODO: In the future we can cache the projections in the state instead.

		auto from_str = ConstantVector::GetData<string_t>(proj_from_vec)[0].GetString();
		auto to_str = ConstantVector::GetData<string_t>(proj_to_vec)[0].GetString();
		auto crs = ProjCRS(proj_create_crs_to_crs(proj_ctx, from_str.c_str(), to_str.c_str(), nullptr));
		if (!crs.get()) {
			throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
		}

		if (info.conventional_gis_order) {
			auto normalized_crs = proj_normalize_for_visualization(proj_ctx, crs.get());
			if (normalized_crs) {
				crs = ProjCRS(normalized_crs);
			}
			// otherwise fall back to the original CRS
		}

		UnaryExecutor::Execute<geometry_t, geometry_t>(geom_vec, result, count, [&](geometry_t input_geom) {
			auto geom = Geometry::Deserialize(arena, input_geom);
			Geometry::Match<TransformOp>(geom, crs.get(), arena);
			return Geometry::Serialize(geom, result);
		});
	} else {
		// General case: projections are not constant
		// we need to create a projection for each geometry
		TernaryExecutor::Execute<geometry_t, string_t, string_t, geometry_t>(
		    geom_vec, proj_from_vec, proj_to_vec, result, count,
		    [&](geometry_t input_geom, string_t proj_from, string_t proj_to) {
			    auto from_str = proj_from.GetString();
			    auto to_str = proj_to.GetString();
			    auto crs = ProjCRS(proj_create_crs_to_crs(proj_ctx, from_str.c_str(), to_str.c_str(), nullptr));

			    if (!crs.get()) {
				    throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
			    }

			    if (info.conventional_gis_order) {
				    auto normalized_crs = proj_normalize_for_visualization(proj_ctx, crs.get());
				    if (normalized_crs) {
					    crs = ProjCRS(normalized_crs);
				    }
				    // otherwise fall back to the original CRS
			    }

			    auto geom = Geometry::Deserialize(arena, input_geom);
			    Geometry::Match<TransformOp>(geom, crs.get(), arena);
			    return Geometry::Serialize(geom, result);
		    });
	}
}

// SPATIAL_REF_SYS table function
struct GenerateSpatialRefSysTable {

	struct State : public GlobalTableFunctionState {
		idx_t current_idx;
		State() : current_idx(0) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

	static void Register(DatabaseInstance &db);
};

unique_ptr<FunctionData> GenerateSpatialRefSysTable::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {

	names.push_back("auth_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("code");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("type");
	return_types.push_back(LogicalType::VARCHAR); // TODO: this should maybe be an enum?
	names.push_back("deprecated");
	return_types.push_back(LogicalType::BOOLEAN);

	// TODO: output BBOX here as well as BOX_2D (or null!)

	names.push_back("area_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("projection_method_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("celestial_body_name");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> GenerateSpatialRefSysTable::Init(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto result = make_uniq<State>();
	return std::move(result);
}

void GenerateSpatialRefSysTable::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	// TODO: This is a lot slower than it has to be, ideally we only do one call to proj_get_crs_info_list
	// and return the whole list in one go.
	auto &state = (State &)*input.global_state;
	int result_count = 0;
	auto crs_list = proj_get_crs_info_list_from_database(nullptr, nullptr, nullptr, &result_count);

	idx_t count = 0;
	auto next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, result_count);

	// TODO: this just returns the crs info, not a spatial_ref_sys table that follows the schema.
	for (idx_t i = state.current_idx; i < next_idx; i++) {
		auto proj = crs_list[i];
		output.SetValue(0, count, Value(proj->auth_name));
		output.SetValue(1, count, Value(proj->code));
		output.SetValue(2, count, Value(proj->name));
		output.SetValue(3, count, Value(proj->type));
		output.SetValue(4, count, Value(proj->deprecated));
		output.SetValue(5, count, Value(proj->area_name));
		output.SetValue(6, count, Value(proj->projection_method_name));
		output.SetValue(7, count, Value(proj->celestial_body_name));
		count++;
	}

	proj_crs_info_list_destroy(crs_list);

	state.current_idx += count;
	output.SetCardinality(count);
}

void GenerateSpatialRefSysTable::Register(DatabaseInstance &db) {
	TableFunction func("ST_List_Proj_CRS", {}, Execute, Bind, Init);
	ExtensionUtil::RegisterFunction(db, func);

	// Also create a view
	/*
	auto view = make_uniq<CreateViewInfo>();
	view->schema = DEFAULT_SCHEMA;
	view->view_name = "SPATIAL_REF_SYS";
	view->sql = "SELECT * FROM st_list_proj_crs()"; // TODO: this is not SQL/MM compliant
	view->temporary = true;
	view->internal = true;
	CreateViewInfo::FromSelect(context, std::move(view));
	catalog.CreateView(context, view.get());
	*/
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
Transforms a geometry between two coordinate systems

The source and target coordinate systems can be specified using any format that the [PROJ library](https://proj.org) supports.

The optional `always_xy` parameter can be used to force the input and output geometries to be interpreted as having a [northing, easting] coordinate axis order regardless of what the source and target coordinate system definition says. This is particularly useful when transforming to/from the [WGS84/EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (what most people think of when they hear "longitude"/"latitude" or "GPS coordinates"), which is defined as having a [latitude, longitude] axis order even though [longitude, latitude] is commonly used in practice (e.g. in [GeoJSON](https://tools.ietf.org/html/rfc7946)). More details available in the [PROJ documentation](https://proj.org/en/9.3/faq.html#why-is-the-axis-ordering-in-proj-not-consistent).

DuckDB spatial vendors its own static copy of the PROJ database of coordinate systems, so if you have your own installation of PROJ on your system the available coordinate systems may differ to what's available in other GIS software.
)";

static constexpr const char *DOC_EXAMPLE = R"(
-- Transform a geometry from EPSG:4326 to EPSG:3857 (WGS84 to WebMercator)
-- Note that since WGS84 is defined as having a [latitude, longitude] axis order
-- we follow the standard and provide the input geometry using that axis order,
-- but the output will be [northing, easting] because that is what's defined by
-- WebMercator.

SELECT ST_AsText(
    ST_Transform(
        st_point(52.373123, 4.892360),
        'EPSG:4326',
        'EPSG:3857'
    )
);
----
POINT (544615.0239773799 6867874.103539125)

-- Alternatively, let's say we got our input point from e.g. a GeoJSON file,
-- which uses WGS84 but with [longitude, latitude] axis order. We can use the
-- `always_xy` parameter to force the input geometry to be interpreted as having
-- a [northing, easting] axis order instead, even though the source coordinate
-- system definition says otherwise.

SELECT ST_AsText(
    ST_Transform(
        -- note the axis order is reversed here
        st_point(4.892360, 52.373123),
        'EPSG:4326',
        'EPSG:3857',
        always_xy := true
    )
);
----
POINT (544615.0239773799 6867874.103539125)
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void ProjFunctions::Register(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Transform");

	using namespace spatial::core;

	set.AddFunction(ScalarFunction({GeoTypes::BOX_2D(), LogicalType::VARCHAR, LogicalType::VARCHAR}, GeoTypes::BOX_2D(),
	                               Box2DTransformFunction, TransformBind, nullptr, nullptr,
	                               ProjFunctionLocalState::Init));
	set.AddFunction(ScalarFunction(
	    {GeoTypes::BOX_2D(), LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN}, GeoTypes::BOX_2D(),
	    Box2DTransformFunction, TransformBind, nullptr, nullptr, ProjFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::POINT_2D(), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               GeoTypes::POINT_2D(), Point2DTransformFunction, TransformBind, nullptr, nullptr,
	                               ProjFunctionLocalState::Init));
	set.AddFunction(ScalarFunction(
	    {GeoTypes::POINT_2D(), LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN}, GeoTypes::POINT_2D(),
	    Point2DTransformFunction, TransformBind, nullptr, nullptr, ProjFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               GeoTypes::GEOMETRY(), GeometryTransformFunction, TransformBind, nullptr, nullptr,
	                               ProjFunctionLocalState::Init));
	set.AddFunction(ScalarFunction(
	    {GeoTypes::GEOMETRY(), LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN}, GeoTypes::GEOMETRY(),
	    GeometryTransformFunction, TransformBind, nullptr, nullptr, ProjFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Transform", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);

	GenerateSpatialRefSysTable::Register(db);
}

} // namespace proj

} // namespace spatial