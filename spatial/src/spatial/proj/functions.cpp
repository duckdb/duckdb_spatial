#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/proj/functions.hpp"
#include "spatial/proj/module.hpp"

#include "proj.h"

namespace spatial {

namespace proj {

struct ProjFunctionLocalState : public FunctionLocalState {

	PJ_CONTEXT *proj_ctx;
	core::GeometryFactory factory;

	ProjFunctionLocalState(ClientContext &context)
	    : proj_ctx(ProjModule::GetThreadProjContext()), factory(BufferAllocator::Get(context)) {
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
		local_state.factory.allocator.Reset();
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

static void TransformGeometry(PJ *crs, core::Point &point) {
	if (point.IsEmpty()) {
		return;
	}
	auto vertex = point.GetVertex();
	auto transformed = proj_trans(crs, PJ_FWD, proj_coord(vertex.x, vertex.y, 0, 0)).xy;
	point.Vertices().Set(0, core::Vertex(transformed.x, transformed.y));
}

static void TransformGeometry(PJ *crs, core::LineString &line) {

	for (uint32_t i = 0; i < line.Vertices().Count(); i++) {
		auto vert = line.Vertices().Get(i);
		auto transformed = proj_trans(crs, PJ_FWD, proj_coord(vert.x, vert.y, 0, 0)).xy;

		core::Vertex new_vert(transformed.x, transformed.y);
		line.Vertices().Set(i, new_vert);
	}
}

static void TransformGeometry(PJ *crs, core::Polygon &poly) {
	for (auto &ring : poly.Rings()) {
		for (uint32_t i = 0; i < ring.Count(); i++) {
			auto vert = ring.Get(i);
			auto transformed = proj_trans(crs, PJ_FWD, proj_coord(vert.x, vert.y, 0, 0)).xy;
			core::Vertex new_vert(transformed.x, transformed.y);
			ring.Set(i, new_vert);
		}
	}
}

static void TransformGeometry(PJ *crs, core::MultiPoint &multi_point) {
	for (auto &point : multi_point) {
		TransformGeometry(crs, point);
	}
}

static void TransformGeometry(PJ *crs, core::MultiLineString &multi_line) {
	for (auto &line : multi_line) {
		TransformGeometry(crs, line);
	}
}

static void TransformGeometry(PJ *crs, core::MultiPolygon &multi_poly) {
	for (auto &poly : multi_poly) {
		TransformGeometry(crs, poly);
	}
}

static void TransformGeometry(PJ *crs, core::Geometry &geom);

static void TransformGeometry(PJ *crs, core::GeometryCollection &geom) {
	for (auto &child : geom) {
		TransformGeometry(crs, child);
	}
}

static void TransformGeometry(PJ *crs, core::Geometry &geom) {
	switch (geom.Type()) {
	case core::GeometryType::POINT:
		TransformGeometry(crs, geom.GetPoint());
		break;
	case core::GeometryType::LINESTRING:
		TransformGeometry(crs, geom.GetLineString());
		break;
	case core::GeometryType::POLYGON:
		TransformGeometry(crs, geom.GetPolygon());
		break;
	case core::GeometryType::MULTIPOINT:
		TransformGeometry(crs, geom.GetMultiPoint());
		break;
	case core::GeometryType::MULTILINESTRING:
		TransformGeometry(crs, geom.GetMultiLineString());
		break;
	case core::GeometryType::MULTIPOLYGON:
		TransformGeometry(crs, geom.GetMultiPolygon());
		break;
	case core::GeometryType::GEOMETRYCOLLECTION:
		TransformGeometry(crs, geom.GetGeometryCollection());
		break;
	default:
		throw NotImplementedException("Unimplemented geometry type!");
	}
}

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
	auto &factory = local_state.factory;

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

		UnaryExecutor::Execute<core::geometry_t, core::geometry_t>(geom_vec, result, count,
		                                                           [&](core::geometry_t input_geom) {
			                                                           auto geom = factory.Deserialize(input_geom);
			                                                           auto copy = factory.CopyGeometry(geom);
			                                                           TransformGeometry(crs.get(), copy);
			                                                           return factory.Serialize(result, copy);
		                                                           });
	} else {
		// General case: projections are not constant
		// we need to create a projection for each geometry
		TernaryExecutor::Execute<core::geometry_t, string_t, string_t, core::geometry_t>(
		    geom_vec, proj_from_vec, proj_to_vec, result, count,
		    [&](core::geometry_t input_geom, string_t proj_from, string_t proj_to) {
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

			    auto geom = factory.Deserialize(input_geom);
			    auto copy = factory.CopyGeometry(geom);
			    TransformGeometry(crs.get(), copy);
			    return factory.Serialize(result, copy);
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

	GenerateSpatialRefSysTable::Register(db);
}

} // namespace proj

} // namespace spatial