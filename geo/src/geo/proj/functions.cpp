#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/proj/functions.hpp"
#include "geo/proj/module.hpp"

#include "proj.h"

namespace geo {

namespace proj {

using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
using POINT_TYPE = StructTypeBinary<double, double>;
using PROJ_TYPE = PrimitiveType<string_t>;

static void Box2DTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &box = args.data[0];
	auto &proj_from = args.data[1];
	auto &proj_to = args.data[2];

	auto proj_ctx = ProjModule::GetThreadProjContext();

	GenericExecutor::ExecuteTernary<BOX_TYPE, PROJ_TYPE, PROJ_TYPE, BOX_TYPE>(
	    box, proj_from, proj_to, result, count, [&](BOX_TYPE box_in, PROJ_TYPE proj_from, PROJ_TYPE proj_to) {
		    auto from_str = proj_from.val.GetString();
		    auto to_str = proj_to.val.GetString();

		    auto crs = proj_create_crs_to_crs(nullptr, from_str.c_str(), to_str.c_str(), nullptr);
		    if (!crs) {
			    throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
		    }

		    // TODO: this may be interesting to use, but at that point we can only return a BOX_TYPE
		    int densify_pts = 0;
		    BOX_TYPE box_out;
		    proj_trans_bounds(proj_ctx, crs, PJ_FWD, box_in.a_val, box_in.b_val, box_in.c_val, box_in.d_val,
		                      &box_out.a_val, &box_out.b_val, &box_out.c_val, &box_out.d_val, densify_pts);

		    proj_destroy(crs);

		    return box_out;
	    });

	proj_context_destroy(proj_ctx);
}

static void Point2DTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &point = args.data[0];
	auto &proj_from = args.data[1];
	auto &proj_to = args.data[2];

	auto ctx = ProjModule::GetThreadProjContext();

	GenericExecutor::ExecuteTernary<POINT_TYPE, PROJ_TYPE, PROJ_TYPE, POINT_TYPE>(
	    point, proj_from, proj_to, result, count, [&](POINT_TYPE point_in, PROJ_TYPE proj_from, PROJ_TYPE proj_to) {
		    auto from_str = proj_from.val.GetString();
		    auto to_str = proj_to.val.GetString();

		    auto crs = proj_create_crs_to_crs(ctx, from_str.c_str(), to_str.c_str(), nullptr);
		    if (!crs) {
			    throw InvalidInputException("Could not create projection: " + from_str + " -> " + to_str);
		    }

		    POINT_TYPE point_out;
		    auto transformed = proj_trans(crs, PJ_FWD, proj_coord(point_in.a_val, point_in.b_val, 0, 0)).xy;
		    point_out.a_val = transformed.x;
		    point_out.b_val = transformed.y;

		    proj_destroy(crs);

		    return point_out;
	    });

	proj_context_destroy(ctx);
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

	static void Register(ClientContext &context);
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
	return make_unique<State>();
}

void GenerateSpatialRefSysTable::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (State &)*input.global_state;
	int result_count = 0;
	auto crs_list = proj_get_crs_info_list_from_database(nullptr, nullptr, nullptr, &result_count);

	idx_t count = 0;
	auto next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, result_count);
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
	output.SetCardinality(count);
}

void GenerateSpatialRefSysTable::Register(ClientContext &context) {
	TableFunction func("st_list_proj_crs", {}, Execute, Bind, Init);
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo info(func);
	catalog.CreateTableFunction(context, &info);

	// Also create a view
	/*
	auto view = make_unique<CreateViewInfo>();
	view->schema = DEFAULT_SCHEMA;
	view->view_name = "SPATIAL_REF_SYS";
	view->sql = "SELECT * FROM st_list_proj_crs()"; // TODO: this is not SQL/MM compliant
	view->temporary = true;
	view->internal = true;
	CreateViewInfo::FromSelect(context, std::move(view));
	catalog.CreateView(context, view.get());
	*/
}

void ProjFunctions::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("st_transform");

	set.AddFunction(ScalarFunction({geo::core::GeoTypes::BOX_2D, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               geo::core::GeoTypes::BOX_2D, Box2DTransformFunction));
	set.AddFunction(ScalarFunction({geo::core::GeoTypes::POINT_2D, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               geo::core::GeoTypes::POINT_2D, Point2DTransformFunction));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);

	GenerateSpatialRefSysTable::Register(context);
}

} // namespace proj

} // namespace geo