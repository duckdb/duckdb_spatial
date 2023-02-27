#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/proj/functions.hpp"

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

	auto proj_ctx = proj_context_create();

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

	GenericExecutor::ExecuteTernary<POINT_TYPE, PROJ_TYPE, PROJ_TYPE, POINT_TYPE>(
	    point, proj_from, proj_to, result, count, [&](POINT_TYPE point_in, PROJ_TYPE proj_from, PROJ_TYPE proj_to) {
		    auto from_str = proj_from.val.GetString();
		    auto to_str = proj_to.val.GetString();

		    auto crs = proj_create_crs_to_crs(nullptr, from_str.c_str(), to_str.c_str(), nullptr);
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
}

} // namespace proj

} // namespace geo