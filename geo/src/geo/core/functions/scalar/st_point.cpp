#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "geo/common.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_factory.hpp"
#include "geo/core/types.hpp"

namespace geo {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void Point2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];

	x.Flatten(count);
	y.Flatten(count);

	auto &children = StructVector::GetEntries(result);
	auto &x_child = children[0];
	auto &y_child = children[1];

	x_child->Reference(x);
	y_child->Reference(y);

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POINT_3D
//------------------------------------------------------------------------------
static void Point3DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];

	x.Flatten(count);
	y.Flatten(count);
	z.Flatten(count);

	auto &children = StructVector::GetEntries(result);
	auto &x_child = children[0];
	auto &y_child = children[1];
	auto &z_child = children[2];

	x_child->Reference(x);
	y_child->Reference(y);
	z_child->Reference(z);

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POINT_4D
//------------------------------------------------------------------------------
static void Point4DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 4);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];
	auto &m = args.data[3];

	x.Flatten(count);
	y.Flatten(count);
	z.Flatten(count);
	m.Flatten(count);

	auto &children = StructVector::GetEntries(result);
	auto &x_child = children[0];
	auto &y_child = children[1];
	auto &z_child = children[2];
	auto &m_child = children[3];

	x_child->Reference(x);
	y_child->Reference(y);
	z_child->Reference(z);
	m_child->Reference(m);

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void PointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc, 1024);
	GeometryFactory ctx(allocator);

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<double, double, string_t>(x, y, result, count, [&](double x, double y) {
		allocator.Reset();
		auto point = ctx.CreatePoint(x, y);
		return ctx.Serialize(result, Geometry(std::move(point)));
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStPoint(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	CreateScalarFunctionInfo st_point_info(ScalarFunction("st_point", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::GEOMETRY, PointFunction));
	catalog.CreateFunction(context, &st_point_info);

	// Non-standard
	CreateScalarFunctionInfo st_point_2d_info(ScalarFunction("st_point_2d", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::POINT_2D, Point2DFunction));
	catalog.CreateFunction(context, &st_point_2d_info);

	CreateScalarFunctionInfo st_point_3d_info(ScalarFunction("st_point_3d", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::POINT_3D, Point3DFunction));
	catalog.CreateFunction(context, &st_point_3d_info);

	CreateScalarFunctionInfo st_point_4d_info(ScalarFunction("st_point_4d", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::POINT_4D, Point4DFunction));
	catalog.CreateFunction(context, &st_point_4d_info);
}

} // namespace core

} // namespace geo