#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

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

	if (count == 1) {
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

	if (count == 1) {
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

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void PointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto &x = args.data[0];
	auto &y = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<double, double, geometry_t>(x, y, result, count, [&](double x, double y) {
		return Geometry::Serialize(Point::CreateFromVertex(arena, VertexXY {x, y}), result);
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

void CoreScalarFunctions::RegisterStPoint(DatabaseInstance &db) {

	ScalarFunction st_point("ST_Point", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), PointFunction,
	                        nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init);

	ExtensionUtil::RegisterFunction(db, st_point);
	auto POINT_DOC_DESCRIPTION = "Creates a GEOMETRY point";
	DocUtil::AddDocumentation(db, "ST_Point", POINT_DOC_DESCRIPTION, nullptr, DOC_TAGS);

	// Non-standard
	ScalarFunction st_point2d("ST_Point2D", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GeoTypes::POINT_2D(),
	                          Point2DFunction);
	ExtensionUtil::RegisterFunction(db, st_point2d);
	auto POINT2D_DOC_DESCRIPTION = "Creates a POINT_2D";
	DocUtil::AddDocumentation(db, "ST_Point2D", POINT2D_DOC_DESCRIPTION, nullptr, DOC_TAGS);

	ScalarFunction st_point_3d("ST_Point3D", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                           GeoTypes::POINT_3D(), Point3DFunction);
	ExtensionUtil::RegisterFunction(db, st_point_3d);
	auto POINT3D_DOC_DESCRIPTION = "Creates a POINT_3D";
	DocUtil::AddDocumentation(db, "ST_Point3D", POINT3D_DOC_DESCRIPTION, nullptr, DOC_TAGS);

	ScalarFunction st_point_4d("ST_Point4D",
	                           {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                           GeoTypes::POINT_4D(), Point4DFunction);

	ExtensionUtil::RegisterFunction(db, st_point_4d);
	auto POINT4D_DOC_DESCRIPTION = "Creates a POINT_4D";
	DocUtil::AddDocumentation(db, "ST_Point4D", POINT4D_DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

} // namespace core

} // namespace spatial