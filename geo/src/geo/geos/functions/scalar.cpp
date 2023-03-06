#pragma once
#include "geo/common.hpp"
#include "geo/gdal/types.hpp"
#include "geo/geos/functions/scalar.hpp"
#include "geo/geos/geos_executor.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace geo {

namespace geos {

static void WKBAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<double>(input, result, count, [](const GeometryPtr &geom) {
		return geom.Area();
	});
}

static void WKBLenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<double>(input, result, count, [](const GeometryPtr &geom) {
		return geom.Length();
	});
}

static void WKBIsSimpleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<bool>(input, result, count, [](const GeometryPtr &geom) {
		return geom.IsSimple();
	});
}

static void WKBIsValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<bool>(input, result, count, [](const GeometryPtr &geom) {
		return geom.IsValid();
	});
}

static void WKBIsEmptyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<bool>(input, result, count, [](const GeometryPtr &geom) {
		return geom.IsEmpty();
	});
}

static void WKBIsRingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<bool>(input, result, count, [](const GeometryPtr &geom) {
		return geom.IsRing();
	});
}

static void WKBIsClosedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];

	GEOSExecutor::ExecuteUnaryToScalar<bool>(input, result, count, [](const GeometryPtr &geom) {
		return geom.IsClosed();
	});
}


void GeosScalarFunctions::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	// ST_AREA
	CreateScalarFunctionInfo area_info(ScalarFunction("ST_Area", {gdal::GeoTypes::WKB_BLOB}, LogicalType::DOUBLE, WKBAreaFunction));
	area_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &area_info);

	// ST_LENGTH
	CreateScalarFunctionInfo length_info(ScalarFunction("ST_Length", {gdal::GeoTypes::WKB_BLOB}, LogicalType::DOUBLE, WKBLenFunction));
	length_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &length_info);

	// ST_IsSimple
	CreateScalarFunctionInfo is_simple_info(ScalarFunction("ST_IsSimple", {gdal::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsSimpleFunction));
	is_simple_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_simple_info);

	// ST_IsValid
	CreateScalarFunctionInfo is_valid_info(ScalarFunction("ST_IsValid", {gdal::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsValidFunction));
	is_valid_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_valid_info);

	// ST_IsEmpty
	CreateScalarFunctionInfo is_empty_info(ScalarFunction("ST_IsEmpty", {gdal::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsEmptyFunction));
	is_empty_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_empty_info);

	// ST_IsRing
	CreateScalarFunctionInfo is_ring_info(ScalarFunction("ST_IsRing", {gdal::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsRingFunction));
	is_ring_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_ring_info);

	// ST_IsClosed
	CreateScalarFunctionInfo is_closed_info(ScalarFunction("ST_IsClosed", {gdal::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsClosedFunction));
	is_closed_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_closed_info);

};

} // namespace geos

} // namespace geo