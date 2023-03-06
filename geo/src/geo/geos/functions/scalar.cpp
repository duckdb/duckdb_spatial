#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/geos/functions/scalar.hpp"
#include "geo/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace geo {

namespace geos {

// Conversion operations
static void WKBFromWKTFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKTReader();
	auto writer = ctx.CreateWKBWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkt) {
		auto geom = reader.Read(wkt);
		return writer.Write(geom, result);
	});
}

static void WKTFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKTWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return writer.Write(geom, result);
	});
}

static void WKBFromBlobFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	// We read a BLOB as wkb, and if it succeeds we write it back as wkb but this time as a WKB_BLOB type (since we now know it is valid)
	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return writer.Write(geom, result);
	});
}


// Property Accessors
static void WKBAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.Area();
	});
}

static void WKBLenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.Length();
	});
}

static void WKBIsSimpleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.IsSimple();
	});
}

static void WKBIsValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.IsValid();
	});
}

static void WKBIsEmptyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.IsEmpty();
	});
}

static void WKBIsRingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.IsRing();
	});
}

static void WKBIsClosedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.IsClosed();
	});
}

static void WKBGetXFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.GetX();
	});
}

static void WKBGetYFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		return geom.GetY();
	});
}

// Constructive operations

static void WKBSimplifyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto tolerance = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	BinaryExecutor::Execute<string_t, double, string_t>(input, tolerance, result, count, [&](string_t &wkb, double tol) {
		auto geom = reader.Read(wkb);
		auto simplified = geom.Simplify(tol);
		return writer.Write(simplified, result);
	});
}

static void WKBSimplifyPreserveTopologyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto tolerance = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	BinaryExecutor::Execute<string_t, double, string_t>(input, tolerance, result, count, [&](string_t &wkb, double tol) {
		auto geom = reader.Read(wkb);
		auto simplified = geom.SimplifyPreserveTopology(tol);
		return writer.Write(simplified, result);
	});
}

static void WKBBufferFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto distance = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	// TODO: Add support for optional arguments (quadsegs, endcapstyle, joinstyle, mitrelimit)
	BinaryExecutor::Execute<string_t, double, string_t>(input, distance, result, count, [&](string_t &wkb, double dist) {
		auto geom = reader.Read(wkb);
		auto buffered = geom.Buffer(dist, 9);
		return writer.Write(buffered, result);
	});
}

static void WKBBoundaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		auto boundary = geom.Boundary();
		return writer.Write(boundary, result);
	});
}

static void WKBCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		auto centroid = geom.Centroid();
		return writer.Write(centroid, result);
	});
}

static void WKBConvexHullFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		auto convex_hull = geom.ConvexHull();
		return writer.Write(convex_hull, result);
	});
}

static void WKBEnvelopeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		auto envelope = geom.Envelope();
		return writer.Write(envelope, result);
	});
}

static void WKBIntersectionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input_left = args.data[0];
	auto input_right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	BinaryExecutor::Execute<string_t, string_t, string_t>(input_left, input_right, result, count, [&](string_t &wkb_left, string_t &wkb_right) {
		auto geom_left = reader.Read(wkb_left);
		auto geom_right = reader.Read(wkb_right);
		auto intersection = geom_left.Intersection(geom_right);
		return writer.Write(intersection, result);
	});
}

// Mutators
static void WKBNormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();
	auto writer = ctx.CreateWKBWriter();

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkb) {
		auto geom = reader.Read(wkb);
		geom.Normalize();
		return writer.Write(geom, result);
	});
}


// Spatial Predicates
static void WKBCoversFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Covers(right_geom);
	});
}

static void WKBCoveredByFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.CoveredBy(right_geom);
	});
}

static void WKBCrossesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Crosses(right_geom);
	});
}

static void WKBDisjointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Disjoint(right_geom);
	});
}

static void WKBEqualsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Equals(right_geom);
	});
}

static void WKBIntersectsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Intersects(right_geom);
	});
}

static void WKBOverlapsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Overlaps(right_geom);
	});
}

static void WKBTouchesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Touches(right_geom);
	});
}

static void WKBWithinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Within(right_geom);
	});
}

static void WKBContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto left = args.data[0];
	auto right = args.data[1];
	auto count = args.size();
	auto ctx = GeosContextWrapper();
	auto reader = ctx.CreateWKBReader();

	BinaryExecutor::Execute<string_t, string_t, bool>(left, right, result, count, [&](string_t &left_wkb, string_t &right_wkb) {
		auto left_geom = reader.Read(left_wkb);
		auto right_geom = reader.Read(right_wkb);
		return left_geom.Contains(right_geom);
	});
}


void GeosScalarFunctions::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	/////////// Conversion Operations

	// TODO: Rename these once we have a proper Geometry type, and not just WKB. These should probably be called ST_WkbFromText and ST_WkbFromBlob
	CreateScalarFunctionInfo wkb_from_wkt_info(ScalarFunction("ST_GeomFromText", {LogicalType::VARCHAR}, core::GeoTypes::WKB_BLOB, WKBFromWKTFunction));
	wkb_from_wkt_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &wkb_from_wkt_info);

	CreateScalarFunctionInfo wkt_from_wkb_info(ScalarFunction("ST_AsText", {core::GeoTypes::WKB_BLOB}, LogicalType::VARCHAR, WKTFromWKBFunction));
	wkt_from_wkb_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &wkt_from_wkb_info);

	CreateScalarFunctionInfo wkb_from_wkb_info(ScalarFunction("ST_GeomFromWKB", {LogicalType::BLOB}, core::GeoTypes::WKB_BLOB, WKBFromBlobFunction));
	wkb_from_wkb_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &wkb_from_wkb_info);

	/////////// Property Accessors
	CreateScalarFunctionInfo area_info(ScalarFunction("ST_Area", {core::GeoTypes::WKB_BLOB}, LogicalType::DOUBLE, WKBAreaFunction));
	area_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &area_info);

	CreateScalarFunctionInfo length_info(ScalarFunction("ST_Length", {core::GeoTypes::WKB_BLOB}, LogicalType::DOUBLE, WKBLenFunction));
	length_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &length_info);

	CreateScalarFunctionInfo is_simple_info(ScalarFunction("ST_IsSimple", {core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsSimpleFunction));
	is_simple_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_simple_info);

	CreateScalarFunctionInfo is_valid_info(ScalarFunction("ST_IsValid", {core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsValidFunction));
	is_valid_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_valid_info);

	CreateScalarFunctionInfo is_empty_info(ScalarFunction("ST_IsEmpty", {core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsEmptyFunction));
	is_empty_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_empty_info);

	CreateScalarFunctionInfo is_ring_info(ScalarFunction("ST_IsRing", {core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsRingFunction));
	is_ring_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_ring_info);

	CreateScalarFunctionInfo is_closed_info(ScalarFunction("ST_IsClosed", {core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIsClosedFunction));
	is_closed_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &is_closed_info);

	CreateScalarFunctionInfo x_info(ScalarFunction("ST_X", {core::GeoTypes::WKB_BLOB}, LogicalType::DOUBLE, WKBGetXFunction));
	x_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &x_info);

	CreateScalarFunctionInfo y_info(ScalarFunction("ST_Y", {core::GeoTypes::WKB_BLOB}, LogicalType::DOUBLE, WKBGetYFunction));
	y_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &y_info);


	/////////// Constructive Operations
	CreateScalarFunctionInfo simplify_info(ScalarFunction("ST_Simplify", {core::GeoTypes::WKB_BLOB, LogicalType::DOUBLE}, core::GeoTypes::WKB_BLOB, WKBSimplifyFunction));
	simplify_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &simplify_info);

	CreateScalarFunctionInfo simplify_preserve_info(ScalarFunction("ST_SimplifyPreserveTopology", {core::GeoTypes::WKB_BLOB, LogicalType::DOUBLE}, core::GeoTypes::WKB_BLOB, WKBSimplifyPreserveTopologyFunction));
	simplify_preserve_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &simplify_preserve_info);

	CreateScalarFunctionInfo buffer_info(ScalarFunction("ST_Buffer", {core::GeoTypes::WKB_BLOB, LogicalType::DOUBLE}, core::GeoTypes::WKB_BLOB, WKBBufferFunction));
	buffer_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &buffer_info);

	CreateScalarFunctionInfo boundary_info(ScalarFunction("ST_Boundary", {core::GeoTypes::WKB_BLOB}, core::GeoTypes::WKB_BLOB, WKBBoundaryFunction));
	boundary_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &boundary_info);

	CreateScalarFunctionInfo convex_hull_info(ScalarFunction("ST_ConvexHull", {core::GeoTypes::WKB_BLOB}, core::GeoTypes::WKB_BLOB, WKBConvexHullFunction));
	convex_hull_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &convex_hull_info);

	CreateScalarFunctionInfo centroid_info(ScalarFunction("ST_Centroid", {core::GeoTypes::WKB_BLOB}, core::GeoTypes::WKB_BLOB, WKBCentroidFunction));
	centroid_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &centroid_info);

	CreateScalarFunctionInfo envelope_info(ScalarFunction("ST_Envelope", {core::GeoTypes::WKB_BLOB}, core::GeoTypes::WKB_BLOB, WKBEnvelopeFunction));
	envelope_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &envelope_info);

	CreateScalarFunctionInfo intersection_info(ScalarFunction("ST_Intersection", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, core::GeoTypes::WKB_BLOB, WKBIntersectionFunction));
	intersection_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &intersection_info);

	/////////// Mutations
	CreateScalarFunctionInfo normalize_info(ScalarFunction("ST_Normalize", {core::GeoTypes::WKB_BLOB}, core::GeoTypes::WKB_BLOB, WKBNormalizeFunction));
	normalize_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &normalize_info);

	/////////// Spatial Predicates
	CreateScalarFunctionInfo contains_info(ScalarFunction("ST_Contains", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBContainsFunction));
	contains_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &contains_info);

	CreateScalarFunctionInfo covers_info(ScalarFunction("ST_Covers", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBCoversFunction));
	covers_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &covers_info);

	CreateScalarFunctionInfo covered_by_info(ScalarFunction("ST_CoveredBy", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBCoveredByFunction));
	covered_by_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &covered_by_info);

	CreateScalarFunctionInfo crosses_info(ScalarFunction("ST_Crosses", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBCrossesFunction));
	crosses_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &crosses_info);

	CreateScalarFunctionInfo disjoint_info(ScalarFunction("ST_Disjoint", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBDisjointFunction));
	disjoint_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &disjoint_info);

	CreateScalarFunctionInfo equals_info(ScalarFunction("ST_Equals", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBEqualsFunction));
	equals_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &equals_info);

	CreateScalarFunctionInfo intersects_info(ScalarFunction("ST_Intersects", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBIntersectsFunction));
	intersects_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &intersects_info);

	CreateScalarFunctionInfo overlaps_info(ScalarFunction("ST_Overlaps", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBOverlapsFunction));
	overlaps_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &overlaps_info);

	CreateScalarFunctionInfo touches_info(ScalarFunction("ST_Touches", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBTouchesFunction));
	touches_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &touches_info);

	CreateScalarFunctionInfo within_info(ScalarFunction("ST_Within", {core::GeoTypes::WKB_BLOB, core::GeoTypes::WKB_BLOB}, LogicalType::BOOLEAN, WKBWithinFunction));
	within_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &within_info);
}

} // namespace geos

} // namespace geo