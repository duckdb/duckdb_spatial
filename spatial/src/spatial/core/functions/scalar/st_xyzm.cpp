#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

struct MinOp {
	static double Default() {
		return std::numeric_limits<double>::max();
	}
	static double Operation(double left, double right) {
		return std::min(left, right);
	}
};

struct MaxOp {
	static double Default() {
		return std::numeric_limits<double>::lowest();
	}
	static double Operation(double left, double right) {
		return std::max(left, right);
	}
};

struct AnyOp {
	static double Default() {
		return 0.0;
	}
	static double Operation(double left, double right) {
		return right;
	}
};

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
template <size_t N>
static void Point2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &point = args.data[0];
	auto &point_children = StructVector::GetEntries(point);
	auto &n_child = point_children[N];
	result.Reference(*n_child);
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------

template <size_t N> // 0: x_min, 1: y_min, 2: x_max, 3: y_max
static void Box2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &box_vec = StructVector::GetEntries(input);
	auto &ordinate_vec = box_vec[N];
	result.Reference(*ordinate_vec);
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
template <size_t N, class OP>
static void LineString2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &line = args.data[0];
	auto &line_coords = ListVector::GetEntry(line);
	auto &line_coords_vec = StructVector::GetEntries(line_coords);
	auto ordinate_data = FlatVector::GetData<double>(*line_coords_vec[N]);

	UnaryExecutor::ExecuteWithNulls<list_entry_t, double>(
	    line, result, args.size(), [&](list_entry_t &line, ValidityMask &mask, idx_t idx) {
		    // Empty line, return NULL
		    if (line.length == 0) {
			    mask.SetInvalid(idx);
			    return 0.0;
		    }

		    auto val = OP::Default();
		    for (idx_t i = line.offset; i < line.offset + line.length; i++) {
			    auto ordinate = ordinate_data[i];
			    val = OP::Operation(val, ordinate);
		    }
		    return val;
	    });

	if (line.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
template <size_t N, class OP>
static void Polygon2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto input = args.data[0];
	auto count = args.size();

	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);

	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &vertex_vec = ListVector::GetEntry(ring_vec);
	auto &vertex_vec_children = StructVector::GetEntries(vertex_vec);
	auto ordinate_data = FlatVector::GetData<double>(*vertex_vec_children[N]);

	UnaryExecutor::ExecuteWithNulls<list_entry_t, double>(
	    input, result, count, [&](list_entry_t polygon, ValidityMask &mask, idx_t idx) {
		    auto polygon_offset = polygon.offset;

		    // Empty polygon, return NULL
		    if (polygon.length == 0) {
			    mask.SetInvalid(idx);
			    return 0.0;
		    }

		    // We only have to check the outer shell
		    auto shell_ring = ring_entries[polygon_offset];
		    auto ring_offset = shell_ring.offset;
		    auto ring_length = shell_ring.length;

		    // Polygon is invalid. This should never happen but just in case
		    if (ring_length == 0) {
			    mask.SetInvalid(idx);
			    return 0.0;
		    }

		    auto val = OP::Default();
		    for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
			    auto ordinate = ordinate_data[coord_idx];
			    val = OP::Operation(val, ordinate);
		    }
		    return val;
	    });
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------

template <size_t N, class OP>
class BoundsProcessor final : GeometryProcessor<> {

	bool is_empty = true;
	double result = 0;

	void HandleVertexData(const VertexData &vertices) {
		if (!vertices.IsEmpty()) {
			is_empty = false;
		}
		for (uint32_t i = 0; i < vertices.count; i++) {
			result = OP::Operation(result, Load<double>(vertices.data[N] + i * vertices.stride[N]));
		}
	}

	void ProcessPoint(const VertexData &vertices) override {
		return HandleVertexData(vertices);
	}

	void ProcessLineString(const VertexData &vertices) override {
		return HandleVertexData(vertices);
	}

	void ProcessPolygon(PolygonState &state) override {
		while (!state.IsDone()) {
			HandleVertexData(state.Next());
		}
	}

	void ProcessCollection(CollectionState &state) override {
		while (!state.IsDone()) {
			state.Next();
		}
	}

public:
	double Execute(const geometry_t &geom) {
		is_empty = true;
		result = OP::Default();
		Process(geom);
		return result;
	}

	bool ResultIsEmpty() const {
		return is_empty;
	}
};

template <size_t N, class OP>
static void GeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	static_assert(N < 4, "Invalid ordinate index");
	D_ASSERT(args.data.size() == 1);

	auto count = args.size();
	auto &input = args.data[0];

	BoundsProcessor<N, OP> processor;
	UnaryExecutor::ExecuteWithNulls<geometry_t, double>(input, result, count,
	                                                    [&](geometry_t blob, ValidityMask &mask, idx_t idx) {
		                                                    auto res = processor.Execute(blob);
		                                                    if (processor.ResultIsEmpty()) {
			                                                    mask.SetInvalid(idx);
			                                                    return 0.0;
		                                                    } else {
			                                                    return res;
		                                                    }
	                                                    });

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <size_t N>
static void GeometryAccessFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	static_assert(N < 4, "Invalid ordinate index");
	D_ASSERT(args.data.size() == 1);

	auto count = args.size();
	auto &input = args.data[0];

	BoundsProcessor<N, AnyOp> processor;
	UnaryExecutor::ExecuteWithNulls<geometry_t, double>(
	    input, result, count, [&](geometry_t blob, ValidityMask &mask, idx_t idx) {
		    if (blob.GetType() != GeometryType::POINT) {
			    throw InvalidInputException("ST_X/ST_Y/ST_Z/ST_M only supports POINT geometries");
		    }
		    auto res = processor.Execute(blob);
		    if (processor.ResultIsEmpty()) {
			    mask.SetInvalid(idx);
			    return 0.0;
		    } else {
			    return res;
		    }
	    });

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------

DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

void CoreScalarFunctions::RegisterStX(DatabaseInstance &db) {

	ScalarFunctionSet st_x("ST_X");
	st_x.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<0>));
	st_x.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAccessFunction<0>));

	ExtensionUtil::RegisterFunction(db, st_x);

	auto DOC_DESCRIPTION = "Returns the X value of a point geometry, or NULL if not a point or empty";
	DocUtil::AddDocumentation(db, "ST_X", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStXMax(DatabaseInstance &db) {

	ScalarFunctionSet st_xmax("ST_XMax");
	st_xmax.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<2>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<0>));
	st_xmax.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<0, MaxOp>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<0, MaxOp>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<0, MaxOp>));

	ExtensionUtil::RegisterFunction(db, st_xmax);

	auto DOC_DESCRIPTION = "Returns the maximum X value of a geometry";
	DocUtil::AddDocumentation(db, "ST_XMax", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStXMin(DatabaseInstance &db) {

	ScalarFunctionSet st_xmin("ST_XMin");
	st_xmin.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<0>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<0>));
	st_xmin.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<0, MinOp>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<0, MinOp>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<0, MinOp>));

	ExtensionUtil::RegisterFunction(db, st_xmin);

	auto DOC_DESCRIPTION = "Returns the minimum X value of a geometry";
	DocUtil::AddDocumentation(db, "ST_XMin", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStY(DatabaseInstance &db) {

	ScalarFunctionSet st_y("ST_Y");
	st_y.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<1>));
	st_y.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAccessFunction<1>));

	ExtensionUtil::RegisterFunction(db, st_y);

	auto DOC_DESCRIPTION = "Returns the Y value of a point geometry, or NULL if not a point or empty";
	DocUtil::AddDocumentation(db, "ST_Y", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStYMax(DatabaseInstance &db) {

	ScalarFunctionSet st_ymax("ST_YMax");
	st_ymax.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<3>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<1>));
	st_ymax.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<1, MaxOp>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<1, MaxOp>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<1, MaxOp>));

	ExtensionUtil::RegisterFunction(db, st_ymax);

	auto DOC_DESCRIPTION = "Returns the maximum Y value of a geometry";
	DocUtil::AddDocumentation(db, "ST_YMax", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStYMin(DatabaseInstance &db) {

	ScalarFunctionSet st_ymin("ST_YMin");
	st_ymin.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<1>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<1>));
	st_ymin.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<1, MinOp>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<1, MinOp>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<1, MinOp>));

	ExtensionUtil::RegisterFunction(db, st_ymin);

	auto DOC_DESCRIPTION = "Returns the minimum Y value of a geometry";
	DocUtil::AddDocumentation(db, "ST_YMin", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStZ(DatabaseInstance &db) {

	ScalarFunctionSet st_z("ST_Z");
	st_z.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAccessFunction<2>));

	ExtensionUtil::RegisterFunction(db, st_z);

	auto DOC_DESCRIPTION = "Returns the Z value of a point geometry, or NULL if not a point or empty";
	DocUtil::AddDocumentation(db, "ST_Z", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStZMax(DatabaseInstance &db) {
	ScalarFunctionSet st_zmax("ST_ZMax");
	st_zmax.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<2, MaxOp>));

	ExtensionUtil::RegisterFunction(db, st_zmax);

	auto DOC_DESCRIPTION = "Returns the maximum Z value of a geometry";
	DocUtil::AddDocumentation(db, "ST_ZMax", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStZMin(DatabaseInstance &db) {
	ScalarFunctionSet st_zmin("ST_ZMin");
	st_zmin.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<2, MinOp>));

	ExtensionUtil::RegisterFunction(db, st_zmin);

	auto DOC_DESCRIPTION = "Returns the minimum Z value of a geometry";
	DocUtil::AddDocumentation(db, "ST_ZMin", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStM(DatabaseInstance &db) {
	ScalarFunctionSet st_m("ST_M");
	st_m.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAccessFunction<3>));

	ExtensionUtil::RegisterFunction(db, st_m);

	auto DOC_DESCRIPTION = "Returns the M value of a point geometry, or NULL if not a point or empty";
	DocUtil::AddDocumentation(db, "ST_M", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStMMax(DatabaseInstance &db) {
	ScalarFunctionSet st_mmax("ST_MMax");
	st_mmax.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<3, MaxOp>));

	ExtensionUtil::RegisterFunction(db, st_mmax);

	auto DOC_DESCRIPTION = "Returns the maximum M value of a geometry";
	DocUtil::AddDocumentation(db, "ST_MMax", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

void CoreScalarFunctions::RegisterStMMin(DatabaseInstance &db) {
	ScalarFunctionSet st_mmin("ST_MMin");
	st_mmin.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<3, MinOp>));

	ExtensionUtil::RegisterFunction(db, st_mmin);

	auto DOC_DESCRIPTION = "Returns the minimum M value of a geometry";
	DocUtil::AddDocumentation(db, "ST_MMin", DOC_DESCRIPTION, nullptr, DOC_TAGS);
}

} // namespace core

} // namespace spatial
