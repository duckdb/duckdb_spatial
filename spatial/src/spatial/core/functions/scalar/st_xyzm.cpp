#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
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

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
template<size_t N>
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

template<size_t N> // 0: x_min, 1: y_min, 2: x_max, 3: y_max
static void Box2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &box_vec = StructVector::GetEntries(input);
	auto &ordinate_vec = box_vec[N];
	result.Reference(*ordinate_vec);
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
template<size_t N, class OP>
static void LineString2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &line = args.data[0];
	auto &line_coords = ListVector::GetEntry(line);
	auto &line_coords_vec = StructVector::GetEntries(line_coords);
	auto ordinate_data = FlatVector::GetData<double>(*line_coords_vec[N]);

	UnaryExecutor::ExecuteWithNulls<list_entry_t, double>(line, result, args.size(), [&](list_entry_t &line, ValidityMask &mask, idx_t idx) {

		// Empty line, return NULL
		if(line.length == 0) {
			mask.SetInvalid(idx);
			return 0.0;
		}

		auto val = OP::Default();
		for(idx_t i = line.offset; i < line.offset + line.length; i++) {
			auto ordinate = ordinate_data[i];
			val = OP::Operation(val, ordinate);
		}
		return val;
	});

	if(line.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
template<size_t N, class OP>
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

	UnaryExecutor::ExecuteWithNulls<list_entry_t, double>(input, result, count,
	                                                      [&](list_entry_t polygon, ValidityMask &mask, idx_t idx) {
		auto polygon_offset = polygon.offset;

		// Empty polygon, return NULL
		if(polygon.length == 0) {
			mask.SetInvalid(idx);
			return 0.0;
		}

		// We only have to check the outer shell
		auto shell_ring = ring_entries[polygon_offset];
		auto ring_offset = shell_ring.offset;
		auto ring_length = shell_ring.length;

		// Polygon is invalid. This should never happen but just in case
		if(ring_length == 0) {
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
template<size_t N, bool MIN>
static void GeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	static_assert(N < 2, "Invalid ordinate index");
	D_ASSERT(args.data.size() == 1);

	auto count = args.size();
	auto &input = args.data[0];

	BoundingBox bbox;

	UnaryExecutor::ExecuteWithNulls<string_t, double>(
	    input, result, count, [&](string_t blob, ValidityMask &mask, idx_t idx) {
		    if(GeometryFactory::TryGetSerializedBoundingBox(blob, bbox)){
				if(MIN && N == 0) {
				    return static_cast<double>(bbox.minx);
			    }
			    if(MIN && N == 1) {
				    return static_cast<double>(bbox.miny);
			    }
			    if(!MIN && N == 0) {
				    return static_cast<double>(bbox.maxx);
			    }
			    if(!MIN && N == 1) {
				    return static_cast<double>(bbox.maxy);
			    }
			    return 0.0; // unreachable
		    } else {
			    mask.SetInvalid(idx);
			    return 0.0;
		    }
	    });

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template<size_t N>
static void GeometryAccessFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	static_assert(N < 2, "Invalid ordinate index");
	D_ASSERT(args.data.size() == 1);

	auto count = args.size();
	auto &input = args.data[0];

	BoundingBox bbox;

	UnaryExecutor::ExecuteWithNulls<string_t, double>(
	    input, result, count, [&](string_t blob, ValidityMask &mask, idx_t idx) {
		    auto header = GeometryHeader::Get(blob);
		    if(header.type != GeometryType::POINT){
			 	throw InvalidInputException("ST_X/ST_Y only supports POINT geometries");
		    }
		    if(!GeometryFactory::TryGetSerializedBoundingBox(blob, bbox)){
			    mask.SetInvalid(idx);
			    return 0.0;
		    }

		    // The bounding box for a point is the same as the point itself
		    // so we can just return the ordinate directly
		    if(N == 0) {
			    return static_cast<double>(bbox.minx);
		    }
		    if( N == 1) {
			    return static_cast<double>(bbox.miny);
		    }
		    return 0.0; // unreachable
	    });

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStX(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_x("st_x");
	st_x.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<0>));
	st_x.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAccessFunction<0>));

	CreateScalarFunctionInfo info(std::move(st_x));
	catalog.AddFunction(context, info);
}

void CoreScalarFunctions::RegisterStXMax(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_xmax("st_xmax");
	st_xmax.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<2>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<0>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<0, MaxOp>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<0, MaxOp>));
	st_xmax.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<0, false>));

	CreateScalarFunctionInfo info(std::move(st_xmax));
	catalog.AddFunction(context, info);
}

void CoreScalarFunctions::RegisterStXMin(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_xmin("st_xmin");
	st_xmin.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<0>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<0>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<0, MinOp>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<0, MinOp>));
	st_xmin.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<0, true>));

	CreateScalarFunctionInfo info(std::move(st_xmin));
	catalog.AddFunction(context, info);
}


void CoreScalarFunctions::RegisterStY(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_y("st_y");
	st_y.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<1>));
	st_y.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAccessFunction<1>));

	CreateScalarFunctionInfo info(std::move(st_y));
	catalog.AddFunction(context, info);
}

void CoreScalarFunctions::RegisterStYMax(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_ymax("st_ymax");
	st_ymax.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<3>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<1>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<1, MaxOp>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<1, MaxOp>));
	st_ymax.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<1, false>));

	CreateScalarFunctionInfo info(std::move(st_ymax));
	catalog.AddFunction(context, info);
}

void CoreScalarFunctions::RegisterStYMin(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_ymin("st_ymin");
	st_ymin.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, Box2DFunction<1>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, Point2DFunction<1>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineString2DFunction<1, MinOp>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, Polygon2DFunction<1, MinOp>));
	st_ymin.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryFunction<1, true>));

	CreateScalarFunctionInfo info(std::move(st_ymin));
	catalog.AddFunction(context, info);
}


} // namespace core

} // namespace spatial
