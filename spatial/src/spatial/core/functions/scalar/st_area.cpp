#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();

	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, double>(input, result, count, [&](list_entry_t polygon) {
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;

		bool first = true;
		double area = 0;
		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			double sum = 0;
			for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
				sum += (x_data[coord_idx] * y_data[coord_idx + 1]) - (x_data[coord_idx + 1] * y_data[coord_idx]);
			}
			sum = std::abs(sum);
			if (first) {
				// Add outer ring
				area = sum * 0.5;
				first = false;
			} else {
				// Subtract holes
				area -= sum * 0.5;
			}
		}
		return area;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineStringAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	UnaryExecutor::Execute<list_entry_t, double>(input, result, args.size(), [](list_entry_t) { return 0; });
}

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void PointAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using AREA_TYPE = PrimitiveType<double>;
	GenericExecutor::ExecuteUnary<POINT_TYPE, AREA_TYPE>(args.data[0], result, args.size(),
	                                                     [](POINT_TYPE) { return 0; });
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void BoxAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using AREA_TYPE = PrimitiveType<double>;

	GenericExecutor::ExecuteUnary<BOX_TYPE, AREA_TYPE>(args.data[0], result, args.size(), [&](BOX_TYPE &box) {
		auto minx = box.a_val;
		auto miny = box.b_val;
		auto maxx = box.c_val;
		auto maxy = box.d_val;
		return AREA_TYPE {(maxx - minx) * (maxy - miny)};
	});
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
class AreaProcessor final : GeometryProcessor<double> {
	static double ProcessVertices(const VertexData &vertices) {
		if (vertices.count < 3) {
			return 0.0;
		}

		const auto count = vertices.count;
		const auto x_data = vertices.data[0];
		const auto y_data = vertices.data[1];
		const auto x_stride = vertices.stride[0];
		const auto y_stride = vertices.stride[1];

		double signed_area = 0.0;

		auto x0 = Load<double>(x_data);

		for (uint32_t i = 1; i < count - 1; ++i) {
			auto x1 = Load<double>(x_data + i * x_stride);
			auto y1 = Load<double>(y_data + (i + 1) * y_stride);
			auto y2 = Load<double>(y_data + (i - 1) * y_stride);
			signed_area += (x1 - x0) * (y2 - y1);
		}

		signed_area *= 0.5;

		return std::abs(signed_area);
	}

	double ProcessPoint(const VertexData &vertices) override {
		return 0.0;
	}

	double ProcessLineString(const VertexData &vertices) override {
		return 0.0;
	}

	double ProcessPolygon(PolygonState &state) override {
		double sum = 0.0;
		if (!state.IsDone()) {
			sum += ProcessVertices(state.Next());
		}
		while (!state.IsDone()) {
			sum -= ProcessVertices(state.Next());
		}
		return std::abs(sum);
	}

	double ProcessCollection(CollectionState &state) override {
		switch (CurrentType()) {
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			double sum = 0;
			while (!state.IsDone()) {
				sum += state.Next();
			}
			return sum;
		}
		default:
			return 0.0;
		}
	}

public:
	double Execute(const geometry_t &geometry) {
		return Process(geometry);
	}
};

static void GeometryAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto count = args.size();
	AreaProcessor processor;
	UnaryExecutor::Execute<geometry_t, double>(input, result, count,
	                                           [&](const geometry_t &input) { return processor.Execute(input); });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the area of a geometry.

    Compute the area of a geometry.

    Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.

    The `POINT_2D` and `LINESTRING_2D` variants of this function always return `0.0` but are included for completeness.
)";

static constexpr const char *DOC_EXAMPLE = R"(
    select ST_Area('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStArea(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Area");
	set.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, PointAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineStringAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, PolygonAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, BoxAreaFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Area", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
