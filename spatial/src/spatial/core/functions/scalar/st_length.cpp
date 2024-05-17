#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// LineString2D
//------------------------------------------------------------------------------
static void LineLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &line_vec = args.data[0];
	auto count = args.size();

	auto &coord_vec = ListVector::GetEntry(line_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, double>(line_vec, result, count, [&](list_entry_t line) {
		auto offset = line.offset;
		auto length = line.length;
		double sum = 0;
		// Loop over the segments
		for (idx_t j = offset; j < offset + length - 1; j++) {
			auto x1 = x_data[j];
			auto y1 = y_data[j];
			auto x2 = x_data[j + 1];
			auto y2 = y_data[j + 1];
			sum += std::sqrt(std::pow(x1 - x2, 2) + std::pow(y1 - y2, 2));
		}
		return sum;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, double>(input, result, count, [&](geometry_t input) {
		auto geom = Geometry::Deserialize(arena, input);
		double length = 0.0;
		Geometry::ExtractLines(geom, [&](const Geometry &line) { length += LineString::Length(line); });
		return length;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the length of the input line geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStLength(DatabaseInstance &db) {

	ScalarFunctionSet length_function_set("ST_Length");

	length_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineLengthFunction));
	length_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryLengthFunction,
	                                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, length_function_set);
	DocUtil::AddDocumentation(db, "ST_Length", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial