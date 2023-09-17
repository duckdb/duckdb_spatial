#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
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

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::LINESTRING:
			return geometry.GetLineString().Length();
		case GeometryType::MULTILINESTRING:
			return geometry.GetMultiLineString().Length();
		case GeometryType::GEOMETRYCOLLECTION:
			return geometry.GetGeometryCollection().Aggregate(
			    [](Geometry &geom, double state) {
				    if (geom.Type() == GeometryType::LINESTRING) {
					    return state + geom.GetLineString().Length();
				    } else if (geom.Type() == GeometryType::MULTILINESTRING) {
					    return state + geom.GetMultiLineString().Length();
				    } else {
					    return state;
				    }
			    },
			    0.0);
		default:
			return 0.0;
		}
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStLength(DatabaseInstance &instance) {
	ScalarFunctionSet length_function_set("ST_Length");

	length_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineLengthFunction));
	length_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryLengthFunction,
	                                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(instance, length_function_set);
	//info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
}

} // namespace core

} // namespace spatial