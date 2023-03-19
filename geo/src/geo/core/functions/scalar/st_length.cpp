#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions/scalar.hpp"

#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_context.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace geo {

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

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryContext ctx(allocator);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		allocator.Reset();
		auto geometry = ctx.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::POINT:
		case GeometryType::POLYGON:
			return 0.0; //
		case GeometryType::LINESTRING:
			return geometry.GetLineString().Length();
		default:
			throw NotImplementedException("Geometry type not implemented");
		}
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStLength(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet length_function_set("st_length");

	length_function_set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D}, LogicalType::DOUBLE, LineLengthFunction));
	length_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY}, LogicalType::DOUBLE, GeometryLengthFunction));

	CreateScalarFunctionInfo info(std::move(length_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace core

} // namespace geo