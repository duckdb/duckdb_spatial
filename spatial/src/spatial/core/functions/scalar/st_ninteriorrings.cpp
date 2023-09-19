#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonInteriorRingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<list_entry_t, int32_t>(input, result, count, [&](list_entry_t polygon) {
		auto rings = polygon.length;
		return rings == 0 ? rings : static_cast<int32_t>(polygon.length) - 1; // -1 for the exterior ring
	});
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryInteriorRingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::ExecuteWithNulls<string_t, uint32_t>(
	    input, result, count, [&](string_t input, ValidityMask &validity, idx_t idx) {
		    auto header = GeometryHeader::Get(input);
		    if (header.type != GeometryType::POLYGON) {
			    validity.SetInvalid(idx);
			    return 0;
		    }
		    auto polygon = lstate.factory.Deserialize(input);
		    auto rings = polygon.GetPolygon().Count();
		    return rings == 0 ? 0 : static_cast<int32_t>(rings - 1); // -1 for the exterior ring
	    });
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStNInteriorRings(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	const char *aliases[] = {"ST_NumInteriorRings", "ST_NInteriorRings"};
	for (auto alias : aliases) {
		ScalarFunctionSet set(alias);
		set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::INTEGER, PolygonInteriorRingsFunction));

		set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::INTEGER, GeometryInteriorRingsFunction,
		                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

		CreateScalarFunctionInfo info(std::move(set));
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateFunction(context, info);
	}
}

} // namespace core

} // namespace spatial