#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"
namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
void GeometryAsWBKFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		auto size = WKBWriter::GetRequiredSize(geometry);
		auto str = StringVector::EmptyString(result, size);
		auto ptr = (data_ptr_t)(str.GetDataUnsafe());
		WKBWriter::Write(geometry, ptr);
		return str;
	});
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsWKB(DatabaseInstance &instance) {
	ScalarFunctionSet as_wkb_function_set("ST_AsWKB");

	as_wkb_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::WKB_BLOB(), GeometryAsWBKFunction,
	                                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

// ??	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	ExtensionUtil::RegisterFunction(instance, as_wkb_function_set);
}

} // namespace core

} // namespace spatial