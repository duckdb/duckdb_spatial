#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/types/blob.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY -> HEX WKB
//------------------------------------------------------------------------------


void GeometryAsHEXWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		auto wkb_size = WKBWriter::GetRequiredSize(geometry);
        unique_ptr<data_t[]> wkb_blob(new data_t[wkb_size]);

        auto wkb_ptr = wkb_blob.get();
		WKBWriter::Write(geometry, wkb_ptr);
        
        auto blob_size = wkb_size * 2; // every byte is rendered as two characters
        auto blob_str = StringVector::EmptyString(result, blob_size);
        auto blob_ptr = blob_str.GetDataWriteable();
        
        idx_t str_idx = 0;
        wkb_ptr = wkb_blob.get(); // reset
        for (idx_t i = 0; i < wkb_size; i++) {
            auto byte_a = wkb_ptr[i] >> 4;
            auto byte_b = wkb_ptr[i] & 0x0F;

            blob_ptr[str_idx++] = Blob::HEX_TABLE[byte_a];
            blob_ptr[str_idx++] = Blob::HEX_TABLE[byte_b];
        }

		return blob_str;
	});
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsHEXWKB(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	CreateScalarFunctionInfo info(ScalarFunction("ST_AsHEXWKB", {GeoTypes::GEOMETRY()}, LogicalType::VARCHAR, GeometryAsHEXWKBFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace core

} // namespace spatial