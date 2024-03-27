#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/types/blob.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
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

	vector<data_t> buffer;
	UnaryExecutor::Execute<geometry_t, string_t>(input, result, count, [&](geometry_t input) {
		buffer.clear();
		WKBWriter::Write(input, buffer);

		auto blob_size = buffer.size() * 2; // every byte is rendered as two characters
		auto blob_str = StringVector::EmptyString(result, blob_size);
		auto blob_ptr = blob_str.GetDataWriteable();

		idx_t str_idx = 0;
		for (auto byte : buffer) {
			auto byte_a = byte >> 4;
			auto byte_b = byte & 0x0F;
			blob_ptr[str_idx++] = Blob::HEX_TABLE[byte_a];
			blob_ptr[str_idx++] = Blob::HEX_TABLE[byte_b];
		}

		blob_str.Finalize();
		return blob_str;
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the geometry as a HEXWKB string
)";

static constexpr const char *DOC_EXAMPLE = R"(
SELECT ST_AsHexWKB('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsHEXWKB(DatabaseInstance &db) {
	ScalarFunction func("ST_AsHEXWKB", {GeoTypes::GEOMETRY()}, LogicalType::VARCHAR, GeometryAsHEXWKBFunction);
	ExtensionUtil::RegisterFunction(db, func);
	DocUtil::AddDocumentation(db, "ST_AsHEXWKB", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial