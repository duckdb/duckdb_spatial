#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/types/blob.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// HEX WKB -> GEOMETRY
//------------------------------------------------------------------------------

void GeometryFromHEXWKB(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	WKBReader reader(lstate.arena);

	UnaryExecutor::Execute<string_t, geometry_t>(input, result, count, [&](string_t input_hex) {
		auto hex_size = input_hex.GetSize();
		auto hex_ptr = const_data_ptr_cast(input_hex.GetData());

		if (hex_size % 2 == 1) {
			throw InvalidInputException("Invalid HEX WKB string, length must be even.");
		}

		auto blob_size = hex_size / 2;

		unique_ptr<data_t[]> wkb_blob(new data_t[blob_size]);
		auto blob_ptr = wkb_blob.get();
		auto blob_idx = 0;
		for (idx_t hex_idx = 0; hex_idx < hex_size; hex_idx += 2) {
			auto byte_a = Blob::HEX_MAP[hex_ptr[hex_idx]];
			auto byte_b = Blob::HEX_MAP[hex_ptr[hex_idx + 1]];
			D_ASSERT(byte_a != -1);
			D_ASSERT(byte_b != -1);

			blob_ptr[blob_idx++] = (byte_a << 4) + byte_b;
		}

		auto geom = reader.Deserialize(blob_ptr, blob_size);
		return Geometry::Serialize(geom, result);
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

// HexWKB
static constexpr const char *DOC_DESCRIPTION = R"(
Creates a GEOMETRY from a HEXWKB string
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

// HexEWKB
static constexpr const char *EXTENDED_DOC_DESCRIPTION = R"(
    Deserialize a GEOMETRY from a HEXEWKB encoded string
)";

static constexpr const char *EXTENDED_DOC_EXAMPLE = R"(

)";

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStGeomFromHEXWKB(DatabaseInstance &db) {
	ScalarFunction hexwkb("ST_GeomFromHEXWKB", {LogicalType::VARCHAR}, GeoTypes::GEOMETRY(), GeometryFromHEXWKB,
	                      nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init);
	ExtensionUtil::RegisterFunction(db, hexwkb);
	DocUtil::AddDocumentation(db, "ST_GeomFromHEXWKB", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);

	// Our WKB reader also parses EWKB, even though it will just ignore SRID's.
	// so we'll just add an alias for now. In the future, once we actually handle
	// EWKB and store SRID's, these functions should differentiate between
	// the two formats.
	ScalarFunction ewkb("ST_GeomFromHEXEWKB", {LogicalType::VARCHAR}, GeoTypes::GEOMETRY(), GeometryFromHEXWKB, nullptr,
	                    nullptr, nullptr, GeometryFunctionLocalState::Init);
	ExtensionUtil::RegisterFunction(db, ewkb);
	DocUtil::AddDocumentation(db, "ST_GeomFromHEXEWKB", EXTENDED_DOC_DESCRIPTION, EXTENDED_DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial