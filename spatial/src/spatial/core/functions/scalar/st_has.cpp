#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
template <bool HAS_Z_NOT_M>
static void GeometryHasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];
	UnaryExecutor::Execute<geometry_t, bool>(input, result, count, [&](const geometry_t &blob) {
		const auto props = blob.GetProperties();
		return HAS_Z_NOT_M ? props.HasZ() : props.HasM();
	});
}

static void GeometryZMFlagFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];
	UnaryExecutor::Execute<geometry_t, uint8_t>(input, result, count, [&](const geometry_t &blob) {
		const auto props = blob.GetProperties();
		const auto has_z = props.HasZ();
		const auto has_m = props.HasM();

		if(has_z && has_m) { return 3; }
		if(has_z) { return 2; }
		if(has_m) { return 1; }
		return 0;
	});
}

//------------------------------------------------------------------------------
// WKB
//------------------------------------------------------------------------------
template <bool HAS_Z_NOT_M>
static void WKBHasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](const string_t &blob) {
		Cursor cursor(blob);
		const auto le = cursor.Read<uint8_t>();
		const auto type = le ? cursor.Read<uint32_t>() : cursor.ReadBigEndian<uint32_t>();
		// Check for ISO WKB and EWKB Z and M flags
		const uint32_t iso_wkb_props = (type & 0xffff) / 1000;
		return HAS_Z_NOT_M ? (iso_wkb_props == 1) || (iso_wkb_props == 3) || ((type & 0x80000000) != 0)
		                   : (iso_wkb_props == 2) || (iso_wkb_props == 3) || ((type & 0x40000000) != 0);
	});
}

static void WKBZMFlagFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, uint8_t>(input, result, count, [&](const string_t &blob) {
		Cursor cursor(blob);
		const auto le = cursor.Read<uint8_t>();
		const auto type = le ? cursor.Read<uint32_t>() : cursor.ReadBigEndian<uint32_t>();
		// Check for ISO WKB and EWKB Z and M flags
		const uint32_t iso_wkb_props = (type & 0xffff) / 1000;
		const auto has_z = (iso_wkb_props == 1) || (iso_wkb_props == 3) || ((type & 0x80000000) != 0);
		const auto has_m = (iso_wkb_props == 2) || (iso_wkb_props == 3) || ((type & 0x40000000) != 0);

		if(has_z && has_m) { return 3; }
		if(has_z) { return 2; }
		if(has_m) { return 1; }
		return 0;
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

// HAS_Z
static constexpr const char *HAS_Z_DESCRIPTION = R"(
	Check if the input geometry has Z values.
)";

static constexpr const char *HAS_Z_EXAMPLE = R"(
	-- HasZ for a 2D geometry
	SELECT ST_HasZ(ST_GeomFromText('POINT(1 1)'));
	----
	false

	-- HasZ for a 3DZ geometry
	SELECT ST_HasZ(ST_GeomFromText('POINT Z(1 1 1)'));
	----
	true

	-- HasZ for a 3DM geometry
	SELECT ST_HasZ(ST_GeomFromText('POINT M(1 1 1)'));
	----
	false

	-- HasZ for a 4D geometry
	SELECT ST_HasZ(ST_GeomFromText('POINT ZM(1 1 1 1)'));
	----
	true
)";

// HAS_M
static constexpr const char *HAS_M_DESCRIPTION = R"(
	Check if the input geometry has M values.
)";

static constexpr const char *HAS_M_EXAMPLE = R"(
	-- HasM for a 2D geometry
	SELECT ST_HasM(ST_GeomFromText('POINT(1 1)'));
	----
	false

	-- HasM for a 3DZ geometry
	SELECT ST_HasM(ST_GeomFromText('POINT Z(1 1 1)'));
	----
	false

	-- HasM for a 3DM geometry
	SELECT ST_HasM(ST_GeomFromText('POINT M(1 1 1)'));
	----
	true

	-- HasM for a 4D geometry
	SELECT ST_HasM(ST_GeomFromText('POINT ZM(1 1 1 1)'));
	----
	true
)";

// ZMFLAG
static constexpr const char *ZMFLAG_DESCRIPTION = R"(
	Returns a flag indicating the presence of Z and M values in the input geometry.
	0 = No Z or M values
	1 = M values only
	2 = Z values only
	3 = Z and M values
)";

static constexpr const char *ZMFLAG_EXAMPLE = R"(
	-- ZMFlag for a 2D geometry
	SELECT ST_ZMFlag(ST_GeomFromText('POINT(1 1)'));
	----
	0

	-- ZMFlag for a 3DZ geometry
	SELECT ST_ZMFlag(ST_GeomFromText('POINT Z(1 1 1)'));
	----
	2

	-- ZMFlag for a 3DM geometry
	SELECT ST_ZMFlag(ST_GeomFromText('POINT M(1 1 1)'));
	----
	1

	-- ZMFlag for a 4D geometry
	SELECT ST_ZMFlag(ST_GeomFromText('POINT ZM(1 1 1 1)'));
	----
	3
)";

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStHas(DatabaseInstance &db) {
	ScalarFunctionSet st_hasz("ST_HasZ");
	st_hasz.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN,
		GeometryHasFunction<true>));
	st_hasz.AddFunction(ScalarFunction({GeoTypes::WKB_BLOB()}, LogicalType::BOOLEAN,
		WKBHasFunction<true>));


	ScalarFunctionSet st_hasm("ST_HasM");
	st_hasm.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN,
		GeometryHasFunction<false>));
	st_hasm.AddFunction(ScalarFunction({GeoTypes::WKB_BLOB()}, LogicalType::BOOLEAN,
		WKBHasFunction<false>));


	ScalarFunctionSet st_zmflag("ST_ZMFlag");
	st_zmflag.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::UTINYINT,
		GeometryZMFlagFunction));
	st_zmflag.AddFunction(ScalarFunction({GeoTypes::WKB_BLOB()}, LogicalType::UTINYINT,
		WKBZMFlagFunction));


	ExtensionUtil::RegisterFunction(db, st_hasz);
	ExtensionUtil::RegisterFunction(db, st_hasm);
	ExtensionUtil::RegisterFunction(db, st_zmflag);

	DocUtil::AddDocumentation(db, "ST_HasZ", HAS_Z_DESCRIPTION, HAS_Z_EXAMPLE, DOC_TAGS);
	DocUtil::AddDocumentation(db, "ST_HasM", HAS_M_DESCRIPTION, HAS_M_EXAMPLE, DOC_TAGS);
	DocUtil::AddDocumentation(db, "ST_ZMFlag", ZMFLAG_DESCRIPTION, ZMFLAG_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
