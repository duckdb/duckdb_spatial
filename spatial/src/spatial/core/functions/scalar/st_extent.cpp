#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"


namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// WKB
//------------------------------------------------------------------------------
static uint8_t ReadByte(Cursor &cursor) {
	return cursor.Read<uint8_t>();
}
static double ReadDouble(const bool le, Cursor &cursor) {
	return le ? cursor.Read<double>() : cursor.ReadBigEndian<double>();
}
static uint32_t ReadInt(const bool le, Cursor &cursor) {
	return le ? cursor.Read<uint32_t>() : cursor.ReadBigEndian<uint32_t>();
}
static void ReadWKB(Cursor &cursor, BoundingBox &bbox);

static void ReadWKB(const bool le, const uint32_t type, const bool has_z, const bool has_m, Cursor &cursor, BoundingBox &bbox) {
	switch(type) {
	case 1: { // POINT
		// Points are special in that they can be all-nan (empty)
		bool all_nan = true;
		double coords[4];
		for (auto i = 0; i < (2 + has_z + has_m); i++) {
			coords[i] = ReadDouble(le, cursor);
			if (!std::isnan(coords[i])) {
				all_nan = false;
			}
		}
		if (!all_nan) {
			bbox.Stretch(coords[0], coords[1]);
		}
	} break;
	case 2: { // LINESTRING
		const auto num_verts = ReadInt(le, cursor);
		for(uint32_t i = 0; i < num_verts; i++) {
			const auto x = ReadDouble(le, cursor);
			const auto y = ReadDouble(le, cursor);
			if(has_z) {
				ReadDouble(le, cursor);
			}
			if(has_m) {
				ReadDouble(le, cursor);
			}
			bbox.Stretch(x, y);
		}
	} break;
	case 3: { // POLYGON
		const auto num_rings = ReadInt(le, cursor);
		for(uint32_t i = 0; i < num_rings; i++) {
			const auto num_verts = ReadInt(le, cursor);
			for(uint32_t j = 0; j < num_verts; j++) {
				const auto x = ReadDouble(le, cursor);
				const auto y = ReadDouble(le, cursor);
				if(has_z) {
					ReadDouble(le, cursor);
				}
				if(has_m) {
					ReadDouble(le, cursor);
				}
				bbox.Stretch(x, y);
			}
		}
	} break;
	case 4: // MULTIPOINT
	case 5: // MULTILINESTRING
	case 6: // MULTIPOLYGON
	case 7: { // GEOMETRYCOLLECTION
		const auto num_items = ReadInt(le, cursor);
		for(uint32_t i = 0; i < num_items; i++) {
			ReadWKB(cursor, bbox);
		}
	} break;
	default:
		throw NotImplementedException("WKB Reader: Geometry type %u not supported", type);
	}
}

static void ReadWKB(Cursor &cursor, BoundingBox &bbox) {
	const auto le = ReadByte(cursor);
	const auto type = ReadInt(le, cursor);

	// Check for ISO WKB and EWKB Z and M flags
	const uint32_t iso_wkb_props = (type & 0xffff) / 1000;
	const auto has_z = (iso_wkb_props == 1) || (iso_wkb_props == 3) || ((type & 0x80000000) != 0);
	const auto has_m = (iso_wkb_props == 2) || (iso_wkb_props == 3) || ((type & 0x40000000) != 0);

	// Skip SRID if present
	const auto has_srid = (type & 0x20000000) != 0;
	if(has_srid) {
		cursor.Skip(sizeof(uint32_t));
	}

	ReadWKB(le, ((type & 0xffff) % 1000), has_z, has_m, cursor, bbox);
}

static void WKBExtFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using WKB_TYPE = PrimitiveType<string_t>;

	GenericExecutor::ExecuteUnary<WKB_TYPE, BOX_TYPE>(args.data[0], result, count, [&](const WKB_TYPE &wkb) {
		BoundingBox bbox;
		Cursor cursor(wkb.val);
		ReadWKB(cursor, bbox);
		return BOX_TYPE {bbox.minx, bbox.miny, bbox.maxx, bbox.maxy};
	});
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void ExtentFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto count = args.size();
	auto &input = args.data[0];
	auto &struct_vec = StructVector::GetEntries(result);
	auto min_x_data = FlatVector::GetData<double>(*struct_vec[0]);
	auto min_y_data = FlatVector::GetData<double>(*struct_vec[1]);
	auto max_x_data = FlatVector::GetData<double>(*struct_vec[2]);
	auto max_y_data = FlatVector::GetData<double>(*struct_vec[3]);

	UnifiedVectorFormat input_vdata;
	input.ToUnifiedFormat(count, input_vdata);
	auto input_data = UnifiedVectorFormat::GetData<geometry_t>(input_vdata);

	BoundingBox bbox;

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = input_vdata.sel->get_index(i);
		if (input_vdata.validity.RowIsValid(row_idx)) {
			auto &blob = input_data[row_idx];

			// Try to get the cached bounding box from the blob
			if (blob.TryGetCachedBounds(bbox)) {
				min_x_data[i] = bbox.minx;
				min_y_data[i] = bbox.miny;
				max_x_data[i] = bbox.maxx;
				max_y_data[i] = bbox.maxy;
			} else {
				// No bounding box, return null
				FlatVector::SetNull(result, i, true);
			}
		} else {
			// Null input, return null
			FlatVector::SetNull(result, i, true);
		}
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the minimal bounding box enclosing the input geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStExtent(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Extent");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::BOX_2D(), ExtentFunction));
	set.AddFunction(ScalarFunction({GeoTypes::WKB_BLOB()}, GeoTypes::BOX_2D(), WKBExtFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Extent", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial