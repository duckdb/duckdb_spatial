#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

struct SimpleWKBReader {
	const char *data = nullptr;
	uint32_t cursor = 0;
	uint32_t length = 0;

	SimpleWKBReader(const char *data, uint32_t length) : data(data), length(length) {
	}

	vector<PointXY> ReadLine() {
		auto byte_order = ReadByte();
		D_ASSERT(byte_order == 1); // Little endian
		(void)byte_order;
		auto type = ReadInt();
		D_ASSERT(type == 2); // LineString
		(void)type;
		auto num_points = ReadInt();
		D_ASSERT(num_points > 0);
		D_ASSERT(cursor + num_points * 2 * sizeof(double) <= length);
		vector<PointXY> result;
		for (uint32_t i = 0; i < num_points; i++) {
			auto x = ReadDouble();
			auto y = ReadDouble();
			result.emplace_back(x, y);
		}
		return result;
	}

	PointXY ReadPoint() {
		auto byte_order = ReadByte();
		D_ASSERT(byte_order == 1); // Little endian
		(void)byte_order;
		auto type = ReadInt();
		D_ASSERT(type == 1); // Point
		(void)type;
		auto x = ReadDouble();
		auto y = ReadDouble();
		return PointXY(x, y);
	}

	vector<vector<PointXY>> ReadPolygon() {
		auto byte_order = ReadByte();
		D_ASSERT(byte_order == 1); // Little endian
		(void)byte_order;
		auto type = ReadInt();
		D_ASSERT(type == 3); // Polygon
		(void)type;
		auto num_rings = ReadInt();
		D_ASSERT(num_rings > 0);
		vector<vector<PointXY>> result;
		for (uint32_t i = 0; i < num_rings; i++) {
			auto num_points = ReadInt();
			D_ASSERT(num_points > 0);
			D_ASSERT(cursor + num_points * 2 * sizeof(double) <= length);
			vector<PointXY> ring;
			for (uint32_t j = 0; j < num_points; j++) {
				auto x = ReadDouble();
				auto y = ReadDouble();
				ring.emplace_back(x, y);
			}
			result.push_back(ring);
		}
		return result;
	}

	uint8_t ReadByte() {
		D_ASSERT(cursor + sizeof(uint8_t) <= length);
		uint8_t result = data[cursor];
		cursor += sizeof(uint8_t);
		return result;
	}

	uint32_t ReadInt() {
		D_ASSERT(cursor + sizeof(uint32_t) <= length);
		// Read uint32_t in little endian
		uint32_t result = 0;
		result |= (uint32_t)data[cursor + 0] << 0 & 0x000000FF;
		result |= (uint32_t)data[cursor + 1] << 8 & 0x0000FF00;
		result |= (uint32_t)data[cursor + 2] << 16 & 0x00FF0000;
		result |= (uint32_t)data[cursor + 3] << 24 & 0xFF000000;
		cursor += sizeof(uint32_t);
		return result;
	}

	double ReadDouble() {
		D_ASSERT(cursor + sizeof(double) <= length);
		// Read double in little endian
		uint64_t result = 0;
		result |= (uint64_t)data[cursor + 0] << 0 & 0x00000000000000FF;
		result |= (uint64_t)data[cursor + 1] << 8 & 0x000000000000FF00;
		result |= (uint64_t)data[cursor + 2] << 16 & 0x0000000000FF0000;
		result |= (uint64_t)data[cursor + 3] << 24 & 0x00000000FF000000;
		result |= (uint64_t)data[cursor + 4] << 32 & 0x000000FF00000000;
		result |= (uint64_t)data[cursor + 5] << 40 & 0x0000FF0000000000;
		result |= (uint64_t)data[cursor + 6] << 48 & 0x00FF000000000000;
		result |= (uint64_t)data[cursor + 7] << 56 & 0xFF00000000000000;
		cursor += sizeof(uint64_t);
		double result_double;
		memcpy(&result_double, &result, sizeof(double));
		return result_double;
	}
};

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void Point2DFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);

	auto &point_children = StructVector::GetEntries(result);
	auto x_data = FlatVector::GetData<double>(*point_children[0]);
	auto y_data = FlatVector::GetData<double>(*point_children[1]);

	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto point = reader.ReadPoint();
		x_data[i] = point.x;
		y_data[i] = point.y;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineString2DFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);

	auto &inner = ListVector::GetEntry(result);
	auto lines = ListVector::GetData(result);

	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	idx_t total_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto line = reader.ReadLine();
		auto line_size = line.size();

		lines[i].offset = total_size;
		lines[i].length = line_size;

		ListVector::Reserve(result, total_size + line_size);

		// Since ListVector::Reserve potentially reallocates, we need to re-fetch the inner vector pointers
		auto &children = StructVector::GetEntries(inner);
		auto &x_child = children[0];
		auto &y_child = children[1];
		auto x_data = FlatVector::GetData<double>(*x_child);
		auto y_data = FlatVector::GetData<double>(*y_child);

		for (idx_t j = 0; j < line_size; j++) {
			x_data[total_size + j] = line[j].x;
			y_data[total_size + j] = line[j].y;
		}

		total_size += line_size;
	}

	ListVector::SetListSize(result, total_size);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void Polygon2DFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();

	// Set up input data
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);
	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	// Set up output data
	auto &ring_vec = ListVector::GetEntry(result);
	auto polygons = ListVector::GetData(result);

	idx_t total_ring_count = 0;
	idx_t total_point_count = 0;

	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto polygon = reader.ReadPolygon();
		auto ring_count = polygon.size();

		polygons[i].offset = total_ring_count;
		polygons[i].length = ring_count;

		ListVector::Reserve(result, total_ring_count + ring_count);
		// Since ListVector::Reserve potentially reallocates, we need to re-fetch the inner vector pointers

		for (idx_t j = 0; j < ring_count; j++) {
			auto ring = polygon[j];
			auto point_count = ring.size();

			ListVector::Reserve(ring_vec, total_point_count + point_count);
			auto ring_entries = ListVector::GetData(ring_vec);
			auto &inner = ListVector::GetEntry(ring_vec);

			auto &children = StructVector::GetEntries(inner);
			auto &x_child = children[0];
			auto &y_child = children[1];
			auto x_data = FlatVector::GetData<double>(*x_child);
			auto y_data = FlatVector::GetData<double>(*y_child);

			for (idx_t k = 0; k < point_count; k++) {
				x_data[total_point_count + k] = ring[k].x;
				y_data[total_point_count + k] = ring[k].y;
			}

			ring_entries[total_ring_count + j].offset = total_point_count;
			ring_entries[total_ring_count + j].length = point_count;

			total_point_count += point_count;
		}
		total_ring_count += ring_count;
	}

	ListVector::SetListSize(result, total_ring_count);
	ListVector::SetListSize(ring_vec, total_point_count);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto &input = args.data[0];
	auto count = args.size();

	WKBReader reader(arena);
	UnaryExecutor::Execute<string_t, geometry_t>(input, result, count, [&](string_t input) {
		auto geom = reader.Deserialize(input);
		return Geometry::Serialize(geom, result);
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Deserializes a GEOMETRY from a WKB encoded blob
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr const DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStGeomFromWKB(DatabaseInstance &db) {

	ScalarFunction point2d_from_wkb_info("ST_Point2DFromWKB", {GeoTypes::WKB_BLOB()}, GeoTypes::POINT_2D(),
	                                     Point2DFromWKBFunction);
	ExtensionUtil::RegisterFunction(db, point2d_from_wkb_info);

	ScalarFunction linestring2d_from_wkb_info("ST_LineString2DFromWKB", {GeoTypes::WKB_BLOB()},
	                                          GeoTypes::LINESTRING_2D(), LineString2DFromWKBFunction);
	ExtensionUtil::RegisterFunction(db, linestring2d_from_wkb_info);

	ScalarFunction polygon2d_from_wkb("ST_Polygon2DFromWKB", {GeoTypes::WKB_BLOB()}, GeoTypes::POLYGON_2D(),
	                                  Polygon2DFromWKBFunction);
	ExtensionUtil::RegisterFunction(db, polygon2d_from_wkb);

	ScalarFunctionSet st_geom_from_wkb("ST_GeomFromWKB");
	st_geom_from_wkb.AddFunction(ScalarFunction({GeoTypes::WKB_BLOB()}, GeoTypes::GEOMETRY(), GeometryFromWKBFunction,
	                                            nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));
	st_geom_from_wkb.AddFunction(ScalarFunction({LogicalType::BLOB}, GeoTypes::GEOMETRY(), GeometryFromWKBFunction,
	                                            nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, st_geom_from_wkb);
	DocUtil::AddDocumentation(db, "ST_GeomFromWKB", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial