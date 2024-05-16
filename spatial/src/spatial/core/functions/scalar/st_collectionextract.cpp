#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
// Collection extract with a specific dimension
static void CollectionExtractTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;
	auto count = args.size();
	auto &input = args.data[0];
	auto &dim = args.data[1];

	// Items vector
	vector<Geometry> items;

	BinaryExecutor::Execute<geometry_t, int32_t,
	                        geometry_t>(input, dim, result, count, [&](geometry_t input, int32_t requested_type) {
		// Reset the items vector
		items.clear();

		// Deserialize the input geometry
		auto props = input.GetProperties();
		auto geometry = Geometry::Deserialize(arena, input);

		// Switch on the requested type
		switch (requested_type) {
		case 1: {
			if (geometry.GetType() == GeometryType::MULTIPOINT || geometry.GetType() == GeometryType::POINT) {
				return input;
			} else if (geometry.IsCollection()) {
				// if it is a geometry collection, we need to collect all points
				if (geometry.GetType() == GeometryType::GEOMETRYCOLLECTION && !GeometryCollection::IsEmpty(geometry)) {
					Geometry::ExtractPoints(geometry, [&](const Geometry &point) { items.push_back(point); });
					auto mpoint = MultiPoint::Create(arena, items, props.HasZ(), props.HasM());
					return Geometry::Serialize(mpoint, result);
				}
				// otherwise, we return an empty multipoint
				auto empty = MultiPoint::CreateEmpty(props.HasZ(), props.HasM());
				return Geometry::Serialize(empty, result);
			} else {
				// otherwise if its not a collection, we return an empty point
				auto empty = Point::CreateEmpty(props.HasZ(), props.HasM());
				return Geometry::Serialize(empty, result);
			}
		}
		case 2: {
			if (geometry.GetType() == GeometryType::MULTILINESTRING || geometry.GetType() == GeometryType::LINESTRING) {
				return input;
			} else if (geometry.IsCollection()) {
				// if it is a geometry collection, we need to collect all lines
				if (geometry.GetType() == GeometryType::GEOMETRYCOLLECTION && !GeometryCollection::IsEmpty(geometry)) {
					Geometry::ExtractLines(geometry, [&](const Geometry &line) { items.push_back(line); });
					auto mline = MultiLineString::Create(arena, items, props.HasZ(), props.HasM());
					return Geometry::Serialize(mline, result);
				}
				// otherwise, we return an empty multilinestring
				auto empty = MultiLineString::CreateEmpty(props.HasZ(), props.HasM());
				return Geometry::Serialize(empty, result);
			} else {
				// otherwise if its not a collection, we return an empty linestring
				auto empty = LineString::CreateEmpty(props.HasZ(), props.HasM());
				return Geometry::Serialize(empty, result);
			}
		}
		case 3: {
			if (geometry.GetType() == GeometryType::MULTIPOLYGON || geometry.GetType() == GeometryType::POLYGON) {
				return input;
			} else if (geometry.IsCollection()) {
				// if it is a geometry collection, we need to collect all polygons
				if (geometry.GetType() == GeometryType::GEOMETRYCOLLECTION && !GeometryCollection::IsEmpty(geometry)) {
					Geometry::ExtractPolygons(geometry, [&](const Geometry &poly) { items.push_back(poly); });
					auto mpoly = MultiPolygon::Create(arena, items, props.HasZ(), props.HasM());
					return Geometry::Serialize(mpoly, result);
				}
				// otherwise, we return an empty multipolygon
				auto empty = MultiPolygon::CreateEmpty(props.HasZ(), props.HasM());
				return Geometry::Serialize(empty, result);
			} else {
				// otherwise if its not a collection, we return an empty polygon
				auto empty = Polygon::CreateEmpty(props.HasZ(), props.HasM());
				return Geometry::Serialize(empty, result);
			}
		}
		default:
			throw InvalidInputException("Invalid requested type parameter for collection extract, must be 1 "
			                            "(POINT), 2 (LINESTRING) or 3 (POLYGON)");
		}
	});
}

// Note: We're being smart here and reusing the memory from the input geometry
static void CollectionExtractAutoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	auto count = args.size();
	auto &input = args.data[0];

	vector<Geometry> items;

	UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](geometry_t input) {
		if (input.GetType() == GeometryType::GEOMETRYCOLLECTION) {
			// Reset the items vector
			items.clear();

			auto props = input.GetProperties();
			auto collection = Geometry::Deserialize(arena, input);
			if (GeometryCollection::IsEmpty(collection)) {
				return input;
			}
			// Find the highest dimension of the geometries in the collection
			// Empty geometries are ignored
			auto dim = Geometry::GetDimension(collection, true);

			switch (dim) {
			// Point case
			case 0: {
				Geometry::ExtractPoints(collection, [&](const Geometry &point) { items.push_back(point); });
				auto mpoint = MultiPoint::Create(arena, items, props.HasZ(), props.HasM());
				return Geometry::Serialize(mpoint, result);
			}
			// LineString case
			case 1: {
				Geometry::ExtractLines(collection, [&](const Geometry &line) { items.push_back(line); });
				auto mline = MultiLineString::Create(arena, items, props.HasZ(), props.HasM());
				return Geometry::Serialize(mline, result);
			}
			// Polygon case
			case 2: {
				Geometry::ExtractPolygons(collection, [&](const Geometry &poly) { items.push_back(poly); });
				auto mpoly = MultiPolygon::Create(arena, items, props.HasZ(), props.HasM());
				return Geometry::Serialize(mpoly, result);
			}
			default: {
				throw InternalException("Invalid dimension in collection extract");
			}
			}
		} else {
			return input;
		}
	});
}
//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Extracts a sub-geometry from a collection geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(
select st_collectionextract('MULTIPOINT(1 2,3 4)'::geometry, 1);
-- POINT(1 2)
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStCollectionExtract(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_CollectionExtract");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), CollectionExtractAutoFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::INTEGER}, GeoTypes::GEOMETRY(),
	                               CollectionExtractTypeFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_CollectionExtract", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial