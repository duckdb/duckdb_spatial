#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("geometry");
	return_types.push_back(GeoTypes::GEOMETRY());

	return nullptr;
}

struct TestGeometryTypesState : public GlobalTableFunctionState {
	TestGeometryTypesState() : offset(0), factory(Allocator::DefaultAllocator()) {
	}
	idx_t offset;
	GeometryFactory factory;
	vector<Geometry> xy_geoms;
};

struct TestGeometryGenerator {
	Allocator &allocator;
	explicit TestGeometryGenerator(Allocator &allocator) : allocator(allocator) {
	}

	vector<Point> GetPoints() {
		vector<Point> points;
		// Standard Point
		points.emplace_back(Point(allocator, 1.0, 2.0));

		// Empty Point
		points.emplace_back(Point(allocator));

		return points;
	}

	vector<LineString> GetLineStrings() {
		vector<LineString> lines;
		// Standard LineString
		LineString line(allocator, false, false);
		line.Vertices().Append({0, 0});
		line.Vertices().Append({1, 1});
		lines.emplace_back(std::move(line));

		// Empty LineString
		lines.emplace_back(LineString(allocator, false, false));

		return lines;
	}

	vector<Polygon> GetPolygons() {
		vector<Polygon> polygons;
		// Standard Polygon
		Polygon polygon(allocator, 1, false, false);
		polygon[0].Append({0, 0});
		polygon[0].Append({1, 0});
		polygon[0].Append({1, 1});
		polygon[0].Append({0, 1});
		polygon[0].Append({0, 0});

		// FUCK: this resizes the vector which reallocates the polygon but then the vertex data is freed. FUCK.
		polygons.emplace_back(std::move(polygon));

		// Empty Polygon
		polygons.emplace_back(Polygon(allocator));

		return polygons;
	}

	vector<MultiPoint> GetMultiPoints() {
		vector<MultiPoint> multi_points;

		// Empty MultiPoint
		multi_points.emplace_back(MultiPoint(allocator, 0));

		// Every combination of point
		auto points = GetPoints();
		for (auto i = 0; i < points.size(); i++) {
			for (auto j = 0; j < points.size(); j++) {
				MultiPoint multi_point(allocator, 2);
				multi_point[0] = points[i].DeepCopy();
				multi_point[1] = points[j].DeepCopy();
				multi_points.emplace_back(std::move(multi_point));
			}
		}
		return multi_points;
	}

	vector<MultiLineString> GetMultiLineStrings() {
		vector<MultiLineString> multi_lines;

		// Empty MultiLineString
		multi_lines.emplace_back(MultiLineString(allocator, 0));

		// Every combination of line
		auto lines = GetLineStrings();
		for (auto i = 0; i < lines.size(); i++) {
			for (auto j = 0; j < lines.size(); j++) {
				MultiLineString multi_line(allocator, 2);
				multi_line[0] = lines[i].DeepCopy();
				multi_line[1] = lines[j].DeepCopy();
				multi_lines.emplace_back(std::move(multi_line));
			}
		}
		return multi_lines;
	}

	vector<MultiPolygon> GetMultiPolygons() {
		vector<MultiPolygon> multi_polygons;

		// Empty MultiPolygon
		multi_polygons.emplace_back(MultiPolygon(allocator, 0));

		// Every combination of polygon
		auto polygons = GetPolygons();
		for (auto i = 0; i < polygons.size(); i++) {
			for (auto j = 0; j < polygons.size(); j++) {
				MultiPolygon multi_polygon(allocator, 2);
				multi_polygon[0] = polygons[i].DeepCopy();
				multi_polygon[1] = polygons[j].DeepCopy();
				multi_polygons.emplace_back(std::move(multi_polygon));
			}
		}
		return multi_polygons;
	}

	vector<GeometryCollection> GetGeometryCollections() {
		vector<GeometryCollection> geometry_collections;

		// Empty GeometryCollection
		geometry_collections.emplace_back(GeometryCollection(allocator, 0));

		// Every combination of geometry
		auto points = GetPoints();
		auto lines = GetLineStrings();
		auto polygons = GetPolygons();
		for (auto i = 0; i < points.size(); i++) {
			for (auto j = 0; j < lines.size(); j++) {
				for (auto k = 0; k < polygons.size(); k++) {
					GeometryCollection geometry_collection(allocator, 3);
					geometry_collection[0] = points[i].DeepCopy();
					geometry_collection[1] = lines[j].DeepCopy();
					geometry_collection[2] = polygons[k].DeepCopy();
					geometry_collections.emplace_back(std::move(geometry_collection));
				}
			}
		}
		return geometry_collections;
	}
};

static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<TestGeometryTypesState>();
	auto &allocator = Allocator::DefaultAllocator();
	TestGeometryGenerator generator(allocator);

	auto points = generator.GetPoints();
	auto lines = generator.GetLineStrings();
	auto polygons = generator.GetPolygons();
	auto multi_points = generator.GetMultiPoints();
	auto multi_lines = generator.GetMultiLineStrings();
	auto multi_polygons = generator.GetMultiPolygons();
	auto geometry_collections = generator.GetGeometryCollections();

	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(points.begin()),
	                        std::make_move_iterator(points.end()));
	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(lines.begin()),
	                        std::make_move_iterator(lines.end()));
	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(polygons.begin()),
	                        std::make_move_iterator(polygons.end()));
	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(multi_points.begin()),
	                        std::make_move_iterator(multi_points.end()));
	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(multi_lines.begin()),
	                        std::make_move_iterator(multi_lines.end()));
	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(multi_polygons.begin()),
	                        std::make_move_iterator(multi_polygons.end()));
	result->xy_geoms.insert(result->xy_geoms.end(), std::make_move_iterator(geometry_collections.begin()),
	                        std::make_move_iterator(geometry_collections.end()));

	return std::move(result);
}

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<TestGeometryTypesState>();
	auto &geom_vector = output.data[0];
	auto geom_data = FlatVector::GetData<geometry_t>(geom_vector);

	idx_t count = 0;
	while (state.offset < state.xy_geoms.size() && count < STANDARD_VECTOR_SIZE) {
		auto &geom = state.xy_geoms[state.offset++];
		geom_data[count] = state.factory.Serialize(geom_vector, geom, false, false);
		count++;
	}

	output.SetCardinality(count);
}

void CoreTableFunctions::RegisterTestTableFunctions(DatabaseInstance &db) {

	TableFunction test_geometry_types("test_geometry_types", {}, Execute, Bind, Init);
	ExtensionUtil::RegisterFunction(db, test_geometry_types);
}

} // namespace core

} // namespace spatial
