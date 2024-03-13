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
static void CollectPoints(Geometry &geom, vector<Point> &points) {
	switch (geom.Type()) {
	case GeometryType::POINT: {
		points.push_back(geom.As<Point>());
		break;
	}
	case GeometryType::MULTIPOINT: {
		auto &multipoint = geom.As<MultiPoint>();
		for (auto &point : multipoint) {
			points.push_back(point);
		}
		break;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &col = geom.As<GeometryCollection>();
		for (auto &g : col) {
			CollectPoints(g, points);
		}
	}
	default: {
		break;
	}
	}
}

static void CollectLines(Geometry &geom, vector<LineString> &lines) {
	switch (geom.Type()) {
	case GeometryType::LINESTRING: {
		lines.push_back(geom.As<LineString>());
		break;
	}
	case GeometryType::MULTILINESTRING: {
		auto &multilines = geom.As<MultiLineString>();
		for (auto &line : multilines) {
			lines.push_back(line);
		}
		break;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &col = geom.As<GeometryCollection>();
		for (auto &g : col) {
			CollectLines(g, lines);
		}
	}
	default: {
		break;
	}
	}
}

static void CollectPolygons(Geometry &geom, vector<Polygon> &polys) {
	switch (geom.Type()) {
	case GeometryType::POLYGON: {
		polys.push_back(geom.As<Polygon>());
		break;
	}
	case GeometryType::MULTIPOLYGON: {
		auto &multipolys = geom.As<MultiPolygon>();
		for (auto &poly : multipolys) {
			polys.push_back(poly);
		}
		break;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &col = geom.As<GeometryCollection>();
		for (auto &g : col) {
			CollectPolygons(g, polys);
		}
	}
	default: {
		break;
	}
	}
}

// Collection extract with a specific dimension
static void CollectionExtractTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto count = args.size();
	auto &input = args.data[0];
	auto &dim = args.data[1];

	BinaryExecutor::Execute<geometry_t, int32_t, geometry_t>(
	    input, dim, result, count, [&](geometry_t input, int32_t requested_type) {
		    auto props = input.GetProperties();
		    auto geometry = lstate.factory.Deserialize(input);
		    switch (requested_type) {
		    case 1: {
			    if (geometry.Type() == GeometryType::MULTIPOINT || geometry.Type() == GeometryType::POINT) {
				    return input;
			    } else if (geometry.IsCollection()) {
				    // if it is a geometry collection, we need to collect all points
				    if (geometry.Type() == GeometryType::GEOMETRYCOLLECTION && !geometry.IsEmpty()) {
					    vector<Point> points;
					    CollectPoints(geometry, points);
					    uint32_t size = points.size();

					    MultiPoint mpoint(lstate.factory.allocator, size, props.HasZ(), props.HasM());
					    for (uint32_t i = 0; i < size; i++) {
						    mpoint[i] = points[i];
					    }
					    return lstate.factory.Serialize(result, mpoint, props.HasZ(), props.HasM());
				    }
				    // otherwise, we return an empty multipoint
				    MultiPoint empty(props.HasZ(), props.HasM());
				    return lstate.factory.Serialize(result, empty, props.HasZ(), props.HasM());
			    } else {
				    // otherwise if its not a collection, we return an empty point
				    Point empty(props.HasZ(), props.HasM());
				    return lstate.factory.Serialize(result, empty, props.HasZ(), props.HasM());
			    }
		    }
		    case 2: {
			    if (geometry.Type() == GeometryType::MULTILINESTRING || geometry.Type() == GeometryType::LINESTRING) {
				    return input;
			    } else if (geometry.IsCollection()) {
				    // if it is a geometry collection, we need to collect all lines
				    if (geometry.Type() == GeometryType::GEOMETRYCOLLECTION && !geometry.IsEmpty()) {
					    vector<LineString> lines;
					    CollectLines(geometry, lines);
					    uint32_t size = lines.size();

					    MultiLineString mline(lstate.factory.allocator, size, props.HasZ(), props.HasM());
					    for (uint32_t i = 0; i < size; i++) {
						    mline[i] = lines[i];
					    }
					    return lstate.factory.Serialize(result, mline, props.HasZ(), props.HasM());
				    }
				    // otherwise, we return an empty multilinestring
				    MultiLineString empty(props.HasZ(), props.HasM());
				    return lstate.factory.Serialize(result, empty, props.HasZ(), props.HasM());
			    } else {
				    // otherwise if its not a collection, we return an empty linestring
				    LineString empty(props.HasZ(), props.HasM());
				    return lstate.factory.Serialize(result, empty, props.HasZ(), props.HasM());
			    }
		    }
		    case 3: {
			    if (geometry.Type() == GeometryType::MULTIPOLYGON || geometry.Type() == GeometryType::POLYGON) {
				    return input;
			    } else if (geometry.IsCollection()) {
				    // if it is a geometry collection, we need to collect all polygons
				    if (geometry.Type() == GeometryType::GEOMETRYCOLLECTION && !geometry.IsEmpty()) {
					    vector<Polygon> polys;
					    CollectPolygons(geometry, polys);
					    uint32_t size = polys.size();

					    MultiPolygon mpoly(lstate.factory.allocator, size, props.HasZ(), props.HasM());
					    for (uint32_t i = 0; i < size; i++) {
						    mpoly[i] = polys[i];
					    }
					    return lstate.factory.Serialize(result, mpoly, props.HasZ(), props.HasM());
				    }
				    // otherwise, we return an empty multipolygon
				    MultiPolygon empty(props.HasZ(), props.HasM());
				    return lstate.factory.Serialize(result, empty, props.HasZ(), props.HasM());
			    } else {
				    // otherwise if its not a collection, we return an empty polygon
				    Polygon empty(props.HasZ(), props.HasM());
				    return lstate.factory.Serialize(result, empty, props.HasZ(), props.HasM());
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

	auto count = args.size();
	auto &input = args.data[0];

	UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](geometry_t input) {
		if (input.GetType() == GeometryType::GEOMETRYCOLLECTION) {
			auto props = input.GetProperties();
			auto geometry = lstate.factory.Deserialize(input);

			auto &collection = geometry.As<GeometryCollection>();
			if (collection.IsEmpty()) {
				return input;
			}
			// Find the highest dimension of the geometries in the collection
			// Empty geometries are ignored
			auto dim = collection.Aggregate(
			    [](const Geometry &geom, uint32_t d) {
				    if (geom.IsEmpty()) {
					    return d;
				    } else {
					    return std::max(geom.Dimension(), d);
				    }
			    },
			    0);

			switch (dim) {
			// Point case
			case 0: {
				vector<Point> points;
				CollectPoints(geometry, points);
				uint32_t size = points.size();
				MultiPoint mpoint(lstate.factory.allocator, size, props.HasZ(), props.HasM());
				for (uint32_t i = 0; i < size; i++) {
					mpoint[i] = points[i];
				}
				return lstate.factory.Serialize(result, mpoint, props.HasZ(), props.HasM());
			}
			// LineString case
			case 1: {
				vector<LineString> lines;
				CollectLines(geometry, lines);
				uint32_t size = lines.size();
				MultiLineString mline(lstate.factory.allocator, size, props.HasZ(), props.HasM());
				for (uint32_t i = 0; i < size; i++) {
					mline[i] = lines[i];
				}
				return lstate.factory.Serialize(result, mline, props.HasZ(), props.HasM());
			}
			// Polygon case
			case 2: {
				vector<Polygon> polys;
				CollectPolygons(geometry, polys);
				uint32_t size = polys.size();
				MultiPolygon mpoly(lstate.factory.allocator, size, props.HasZ(), props.HasM());
				for (uint32_t i = 0; i < size; i++) {
					mpoly[i] = polys[i];
				}
				return lstate.factory.Serialize(result, mpoly, props.HasZ(), props.HasM());
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

void CoreScalarFunctions::RegisterStCollectionExtract(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_CollectionExtract");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), CollectionExtractAutoFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::INTEGER}, GeoTypes::GEOMETRY(),
	                               CollectionExtractTypeFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial