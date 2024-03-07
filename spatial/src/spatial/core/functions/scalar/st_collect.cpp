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

static void CollectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();
	auto &child_vec = ListVector::GetEntry(args.data[0]);
	UnifiedVectorFormat format;
	child_vec.ToUnifiedFormat(count, format);

	UnaryExecutor::Execute<list_entry_t, geometry_t>(args.data[0], result, count, [&](list_entry_t &geometry_list) {
		auto offset = geometry_list.offset;
		auto length = geometry_list.length;

		// First figure out if we have Z or M
		bool has_z = false;
		bool has_m = false;
		for (idx_t i = offset; i < offset + length; i++) {
			auto mapped_idx = format.sel->get_index(i);
			if (format.validity.RowIsValid(mapped_idx)) {
				auto geometry_blob = ((geometry_t *)format.data)[mapped_idx];
				auto props = geometry_blob.GetProperties();
				has_z = has_z || props.HasZ();
				has_m = has_m || props.HasM();
			}
		}

		// TODO: Peek the types first
		vector<Geometry> geometries;
		for (idx_t i = offset; i < offset + length; i++) {
			auto mapped_idx = format.sel->get_index(i);
			if (format.validity.RowIsValid(mapped_idx)) {
				auto geometry_blob = ((geometry_t *)format.data)[mapped_idx];
				auto geometry = lstate.factory.Deserialize(geometry_blob);
				// Dont add empty geometries
				if (!geometry.IsEmpty()) {
					geometries.push_back(geometry);
				}
			}
		}

		if (geometries.empty()) {
			auto empty = lstate.factory.CreateGeometryCollection(0);
			return lstate.factory.Serialize(result, empty, false, false);
		}

		bool all_points = true;
		bool all_lines = true;
		bool all_polygons = true;

		for (auto &geometry : geometries) {
			if (geometry.Type() != GeometryType::POINT) {
				all_points = false;
			}
			if (geometry.Type() != GeometryType::LINESTRING) {
				all_lines = false;
			}
			if (geometry.Type() != GeometryType::POLYGON) {
				all_polygons = false;
			}
		}

		if (all_points) {
			auto collection = lstate.factory.CreateMultiPoint(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection[i] = geometries[i].SetVertexType(has_z, has_m).As<Point>();
			}
			return lstate.factory.Serialize(result, collection, has_z, has_m);
		} else if (all_lines) {
			auto collection = lstate.factory.CreateMultiLineString(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection[i] = geometries[i].SetVertexType(has_z, has_m).As<LineString>();
			}
			return lstate.factory.Serialize(result, collection, has_z, has_m);
		} else if (all_polygons) {
			auto collection = lstate.factory.CreateMultiPolygon(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection[i] = geometries[i].SetVertexType(has_z, has_m).As<Polygon>();
			}
			return lstate.factory.Serialize(result, collection, has_z, has_m);
		} else {
			auto collection = lstate.factory.CreateGeometryCollection(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection[i] = geometries[i].SetVertexType(has_z, has_m);
			}
			return lstate.factory.Serialize(result, collection, has_z, has_m);
		}
	});
}

void CoreScalarFunctions::RegisterStCollect(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Collect");

	set.AddFunction(ScalarFunction({LogicalType::LIST(GeoTypes::GEOMETRY())}, GeoTypes::GEOMETRY(), CollectFunction,
	                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
