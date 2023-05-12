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


	UnaryExecutor::Execute<list_entry_t, string_t>(args.data[0], result, count, [&](list_entry_t &geometry_list) {
        auto offset = geometry_list.offset;
        auto length = geometry_list.length;

		// TODO: Peek the types first
		vector<Geometry> geometries;
        for(idx_t i = offset; i < offset + length; i++) {
            auto mapped_idx = format.sel->get_index(i);
            if(format.validity.RowIsValid(mapped_idx)) {
				auto geometry_blob = ((string_t*)format.data)[mapped_idx];
				auto geometry = lstate.factory.Deserialize(geometry_blob);
				geometries.push_back(geometry);
            }
        }

		if(geometries.empty()) {
			auto empty = lstate.factory.CreateGeometryCollection(0);
			return lstate.factory.Serialize(result, Geometry(empty));
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

		if(all_points) {
			auto collection = lstate.factory.CreateMultiPoint(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection.points[i] = geometries[i].GetPoint();
			}
			return lstate.factory.Serialize(result, Geometry(collection));
		}
		else if(all_lines) {
			auto collection = lstate.factory.CreateMultiLineString(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection.linestrings[i] = geometries[i].GetLineString();
			}
			return lstate.factory.Serialize(result, Geometry(collection));
		}
		else if(all_polygons) {
			auto collection = lstate.factory.CreateMultiPolygon(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection.polygons[i] = geometries[i].GetPolygon();
			}
			return lstate.factory.Serialize(result, Geometry(collection));
		}
		else {
			auto collection = lstate.factory.CreateGeometryCollection(geometries.size());
			for (idx_t i = 0; i < geometries.size(); i++) {
				collection.geometries[i] = geometries[i];
			}
			return lstate.factory.Serialize(result, Geometry(collection));
		}
	});
}

void CoreScalarFunctions::RegisterStCollect(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Collect");

	set.AddFunction(ScalarFunction({LogicalType::LIST(GeoTypes::GEOMETRY())}, GeoTypes::GEOMETRY(), CollectFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace spatials

} // namespace spatial
