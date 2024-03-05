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

static void MakeLineListFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();
	auto &child_vec = ListVector::GetEntry(args.data[0]);
	UnifiedVectorFormat format;
	child_vec.ToUnifiedFormat(count, format);

	UnaryExecutor::Execute<list_entry_t, geometry_t>(args.data[0], result, count, [&](list_entry_t &geometry_list) {
		auto offset = geometry_list.offset;
		auto length = geometry_list.length;

		auto line_geom = lstate.factory.CreateLineString(length);

		for (idx_t i = offset; i < offset + length; i++) {

			auto mapped_idx = format.sel->get_index(i);
			if (!format.validity.RowIsValid(mapped_idx)) {
				continue;
			}
			auto geometry_blob = ((geometry_t *)format.data)[mapped_idx];
			auto geometry = lstate.factory.Deserialize(geometry_blob);

			if (geometry.Type() != GeometryType::POINT) {
				throw InvalidInputException("ST_MakeLine only accepts POINT geometries");
			}
			auto &point = geometry.GetPoint();
			if (point.IsEmpty()) {
				continue;
			}
			line_geom.Vertices().Add(point.GetVertex());
		}

		if (line_geom.Count() == 1) {
			throw InvalidInputException("ST_MakeLine requires zero or two or more POINT geometries");
		}

		return lstate.factory.Serialize(result, Geometry(line_geom));
	});
}

static void MakeLineBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();

	BinaryExecutor::Execute<geometry_t, geometry_t, geometry_t>(
	    args.data[0], args.data[1], result, count, [&](geometry_t &geom_blob_left, geometry_t &geom_blob_right) {
		    if (geom_blob_left.GetType() != GeometryType::POINT || geom_blob_right.GetType() != GeometryType::POINT) {
			    throw InvalidInputException("ST_MakeLine only accepts POINT geometries");
		    }

		    auto geometry_left = lstate.factory.Deserialize(geom_blob_left);
		    auto geometry_right = lstate.factory.Deserialize(geom_blob_right);

		    auto &point_left = geometry_left.GetPoint();
		    auto &point_right = geometry_right.GetPoint();

		    // TODO: we should add proper abstractions to append/concat VertexVectors
		    auto line_geom = lstate.factory.CreateLineString(2);
		    if (!point_left.IsEmpty()) {
			    line_geom.Vertices().Add(point_left.GetVertex());
		    }
		    if (!point_right.IsEmpty()) {
			    line_geom.Vertices().Add(point_right.GetVertex());
		    }

		    if (line_geom.Count() == 1) {
			    throw InvalidInputException("ST_MakeLine requires zero or two or more POINT geometries");
		    }

		    return lstate.factory.Serialize(result, Geometry(line_geom));
	    });
}

void CoreScalarFunctions::RegisterStMakeLine(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_MakeLine");

	set.AddFunction(ScalarFunction({LogicalType::LIST(GeoTypes::GEOMETRY())}, GeoTypes::GEOMETRY(),
	                               MakeLineListFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                               MakeLineBinaryFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
