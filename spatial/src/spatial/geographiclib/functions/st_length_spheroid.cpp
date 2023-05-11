#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"
#include "GeographicLib/PolygonArea.hpp"

namespace spatial {

namespace geographiclib {

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void GeodesicLine2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 1);

	auto &line_vec = args.data[0];
	auto count = args.size();

	auto &coord_vec = ListVector::GetEntry(line_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

    const GeographicLib::Geodesic& geod = GeographicLib::Geodesic::WGS84();
	auto polygon_area = GeographicLib::PolygonArea(geod, true);

	UnaryExecutor::Execute<list_entry_t, double>(line_vec, result, count, [&](list_entry_t line) {
		polygon_area.Clear();
		auto offset = line.offset;
		auto length = line.length;
		// Loop over the segments
		for (idx_t j = offset; j < offset + length; j++) {
			auto x = x_data[j];
			auto y = y_data[j];
			polygon_area.AddPoint(x, y);
		}
		double _area;
		double linestring_length;
		polygon_area.Compute(false, true, linestring_length, _area);
		return linestring_length;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void GeographicLibFunctions::RegisterLength(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

    // Length
    ScalarFunctionSet set("st_length_spheroid");
    set.AddFunction(ScalarFunction({spatial::core::GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE,
                                          GeodesicLine2DFunction));
    CreateScalarFunctionInfo info(std::move(set));
    info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(context, &info);
}

} // namespace geographiclib

} // namespace spatial