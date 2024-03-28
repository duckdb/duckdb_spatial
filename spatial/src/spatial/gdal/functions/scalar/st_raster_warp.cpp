#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/file_handler.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_RasterWarp
//------------------------------------------------------------------------------

static void RasterWarpFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &ctx_state = GDALClientContextState::GetOrCreate(context);

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using LIST_TYPE = PrimitiveType<list_entry_t>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p2_entry = ListVector::GetEntry(p2);

	GenericExecutor::ExecuteBinary<POINTER_TYPE, LIST_TYPE, POINTER_TYPE>(p1, p2, result, args.size(),
		[&](POINTER_TYPE p1, LIST_TYPE p2_offlen) {
			auto input = p1.val;
			auto offlen = p2_offlen.val;

			GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

			if (dataset->GetRasterCount() == 0) {
				throw InvalidInputException("Input Raster has no RasterBands");
			}

			auto options = std::vector<std::string>();

			for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
				const auto &child_value = p2_entry.GetValue(i);
				const auto option = child_value.ToString();
				options.emplace_back(option);
			}

			GDALDataset *result = Raster::Warp(dataset, options);

			if (result == nullptr) {
				auto error = Raster::GetLastErrorMsg();
				throw IOException("Could not warp raster (" + error + ")");
			}

			auto &raster_registry = ctx_state.GetRasterRegistry(context);
			raster_registry.RegisterRaster(result);

			return CastPointerToValue(result);
		}
	);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStRasterWarp(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_RasterWarp");

	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::LIST(LogicalType::VARCHAR)},
	                               GeoTypes::RASTER(),
	                               RasterWarpFunction,
	                               nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace gdal

} // namespace spatial
