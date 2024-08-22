#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/file_handler.hpp"
#include "spatial/gdal/functions/aggregate.hpp"
#include "spatial/gdal/functions/raster_agg.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------
// ST_RasterMosaic_Agg
//------------------------------------------------------------------------

template <class T, class STATE>
static void RasterMosaicFunction(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
	if (!state.is_set) {
		finalize_data.ReturnNull();
	} else {
		auto datasets = state.datasets;
		auto &bind_data = finalize_data.input.bind_data->Cast<RasterAggBindData>();
		auto &context = bind_data.context;
		auto &options = bind_data.options;

		GDALDataset *result = Raster::BuildVRT(datasets, options);

		if (result == nullptr) {
			auto error = Raster::GetLastErrorMsg();
			throw IOException("Could not make mosaic: (" + error + ")");
		}

		auto &ctx_state = GDALClientContextState::GetOrCreate(context);
		auto &raster_registry = ctx_state.GetRasterRegistry(context);
		raster_registry.RegisterRaster(result);

		target = CastPointerToValue(result);
	}
}

struct MosaicAggUnaryOperation : RasterAggUnaryOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		RasterMosaicFunction(state, target, finalize_data);
	}
};

struct MosaicAggBinaryOperation : RasterAggBinaryOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		RasterMosaicFunction(state, target, finalize_data);
	}
};

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns a mosaic of a set of raster tiles into a single raster.

	Tiles are considered as source rasters of a larger mosaic and the result dataset has as many bands as one of the input files.

	`options` is optional, an array of parameters like [GDALBuildVRT](https://gdal.org/programs/gdalbuildvrt.html).
)";

static constexpr const char *DOC_EXAMPLE = R"(
	WITH __input AS (
		SELECT
			1 AS raster_id,
			ST_RasterFromFile(file) AS raster
		FROM
			glob('./test/data/mosaic/*.tiff')
	),
	SELECT
		ST_RasterMosaic_Agg(raster, options => ['-r', 'bilinear']) AS r
	FROM
		__input
	GROUP BY
		raster_id
	;
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "aggregation"}};

//------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------

void GdalAggregateFunctions::RegisterStRasterMosaicAgg(DatabaseInstance &db) {

	AggregateFunctionSet st_mosaic_agg("ST_RasterMosaic_Agg");

	auto fun01 = AggregateFunction::UnaryAggregate<RasterAggState, uintptr_t, uintptr_t, MosaicAggUnaryOperation>(
	    GeoTypes::RASTER(), GeoTypes::RASTER());
	fun01.bind = BindRasterAggOperation;

	st_mosaic_agg.AddFunction(fun01);

	auto fun02 = AggregateFunction::BinaryAggregate<RasterAggState, uintptr_t, list_entry_t, uintptr_t,
	                                                MosaicAggBinaryOperation>(
	    GeoTypes::RASTER(), LogicalType::LIST(LogicalType::VARCHAR), GeoTypes::RASTER());
	fun02.bind = BindRasterAggOperation;

	st_mosaic_agg.AddFunction(fun02);

	ExtensionUtil::RegisterFunction(db, st_mosaic_agg);

	DocUtil::AddDocumentation(db, "ST_RasterMosaic_Agg", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
