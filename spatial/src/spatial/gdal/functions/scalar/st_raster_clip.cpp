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
// ST_RasterClip
//------------------------------------------------------------------------------

static void RasterClipFunction_01(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &ctx_state = GDALClientContextState::GetOrCreate(context);

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];

	GenericExecutor::ExecuteBinary<POINTER_TYPE, GEOMETRY_TYPE, POINTER_TYPE>(
	    p1, p2, result, args.size(), [&](POINTER_TYPE p1, GEOMETRY_TYPE p2) {
		    auto input = p1.val;
		    auto geometry = p2.val;

		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		    if (dataset->GetRasterCount() == 0) {
			    throw InvalidInputException("Input Raster has no RasterBands");
		    }

		    GDALDataset *result = Raster::Clip(dataset, geometry);

		    if (result == nullptr) {
			    auto error = Raster::GetLastErrorMsg();
			    throw IOException("Could not clip raster (" + error + ")");
		    }

		    auto &raster_registry = ctx_state.GetRasterRegistry(context);
		    raster_registry.RegisterRaster(result);

		    return CastPointerToValue(result);
	    });
}

static void RasterClipFunction_02(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &ctx_state = GDALClientContextState::GetOrCreate(context);

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;
	using LIST_TYPE = PrimitiveType<list_entry_t>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p3 = args.data[2];
	auto &p3_entry = ListVector::GetEntry(p3);

	GenericExecutor::ExecuteTernary<POINTER_TYPE, GEOMETRY_TYPE, LIST_TYPE, POINTER_TYPE>(
	    p1, p2, p3, result, args.size(), [&](POINTER_TYPE p1, GEOMETRY_TYPE p2, LIST_TYPE p3_offlen) {
		    auto input = p1.val;
		    auto geometry = p2.val;
		    auto offlen = p3_offlen.val;

		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		    if (dataset->GetRasterCount() == 0) {
			    throw InvalidInputException("Input Raster has no RasterBands");
		    }

		    auto options = std::vector<std::string>();

		    for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			    const auto &child_value = p3_entry.GetValue(i);
			    const auto option = child_value.ToString();
			    options.emplace_back(option);
		    }

		    GDALDataset *result = Raster::Clip(dataset, geometry, options);

		    if (result == nullptr) {
			    auto error = Raster::GetLastErrorMsg();
			    throw IOException("Could not clip raster (" + error + ")");
		    }

		    auto &raster_registry = ctx_state.GetRasterRegistry(context);
		    raster_registry.RegisterRaster(result);

		    return CastPointerToValue(result);
	    });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns a raster that is clipped by the input geometry.

	`options` is optional, an array of parameters like [GDALWarp](https://gdal.org/programs/gdalwarp.html).
)";

static constexpr const char *DOC_EXAMPLE = R"(
	WITH __input AS (
		SELECT
			ST_RasterFromFile(file) AS raster
		FROM
			glob('./test/data/mosaic/SCL.tif-land-clip00.tiff')
	),
	__geometry AS (
		SELECT geom FROM ST_Read('./test/data/mosaic/CATAST_Pol_Township-PNA.gpkg')
	)
	SELECT
		ST_RasterClip(mosaic,
					(SELECT geom FROM __geometry LIMIT 1),
					options =>
						[
							'-r', 'bilinear', '-crop_to_cutline', '-wo', 'CUTLINE_ALL_TOUCHED=TRUE'
						]
		) AS clip
	FROM
		__input
	;
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStRasterClip(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_RasterClip");
	set.AddFunction(
	    ScalarFunction({GeoTypes::RASTER(), GeoTypes::GEOMETRY()}, GeoTypes::RASTER(), RasterClipFunction_01));

	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), GeoTypes::GEOMETRY(), LogicalType::LIST(LogicalType::VARCHAR)},
	                               GeoTypes::RASTER(), RasterClipFunction_02));

	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_RasterClip", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
