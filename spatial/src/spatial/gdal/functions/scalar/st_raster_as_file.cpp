#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/file_handler.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"
#include "spatial/gdal/raster/raster_factory.hpp"
#include "spatial/gdal/raster/raster_registry.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_RasterAsFile
//------------------------------------------------------------------------------

static void RasterAsFileFunction_01(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	TernaryExecutor::Execute<uintptr_t, string_t, string_t, bool>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](uintptr_t input, string_t file_name, string_t driver_name) {
		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		    auto gdal_driver_name = driver_name.GetString();
		    if (gdal_driver_name.empty()) {
			    throw InvalidInputException("Driver name must be specified");
		    }

		    auto raw_file_name = file_name.GetString();
		    auto &client_ctx = GDALClientContextState::GetOrCreate(context);
		    auto prefixed_file_name = client_ctx.GetPrefix(raw_file_name);

		    if (!RasterFactory::WriteFile(dataset, prefixed_file_name, gdal_driver_name)) {
			    auto error = Raster::GetLastErrorMsg();
			    if (error.length()) {
				    throw IOException("Could not save file: " + raw_file_name + " (" + error + ")");
			    }
			    return false;
		    }
		    return true;
	    });
}

static void RasterAsFileFunction_02(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	using POINTER_TYPE = PrimitiveType<uintptr_t>;
	using STRING_TYPE = PrimitiveType<string_t>;
	using LIST_TYPE = PrimitiveType<list_entry_t>;
	using BOOL_TYPE = PrimitiveType<bool>;

	auto &p1 = args.data[0];
	auto &p2 = args.data[1];
	auto &p3 = args.data[2];
	auto &p4 = args.data[3];
	auto &p4_entry = ListVector::GetEntry(p4);

	GenericExecutor::ExecuteQuaternary<POINTER_TYPE, STRING_TYPE, STRING_TYPE, LIST_TYPE, BOOL_TYPE>(
	    p1, p2, p3, p4, result, args.size(), [&](POINTER_TYPE p1, STRING_TYPE p2, STRING_TYPE p3, LIST_TYPE p4_offlen) {
		    auto input = p1.val;
		    auto file_name = p2.val;
		    auto driver_name = p3.val;
		    auto offlen = p4_offlen.val;

		    GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		    auto gdal_driver_name = driver_name.GetString();
		    if (gdal_driver_name.empty()) {
			    throw InvalidInputException("Driver name must be specified");
		    }

		    auto raw_file_name = file_name.GetString();
		    auto &client_ctx = GDALClientContextState::GetOrCreate(context);
		    auto prefixed_file_name = client_ctx.GetPrefix(raw_file_name);

		    auto options = std::vector<std::string>();

		    for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			    const auto &child_value = p4_entry.GetValue(i);
			    const auto option = child_value.ToString();
			    options.emplace_back(option);
		    }

		    if (!RasterFactory::WriteFile(dataset, prefixed_file_name, gdal_driver_name, options)) {
			    auto error = Raster::GetLastErrorMsg();
			    if (error.length()) {
				    throw IOException("Could not save file: " + raw_file_name + " (" + error + ")");
			    }
			    return false;
		    }
		    return true;
	    });
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Writes a raster to a file path.

	`write_options` is optional, an array of parameters for the GDAL driver specified.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	WITH __input AS (
		SELECT
			ST_RasterFromFile(file) AS raster
		FROM
			glob('./test/data/mosaic/SCL.tif-land-clip00.tiff')
	)
	SELECT
		ST_RasterAsFile(raster, './rasterasfile.tiff', 'Gtiff', ['COMPRESS=LZW']) AS result
	FROM
		__input
	;
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStRasterAsFile(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_RasterAsFile");

	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::BOOLEAN, RasterAsFileFunction_01));

	set.AddFunction(ScalarFunction(
	    {GeoTypes::RASTER(), LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    LogicalType::BOOLEAN, RasterAsFileFunction_02));

	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_RasterAsFile", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
