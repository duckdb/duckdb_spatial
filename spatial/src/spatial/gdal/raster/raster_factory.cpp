#include "spatial/common.hpp"
#include "spatial/gdal/raster/raster_factory.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

GDALDataset *RasterFactory::FromFile(const std::string &file_path, const std::vector<std::string> &allowed_drivers,
                                     const std::vector<std::string> &open_options,
                                     const std::vector<std::string> &sibling_files) {

	auto gdal_allowed_drivers = RasterFactory::FromVectorOfStrings(allowed_drivers);
	auto gdal_open_options = RasterFactory::FromVectorOfStrings(open_options);
	auto gdal_sibling_files = RasterFactory::FromVectorOfStrings(sibling_files);

	GDALDataset *dataset = GDALDataset::Open(file_path.c_str(), GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR,
	                                         gdal_allowed_drivers.empty() ? nullptr : gdal_allowed_drivers.data(),
	                                         gdal_open_options.empty() ? nullptr : gdal_open_options.data(),
	                                         gdal_sibling_files.empty() ? nullptr : gdal_sibling_files.data());

	return dataset;
}

bool RasterFactory::WriteFile(GDALDataset *dataset, const std::string &file_path, const std::string &driver_name,
                              const std::vector<std::string> &write_options) {

	auto driver = GetGDALDriverManager()->GetDriverByName(driver_name.c_str());
	if (!driver) {
		throw InvalidInputException("Unknown driver '%s'", driver_name.c_str());
	}

	bool copy_available = CSLFetchBoolean(driver->GetMetadata(), GDAL_DCAP_CREATECOPY, FALSE);
	auto gdal_write_options = RasterFactory::FromVectorOfStrings(write_options);
	auto gdal_options = gdal_write_options.empty() ? nullptr : gdal_write_options.data();

	GDALDatasetUniquePtr output;
	CPLErrorReset();

	if (copy_available) {
		output = GDALDatasetUniquePtr(driver->CreateCopy(file_path.c_str(), dataset, FALSE, gdal_options, NULL, NULL));

		if (output.get() == nullptr) {
			return false;
		}
	} else {
		int cols = dataset->GetRasterXSize();
		int rows = dataset->GetRasterYSize();
		int band_count = dataset->GetRasterCount();

		if (band_count == 0) {
			throw InvalidInputException("Input Raster has no RasterBands");
		}

		GDALRasterBand *raster_band = dataset->GetRasterBand(1);
		GDALDataType data_type = raster_band->GetRasterDataType();
		int date_type_size = GDALGetDataTypeSize(data_type);

		output =
		    GDALDatasetUniquePtr(driver->Create(file_path.c_str(), cols, rows, band_count, data_type, gdal_options));

		if (output.get() == nullptr) {
			return false;
		}

		double gt[6] = {0, 1, 0, 0, 0, -1};
		dataset->GetGeoTransform(gt);
		output->SetGeoTransform(gt);

		output->SetProjection(dataset->GetProjectionRef());
		output->SetMetadata(dataset->GetMetadata());

		void *pafScanline = CPLMalloc(date_type_size * cols * rows);

		for (int i = 1; i <= band_count; i++) {
			GDALRasterBand *source_band = dataset->GetRasterBand(i);
			GDALRasterBand *target_band = output->GetRasterBand(i);

			target_band->SetMetadata(source_band->GetMetadata());
			target_band->SetNoDataValue(source_band->GetNoDataValue());
			target_band->SetColorInterpretation(source_band->GetColorInterpretation());

			if (source_band->RasterIO(GF_Read, 0, 0, cols, rows, pafScanline, cols, rows, data_type, 0, 0) != CE_None ||
			    target_band->RasterIO(GF_Write, 0, 0, cols, rows, pafScanline, cols, rows, data_type, 0, 0) !=
			        CE_None) {
				CPLFree(pafScanline);
				return false;
			}
		}

		CPLFree(pafScanline);
	}
	output->FlushCache();

	return true;
}

std::vector<char const *> RasterFactory::FromVectorOfStrings(const std::vector<std::string> &input) {
	auto output = std::vector<char const *>();

	if (input.size()) {
		output.reserve(input.size() + 1);

		for (auto it = input.begin(); it != input.end(); ++it) {
			output.push_back((*it).c_str());
		}
		output.push_back(nullptr);
	}
	return output;
}

std::vector<char const *> RasterFactory::FromNamedParameters(const named_parameter_map_t &input,
                                                             const std::string &keyname) {
	auto output = std::vector<char const *>();

	auto input_param = input.find(keyname);
	if (input_param != input.end()) {
		output.reserve(input.size() + 1);

		for (auto &param : ListValue::GetChildren(input_param->second)) {
			output.push_back(StringValue::Get(param).c_str());
		}
		output.push_back(nullptr);
	}
	return output;
}

} // namespace gdal

} // namespace spatial
