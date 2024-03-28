#include "spatial/common.hpp"
#include "spatial/gdal/raster/raster_factory.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

GDALDataset *RasterFactory::FromFile(const std::string& file_path,
                                     const std::vector<std::string> &allowed_drivers,
                                     const std::vector<std::string> &open_options,
                                     const std::vector<std::string> &sibling_files) {

	auto gdal_allowed_drivers = RasterFactory::FromVectorOfStrings(allowed_drivers);
	auto gdal_open_options = RasterFactory::FromVectorOfStrings(open_options);
	auto gdal_sibling_files = RasterFactory::FromVectorOfStrings(sibling_files);

	GDALDataset *dataset =
		GDALDataset::Open(file_path.c_str(),
		                  GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR,
		                  gdal_allowed_drivers.empty() ? nullptr : gdal_allowed_drivers.data(),
		                  gdal_open_options.empty() ? nullptr : gdal_open_options.data(),
		                  gdal_sibling_files.empty() ? nullptr : gdal_sibling_files.data());

	return dataset;
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

std::vector<char const *> RasterFactory::FromNamedParameters(const named_parameter_map_t &input, const std::string &keyname) {
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
