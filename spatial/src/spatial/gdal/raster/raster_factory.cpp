#include "spatial/common.hpp"
#include "spatial/gdal/raster/raster_factory.hpp"

namespace spatial {

namespace gdal {

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
