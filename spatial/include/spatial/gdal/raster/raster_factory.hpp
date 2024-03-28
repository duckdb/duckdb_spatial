#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

//! A loader of GDALDataset from several data source types.
//! Does not take ownership of the pointer.
class RasterFactory {
public:

	//! Transforms a vector of strings as a vector of const char pointers.
	static std::vector<char const *> FromVectorOfStrings(const std::vector<std::string> &input);
	//! Transforms a map of params as a vector of const char pointers.
	static std::vector<char const *> FromNamedParameters(const named_parameter_map_t &input, const std::string &keyname);
};

} // namespace gdal

} // namespace spatial
