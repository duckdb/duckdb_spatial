#pragma once
#include "spatial/common.hpp"

class GDALDataset;

namespace spatial {

namespace gdal {

//! A loader of GDALDataset from several data source types.
//! Does not take ownership of the pointer.
class RasterFactory {
public:
	//! Given a file path, returns a GDALDataset
	static GDALDataset *FromFile(const std::string &file_path,
	                             const std::vector<std::string> &allowed_drivers = std::vector<std::string>(),
	                             const std::vector<std::string> &open_options = std::vector<std::string>(),
	                             const std::vector<std::string> &sibling_files = std::vector<std::string>());

	//! Writes a GDALDataset to a file path
	static bool WriteFile(GDALDataset *dataset, const std::string &file_path, const std::string &driver_name = "COG",
	                      const std::vector<std::string> &write_options = std::vector<std::string>());

	//! Transforms a vector of strings as a vector of const char pointers.
	static std::vector<char const *> FromVectorOfStrings(const std::vector<std::string> &input);
	//! Transforms a map of params as a vector of const char pointers.
	static std::vector<char const *> FromNamedParameters(const named_parameter_map_t &input,
	                                                     const std::string &keyname);
};

} // namespace gdal

} // namespace spatial
