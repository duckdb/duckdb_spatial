#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

struct GeographicLibModule {
public:
	static void Register(DatabaseInstance &db);
};

} // namespace geographiclib

} // namespace spatial