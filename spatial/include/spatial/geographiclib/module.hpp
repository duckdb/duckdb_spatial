#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

struct GeographicLibModule {
public:
	static void Register(ClientContext &context);
};

} // namespace geographiclib

} // namespace spatial