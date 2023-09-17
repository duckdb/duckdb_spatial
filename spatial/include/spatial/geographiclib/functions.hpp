#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

struct GeographicLibFunctions {
public:
	static void Register(DatabaseInstance &instance) {
		RegisterDistance(instance);
		RegisterDistanceWithin(instance);
		RegisterLength(instance);
		RegisterArea(instance);
		RegisterPerimeter(instance);
	}

private:
	static void RegisterDistance(DatabaseInstance &instance);
	static void RegisterDistanceWithin(DatabaseInstance &instance);
	static void RegisterLength(DatabaseInstance &instance);
	static void RegisterArea(DatabaseInstance &instance);
	static void RegisterPerimeter(DatabaseInstance &instance);
};

} // namespace geographiclib

} // namespace spatial