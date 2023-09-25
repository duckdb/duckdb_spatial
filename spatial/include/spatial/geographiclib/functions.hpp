#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

struct GeographicLibFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterDistance(db);
		RegisterDistanceWithin(db);
		RegisterLength(db);
		RegisterArea(db);
		RegisterPerimeter(db);
	}

private:
	static void RegisterDistance(DatabaseInstance &db);
	static void RegisterDistanceWithin(DatabaseInstance &db);
	static void RegisterLength(DatabaseInstance &db);
	static void RegisterArea(DatabaseInstance &db);
	static void RegisterPerimeter(DatabaseInstance &db);
};

} // namespace geographiclib

} // namespace spatial