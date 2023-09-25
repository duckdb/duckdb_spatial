#include "spatial/geographiclib/module.hpp"
#include "spatial/geographiclib/functions.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

void GeographicLibModule::Register(DatabaseInstance &db) {

	// Register functions
	GeographicLibFunctions::Register(db);
}

} // namespace geographiclib

} // namespace spatial
