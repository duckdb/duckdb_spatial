#include "spatial/geographiclib/module.hpp"
#include "spatial/geographiclib/functions.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

void GeographicLibModule::Register(DatabaseInstance &instance) {

	// Register functions
	GeographicLibFunctions::Register(instance);
}

} // namespace geographiclib

} // namespace spatial
