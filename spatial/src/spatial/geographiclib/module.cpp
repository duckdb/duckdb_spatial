#include "spatial/geographiclib/module.hpp"
#include "spatial/geographiclib/functions.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

void GeographicLibModule::Register(ClientContext &context) {

	// Register functions
	GeographicLibFunctions::Register(context);
}

} // namespace geographiclib

} // namespace spatial
