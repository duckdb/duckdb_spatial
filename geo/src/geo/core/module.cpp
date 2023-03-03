
#include "geo/common.hpp"
#include "geo/core/module.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions.hpp"

namespace geo {

namespace core {

void CoreModule::Register(ClientContext &context) {
	GeoTypes::Register(context);
	GeometryFunctions::Register(context);
}

} // namespace core

} // namespace geo