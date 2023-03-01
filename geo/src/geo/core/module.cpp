
#include "geo/common.hpp"
#include "geo/core/module.hpp"
#include "geo/core/types.hpp"

namespace geo {

namespace core {

void CoreModule::Register(ClientContext &context) {
	GeoTypes::Register(context);
}

} // namespace core

} // namespace geo