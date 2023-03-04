
#include "geo/common.hpp"
#include "geo/core/module.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/functions/aggregate.hpp"

namespace geo {

namespace core {

void CoreModule::Register(ClientContext &context) {
	GeoTypes::Register(context);
	CoreScalarFunctions::Register(context);
	CoreAggregateFunctions::Register(context);
}

} // namespace core

} // namespace geo