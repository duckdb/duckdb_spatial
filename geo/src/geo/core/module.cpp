
#include "geo/core/module.hpp"

#include "geo/common.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/functions/cast.hpp"
#include "geo/core/functions/aggregate.hpp"
#include "geo/core/layout_benchmark/test.hpp"
#include "geo/core/types.hpp"

namespace geo {

namespace core {

void CoreModule::Register(ClientContext &context) {
	GeoTypes::Register(context);
	CoreScalarFunctions::Register(context);
	// CoreAggregateFunctions::Register(context);
}

} // namespace core

} // namespace geo