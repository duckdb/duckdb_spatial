
#include "spatial/core/module.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/functions/aggregate.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/optimizers.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

void CoreModule::Register(ClientContext &context) {
	GeoTypes::Register(context);
	CoreScalarFunctions::Register(context);
	CoreCastFunctions::Register(context);
	CoreTableFunctions::Register(context);
	CoreAggregateFunctions::Register(context);
	CoreOptimizerRules::Register(context);
}

} // namespace core

} // namespace spatial