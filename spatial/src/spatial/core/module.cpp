
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

void CoreModule::Register(DatabaseInstance &instance) {
	GeoTypes::Register(instance);
	CoreScalarFunctions::Register(instance);
	CoreCastFunctions::Register(instance);
	CoreTableFunctions::Register(instance);
	CoreAggregateFunctions::Register(instance);
	CoreOptimizerRules::Register(instance);
}

} // namespace core

} // namespace spatial