
#include "spatial/core/module.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/aggregate.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/functions/macros.hpp"
#include "spatial/core/optimizer_rules.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

void CoreModule::Register(DatabaseInstance &db) {
	GeoTypes::Register(db);
	CoreScalarFunctions::Register(db);
	CoreCastFunctions::Register(db);
	CoreTableFunctions::Register(db);
	CoreAggregateFunctions::Register(db);
	CoreOptimizerRules::Register(db);
	CoreScalarMacros::Register(db);
}

} // namespace core

} // namespace spatial