#include "spatial/geos/module.hpp"
#include "spatial/geos/functions/aggregate.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/cast.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace geos {

void GeosModule::Register(DatabaseInstance &instance) {
	GEOSScalarFunctions::Register(instance);
	GeosAggregateFunctions::Register(instance);
	GeosCastFunctions::Register(instance);
}

} // namespace geos

} // namespace spatial
