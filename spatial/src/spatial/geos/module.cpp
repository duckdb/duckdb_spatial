#include "spatial/geos/module.hpp"
#include "spatial/geos/functions/aggregate.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/cast.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace geos {

void GeosModule::Register(ClientContext &context) {
	GEOSScalarFunctions::Register(context);
	GeosAggregateFunctions::Register(context);
	GeosCastFunctions::Register(context);
}

} // namespace geos

} // namespace spatial
