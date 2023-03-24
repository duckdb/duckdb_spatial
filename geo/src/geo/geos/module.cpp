#include "geo/geos/module.hpp"
#include "geo/geos/functions/aggregate.hpp"
#include "geo/geos/functions/scalar.hpp"
#include "geo/geos/functions/cast.hpp"

#include "geo/common.hpp"

namespace geo {

namespace geos {

void GeosModule::Register(ClientContext &context) {
	GEOSScalarFunctions::Register(context);
	GeosAggregateFunctions::Register(context);
	GeosCastFunctions::Register(context);
}

} // namespace geos

} // namespace geo
