#include "spatial/geos/module.hpp"
#include "spatial/geos/functions/aggregate.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/cast.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace geos {

void GeosModule::Register(DatabaseInstance &db) {
	GEOSScalarFunctions::Register(db);
	GeosAggregateFunctions::Register(db);
	GeosCastFunctions::Register(db);
}

} // namespace geos

} // namespace spatial
