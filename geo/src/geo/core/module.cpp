
#include "geo/core/module.hpp"

#include "geo/common.hpp"
#include "geo/core/functions.hpp"
#include "geo/core/layout_benchmark/test.hpp"
#include "geo/core/types.hpp"

namespace geo {

namespace core {

void CoreModule::Register(ClientContext &context) {
	GeoTypes::Register(context);
	GeometryFunctions::Register(context);
	LayoutBenchmark::Register(context);
}

} // namespace core

} // namespace geo