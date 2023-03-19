#pragma once
#include "geo/common.hpp"

namespace geo {

namespace core {

struct CoreAggregateFunctions {
	static void Register(ClientContext &context);
};

} // namespace core

} // namespace geo