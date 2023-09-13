#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreOptimizerRules {
public:
	static void Register(ClientContext &context);
};

} // namespace core

} // namespace spatial