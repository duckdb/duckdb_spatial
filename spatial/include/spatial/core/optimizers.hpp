#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreOptimizerRules {
public:
	static void Register(DatabaseInstance &instance);
};

} // namespace core

} // namespace spatial