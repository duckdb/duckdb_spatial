#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreOptimizerRules {
public:
	static void Register(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial