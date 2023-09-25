#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace proj {

struct ProjFunctions {

public:
	static void Register(DatabaseInstance &db);
};

} // namespace proj

} // namespace spatial