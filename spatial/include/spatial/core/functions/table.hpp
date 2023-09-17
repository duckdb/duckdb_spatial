#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreTableFunctions {
public:
	static void Register(DatabaseInstance &instance) {
		RegisterOsmTableFunction(instance);
	}

private:
	static void RegisterOsmTableFunction(DatabaseInstance &instance);
};

} // namespace core

} // namespace spatial