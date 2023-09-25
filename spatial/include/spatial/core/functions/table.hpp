#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreTableFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterOsmTableFunction(db);
	}

private:
	static void RegisterOsmTableFunction(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial