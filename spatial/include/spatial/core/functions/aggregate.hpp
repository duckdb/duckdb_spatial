#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreAggregateFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStExtentAgg(db);
	}

private:
	static void RegisterStExtentAgg(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial