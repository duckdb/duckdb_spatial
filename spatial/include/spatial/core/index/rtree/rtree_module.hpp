#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct RTreeModule {
	static void RegisterIndex(DatabaseInstance &db);
	static void RegisterIndexScan(DatabaseInstance &db);
	static void RegisterIndexPlanScan(DatabaseInstance &db);
	static void RegisterIndexPlanCreate(DatabaseInstance &db);
	static void RegisterIndexPragmas(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial