#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/table.hpp"

namespace spatial {

namespace core {

void CoreTableFunctions::RegisterTestTableFunctions(DatabaseInstance &db) {

	// TODO:
	// TableFunction test_geometry_types("test_geometry_types", {}, Execute, Bind, Init);
	// ExtensionUtil::RegisterFunction(db, test_geometry_types);
}

} // namespace core

} // namespace spatial
