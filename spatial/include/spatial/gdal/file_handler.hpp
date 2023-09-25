#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalFileHandler {
	static void Register();

	// This is a workaround to allow the global file handler to access the current client context
	// by storing it in a thread_local variable before executing a GDAL IO operation
	static void SetLocalClientContext(ClientContext &context);
	static ClientContext &GetLocalClientContext();
};

} // namespace gdal

} // namespace spatial
