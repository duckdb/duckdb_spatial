#pragma once

#include "geo/common.hpp"

#include "proj.h"

namespace geo {

namespace proj {

struct ProjModule {
public:
	static PJ_CONTEXT *GetThreadProjContext();
	static void Register(ClientContext &context);
};

} // namespace proj

} // namespace geo