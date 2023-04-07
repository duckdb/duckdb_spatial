#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreTableFunctions {
public:
	static void Register(ClientContext &context) {
        RegisterOsmTableFunction(context);
    }
private:
    static void RegisterOsmTableFunction(ClientContext &context);
};

} // namespace core

} // namespace spatial