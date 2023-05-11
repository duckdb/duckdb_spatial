#pragma once

#include "spatial/common.hpp"

namespace spatial {

namespace geographiclib {

struct GeographicLibFunctions {
public:
	static void Register(ClientContext &context) {
        RegisterDistance(context);
        RegisterLength(context);
        RegisterArea(context);
        RegisterPerimeter(context);
    }
private:
    static void RegisterDistance(ClientContext &context);
    static void RegisterLength(ClientContext &context);
    static void RegisterArea(ClientContext &context);
    static void RegisterPerimeter(ClientContext &context);
};

}

}