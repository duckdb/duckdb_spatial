#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreAggregateFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterStEnvelopeAgg(context);
	}

private:
	static void RegisterStEnvelopeAgg(ClientContext &context);
};

} // namespace core

} // namespace spatial