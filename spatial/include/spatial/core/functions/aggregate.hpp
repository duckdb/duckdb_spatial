#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreAggregateFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStEnvelopeAgg(db);
	}

private:
	static void RegisterStEnvelopeAgg(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial