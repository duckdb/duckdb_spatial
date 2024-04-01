#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/aggregate.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "geos_c.h"

namespace spatial {

namespace geos {

struct GEOSAggState {
	GEOSGeometry *geom = nullptr;
	GEOSContextHandle_t context = nullptr;

	~GEOSAggState() {
		if (geom) {
			GEOSGeom_destroy_r(context, geom);
			geom = nullptr;
		}
		if (context) {
			GEOS_finish_r(context);
			context = nullptr;
		}
	}
};

//------------------------------------------------------------------------
// INTERSECTION
//------------------------------------------------------------------------
struct IntersectionAggFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.geom = nullptr;
		state.context = GEOS_init_r();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &data) {
		if (!source.geom) {
			return;
		}
		if (!target.geom) {
			target.geom = GEOSGeom_clone_r(target.context, source.geom);
			return;
		}
		auto curr = target.geom;
		target.geom = GEOSIntersection_r(target.context, curr, source.geom);
		GEOSGeom_destroy_r(target.context, curr);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (!state.geom) {
			state.geom = DeserializeGEOSGeometry(input, state.context);
		} else {
			auto next = DeserializeGEOSGeometry(input, state.context);
			auto curr = state.geom;
			state.geom = GEOSIntersection_r(state.context, curr, next);
			GEOSGeom_destroy_r(state.context, next);
			GEOSGeom_destroy_r(state.context, curr);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		// There is no point in doing anything else, intersection is idempotent
		if (!state.geom) {
			state.geom = DeserializeGEOSGeometry(input, state.context);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.geom) {
			finalize_data.ReturnNull();
		} else {
			target = SerializeGEOSGeometry(finalize_data.result, state.geom, state.context);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.geom) {
			GEOSGeom_destroy_r(state.context, state.geom);
			state.geom = nullptr;
		}
		if (state.context) {
			GEOS_finish_r(state.context);
			state.context = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

//------------------------------------------------------------------------
// UNION
//------------------------------------------------------------------------

struct UnionAggFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.geom = nullptr;
		state.context = GEOS_init_r();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &data) {
		if (!source.geom) {
			return;
		}
		if (!target.geom) {
			target.geom = GEOSGeom_clone_r(target.context, source.geom);
			return;
		}
		auto curr = target.geom;
		target.geom = GEOSUnion_r(target.context, curr, source.geom);
		GEOSGeom_destroy_r(target.context, curr);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (!state.geom) {
			state.geom = DeserializeGEOSGeometry(input, state.context);
		} else {
			auto next = DeserializeGEOSGeometry(input, state.context);
			auto curr = state.geom;
			state.geom = GEOSUnion_r(state.context, curr, next);
			GEOSGeom_destroy_r(state.context, next);
			GEOSGeom_destroy_r(state.context, curr);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		// There is no point in doing anything else, union is idempotent
		if (!state.geom) {
			state.geom = DeserializeGEOSGeometry(input, state.context);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.geom) {
			finalize_data.ReturnNull();
		} else {
			target = SerializeGEOSGeometry(finalize_data.result, state.geom, state.context);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.geom) {
			GEOSGeom_destroy_r(state.context, state.geom);
			state.geom = nullptr;
		}
		if (state.context) {
			GEOS_finish_r(state.context);
			state.context = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};
static constexpr const char *INTERSECTION_DOC_DESCRIPTION = R"(
    Computes the intersection of a set of geometries
)";
static constexpr const char *INTERSECTION_DOC_EXAMPLE = R"(

)";

static constexpr const char *UNION_DOC_DESCRIPTION = R"(
    Computes the union of a set of input geometries
)";

static constexpr const char *UNION_DOC_EXAMPLE = R"(

)";

//------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------
void GeosAggregateFunctions::Register(DatabaseInstance &db) {

	AggregateFunctionSet st_intersection_agg("ST_Intersection_Agg");
	st_intersection_agg.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<GEOSAggState, geometry_t, geometry_t, IntersectionAggFunction>(
	        core::GeoTypes::GEOMETRY(), core::GeoTypes::GEOMETRY()));

	ExtensionUtil::RegisterFunction(db, st_intersection_agg);
	DocUtil::AddDocumentation(db, "ST_Intersection_Agg", INTERSECTION_DOC_DESCRIPTION, INTERSECTION_DOC_EXAMPLE,
	                          DOC_TAGS);

	AggregateFunctionSet st_union_agg("ST_Union_Agg");
	st_union_agg.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<GEOSAggState, geometry_t, geometry_t, UnionAggFunction>(
	        core::GeoTypes::GEOMETRY(), core::GeoTypes::GEOMETRY()));

	ExtensionUtil::RegisterFunction(db, st_union_agg);
	DocUtil::AddDocumentation(db, "ST_Union_Agg", UNION_DOC_DESCRIPTION, UNION_DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial