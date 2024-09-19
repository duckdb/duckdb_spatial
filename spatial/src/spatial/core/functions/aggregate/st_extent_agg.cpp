#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/bbox.hpp"
#include "spatial/core/functions/aggregate.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

struct ExtentAggState {
	bool is_set;
	double xmin;
	double xmax;
	double ymin;
	double ymax;
};

//------------------------------------------------------------------------
// ENVELOPE AGG
//------------------------------------------------------------------------
struct ExtentAggFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.is_set = false;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		if (!target.is_set) {
			target = source;
			return;
		}
		target.xmin = std::min(target.xmin, source.xmin);
		target.xmax = std::max(target.xmax, source.xmax);
		target.ymin = std::min(target.ymin, source.ymin);
		target.ymax = std::max(target.ymax, source.ymax);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		Box2D<double> bbox;
		if (!state.is_set) {
			if (input.TryGetCachedBounds(bbox)) {
				state.is_set = true;
				state.xmin = bbox.min.x;
				state.xmax = bbox.max.x;
				state.ymin = bbox.min.y;
				state.ymax = bbox.max.y;
			}
		} else {
			if (input.TryGetCachedBounds(bbox)) {
				state.xmin = std::min(state.xmin, bbox.min.x);
				state.xmax = std::max(state.xmax, bbox.max.x);
				state.ymin = std::min(state.ymin, bbox.min.y);
				state.ymax = std::max(state.ymax, bbox.max.y);
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &agg, idx_t) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, agg);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			auto &arena = finalize_data.input.allocator;
			auto box = Polygon::CreateFromBox(arena, state.xmin, state.ymin, state.xmax, state.ymax);
			target = Geometry::Serialize(box, finalize_data.result);
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
static constexpr const char *DOC_DESCRIPTION = R"(
    Computes the minimal-bounding-box polygon containing the set of input geometries
)";
static constexpr const char *DOC_EXAMPLE = R"(
	SELECT ST_Extent_Agg(geom) FROM UNNEST([ST_Point(1,1), ST_Point(5,5)]) AS _(geom);
	-- POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))
)";

static constexpr const char* DOC_ALIAS_DESCRIPTION = R"(
	Alias for [ST_Extent_Agg](#st_extent_agg).

	Computes the minimal-bounding-box polygon containing the set of input geometries.
)";

//------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------
void CoreAggregateFunctions::RegisterStExtentAgg(DatabaseInstance &db) {

	auto function = AggregateFunction::UnaryAggregate<ExtentAggState, geometry_t, geometry_t, ExtentAggFunction>(
			GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY());

	// Register the function
	function.name = "ST_Extent_Agg";
	ExtensionUtil::RegisterFunction(db, function);
	DocUtil::AddDocumentation(db, "ST_Extent_Agg", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);

	// Also add an alias with the name ST_Envelope_Agg
	function.name = "ST_Envelope_Agg";
	ExtensionUtil::RegisterFunction(db, function);
	DocUtil::AddDocumentation(db, "ST_Envelope_Agg", DOC_ALIAS_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);

}

} // namespace core

} // namespace spatial