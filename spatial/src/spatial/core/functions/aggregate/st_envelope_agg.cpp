#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/aggregate.hpp"

namespace spatial {

namespace core {

struct EnvelopeAggState {
	bool is_set;
	double xmin;
	double xmax;
	double ymin;
	double ymax;
};

//------------------------------------------------------------------------
// ENVELOPE AGG
//------------------------------------------------------------------------
struct EnvelopeAggFunction {
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
		BoundingBox bbox;
		if (!state.is_set) {
			if (GeometryFactory::TryGetSerializedBoundingBox(input, bbox)) {
				state.is_set = true;
				state.xmin = bbox.minx;
				state.xmax = bbox.maxx;
				state.ymin = bbox.miny;
				state.ymax = bbox.maxy;
			}
		} else {
			if (GeometryFactory::TryGetSerializedBoundingBox(input, bbox)) {
				state.xmin = std::min(state.xmin, bbox.minx);
				state.xmax = std::max(state.xmax, bbox.maxx);
				state.ymin = std::min(state.ymin, bbox.miny);
				state.ymax = std::max(state.ymax, bbox.maxy);
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
			auto &arena_alloc = finalize_data.input.allocator;
			auto &alloc = arena_alloc.GetAllocator();
			GeometryFactory factory(alloc);
			auto box = factory.CreateBox(state.xmin, state.ymin, state.xmax, state.ymax);
			target = factory.Serialize(finalize_data.result, box);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

//------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------
void CoreAggregateFunctions::RegisterStEnvelopeAgg(ClientContext &context) {

	auto &catalog = Catalog::GetSystemCatalog(context);

	AggregateFunctionSet st_envelope_agg("st_envelope_agg");
	st_envelope_agg.AddFunction(
	    AggregateFunction::UnaryAggregate<EnvelopeAggState, string_t, string_t, EnvelopeAggFunction>(
	        core::GeoTypes::GEOMETRY(), core::GeoTypes::GEOMETRY()));
	CreateAggregateFunctionInfo envelope_info(std::move(st_envelope_agg));

	envelope_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, envelope_info);
}

} // namespace core

} // namespace spatial