#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions/aggregate.hpp"

#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace geo {

namespace core {


struct Box2DAggState {
	bool is_set;
	double min_x;
	double min_y;
	double max_x;
	double max_y;
};

struct Box2DAggFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->is_set = false;
		state->min_x = std::numeric_limits<double>::max();
		state->min_y = std::numeric_limits<double>::max();
		state->max_x = std::numeric_limits<double>::lowest();
		state->max_y = std::numeric_limits<double>::lowest();
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		// do nothing
	}

	static bool IgnoreNull() {
		return true;
	}
};

static void Update(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state_vector,
                   idx_t count) {

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto states = (Box2DAggState **)sdata.data;

	auto &entries = StructVector::GetEntries(input);
	auto &in_min_x = *entries[0];
	auto &in_min_y = *entries[1];
	auto &in_max_x = *entries[2];
	auto &in_max_y = *entries[3];

	auto min_x = FlatVector::GetData<double>(in_min_x);
	auto min_y = FlatVector::GetData<double>(in_min_y);
	auto max_x = FlatVector::GetData<double>(in_max_x);
	auto max_y = FlatVector::GetData<double>(in_max_y);

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];

		state->is_set = true;
		state->min_x = std::min(state->min_x, min_x[i]);
		state->min_y = std::min(state->min_y, min_y[i]);
		state->max_x = std::max(state->max_x, max_x[i]);
		state->max_y = std::max(state->max_y, max_y[i]);
	}
}

static void Combine(Vector &state_vec, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vec.ToUnifiedFormat(count, sdata);
	auto states_ptr = (Box2DAggState **)sdata.data;

	auto combined_ptr = FlatVector::GetData<Box2DAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];

		if (!state->is_set) {
			// Not set, skip
			continue;
		}

		combined_ptr[i]->is_set = true;
		combined_ptr[i]->min_x = std::min(combined_ptr[i]->min_x, state->min_x);
		combined_ptr[i]->min_y = std::min(combined_ptr[i]->min_y, state->min_y);
		combined_ptr[i]->max_x = std::max(combined_ptr[i]->max_x, state->max_x);
		combined_ptr[i]->max_y = std::max(combined_ptr[i]->max_y, state->max_y);
	}
}

static void Finalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                     idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (Box2DAggState **)sdata.data;

	auto &entries = StructVector::GetEntries(result);
	auto &out_min_x = *entries[0];
	auto &out_min_y = *entries[1];
	auto &out_max_x = *entries[2];
	auto &out_max_y = *entries[3];

	for (idx_t i = 0; i < count; i++) {

		auto state = states[sdata.sel->get_index(i)];
		const auto rid = i + offset;

		FlatVector::GetData<double>(out_min_x)[rid] = state->min_x;
		FlatVector::GetData<double>(out_min_y)[rid] = state->min_y;
		FlatVector::GetData<double>(out_max_x)[rid] = state->max_x;
		FlatVector::GetData<double>(out_max_y)[rid] = state->max_y;
	}
}

void CoreAggregateFunctions::Register(ClientContext &context) {
	auto box_2d_agg = AggregateFunction("st_box2d_agg", {GeoTypes::BOX_2D}, GeoTypes::BOX_2D,
	                           AggregateFunction::StateSize<Box2DAggState>,
	                           AggregateFunction::StateInitialize<Box2DAggState, Box2DAggFunction>, Update, Combine,
	                           Finalize, nullptr, nullptr);

	CreateAggregateFunctionInfo box_2d_info(box_2d_agg);
	box_2d_info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	Catalog::GetSystemCatalog(context).CreateFunction(context, &box_2d_info);
}

} // namespace core

} // namespace geo