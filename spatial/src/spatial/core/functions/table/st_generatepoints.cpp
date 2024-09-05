#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

#include <spatial/core/geometry/bbox.hpp>

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
struct GeneratePointsBindData final : public TableFunctionData {
	idx_t count = 0;
	int64_t seed = -1;
	Box2D<double> bbox;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GeneratePointsBindData>();

	return_types.push_back(GeoTypes::POINT_2D());
	names.push_back("point");

	// Extract the bounding box
	const auto &box_value = input.inputs[0];
	auto &box_components = StructValue::GetChildren(box_value);
	result->bbox.min.x = box_components[0].GetValue<double>();
	result->bbox.min.y = box_components[1].GetValue<double>();
	result->bbox.max.x = box_components[2].GetValue<double>();
	result->bbox.max.y = box_components[3].GetValue<double>();

	// Extract the count
	const auto &count_value = input.inputs[1];
	const auto count = count_value.GetValue<int64_t>();
	if (count < 0) {
		throw BinderException("Count must be a non-negative integer");
	}
	result->count = UnsafeNumericCast<idx_t>(count);

	// Extract the seed (optional)
	if (input.inputs.size() == 3) {
		result->seed = input.inputs[2].GetValue<int64_t>();
	}

	return std::move(result);
}

//------------------------------------------------------------------------------
// Init
//------------------------------------------------------------------------------
struct GeneratePointsState final : public GlobalTableFunctionState {
	RandomEngine rng;
	idx_t current_idx;

	explicit GeneratePointsState(const int64_t seed) : rng(seed), current_idx(0) {
	}
};

static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<GeneratePointsBindData>();
	auto result = make_uniq<GeneratePointsState>(bind_data.seed);
	return std::move(result);
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------
static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<GeneratePointsBindData>();
	auto &state = data_p.global_state->Cast<GeneratePointsState>();

	const auto &point_vec = StructVector::GetEntries(output.data[0]);
	const auto &x_data = FlatVector::GetData<double>(*point_vec[0]);
	const auto &y_data = FlatVector::GetData<double>(*point_vec[1]);

	const auto chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.count - state.current_idx);
	for (idx_t i = 0; i < chunk_size; i++) {

		x_data[i] = state.rng.NextRandom(bind_data.bbox.min.x, bind_data.bbox.max.x);
		y_data[i] = state.rng.NextRandom(bind_data.bbox.min.y, bind_data.bbox.max.y);

		state.current_idx++;
	}
	output.SetCardinality(chunk_size);
}

//------------------------------------------------------------------------------
// Cardinality
//------------------------------------------------------------------------------
unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<GeneratePointsBindData>();
	return make_uniq<NodeStatistics>(bind_data.count, bind_data.count);
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterGeneratePointsTableFunction(DatabaseInstance &db) {
	TableFunctionSet set("ST_GeneratePoints");

	TableFunction generate_points({GeoTypes::BOX_2D(), LogicalType::BIGINT}, Execute, Bind, Init);
	generate_points.cardinality = Cardinality;

	// Overload without seed
	set.AddFunction(generate_points);

	// Overload with seed
	generate_points.arguments = {GeoTypes::BOX_2D(), LogicalType::BIGINT, LogicalType::BIGINT};
	set.AddFunction(generate_points);
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
