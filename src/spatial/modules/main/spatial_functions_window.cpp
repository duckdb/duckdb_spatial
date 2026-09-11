#include "spatial/geometry/flat_rtree.hpp"
#include "spatial/geometry/dbscan_engine.hpp"
#include "spatial/modules/main/spatial_functions.hpp"
#include "spatial/spatial_types.hpp"
#include "spatial/util/function_builder.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
namespace {

struct DBSCANWindowState {
	std::vector<int32_t> cluster_ids;
};

struct ST_ClusterDBSCAN_Point2D {
	static spatial::DBSCANParams ReadParameters(DataChunk &chunk, idx_t row) {
		const auto eps = chunk.data[1].GetValue(row);
		const auto min_points = chunk.data[2].GetValue(row);
		if (eps.IsNull() || min_points.IsNull()) {
			throw InvalidInputException("ST_ClusterDBSCAN parameters must not be NULL");
		}
		spatial::DBSCANParams params(eps.GetValue<double>(), min_points.GetValue<int64_t>());
		try {
			params.Validate();
		} catch (const std::invalid_argument &error) {
			throw InvalidInputException("%s", error.what());
		}
		return params;
	}

	static idx_t StateSize(const AggregateFunction &) {
		return sizeof(DBSCANWindowState);
	}

	static void StateInitialize(const AggregateFunction &, data_ptr_t state) {
		new (state) DBSCANWindowState();
	}

	static void StateDestructor(Vector &state, AggregateInputData &, idx_t count) {
		UnifiedVectorFormat sdata;
		state.ToUnifiedFormat(count, sdata);
		auto states = UnifiedVectorFormat::GetData<DBSCANWindowState *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = sdata.sel->get_index(i);
			if (sdata.validity.RowIsValid(idx)) {
				states[idx]->~DBSCANWindowState();
			}
		}
	}

	static void ClusterPartition(const std::vector<spatial::Point2D> &points, const std::vector<size_t> &rows,
	                             const spatial::DBSCANParams &params, DBSCANWindowState &state,
	                             ClientContext &context) {
		if (points.empty()) {
			return;
		}
		spatial::FlatRTree2D index(32, [&context]() {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
		});
		index.Build(points);
		const auto clusters = spatial::DBSCANEngine::Cluster2D(index, params);
		for (size_t i = 0; i < rows.size(); i++) {
			state.cluster_ids[rows[i]] = clusters.GetClusterId(i);
		}
	}

	static void WindowInit(AggregateInputData &, const WindowPartitionInput &partition, data_ptr_t g_state) {
		auto &state = *reinterpret_cast<DBSCANWindowState *>(g_state);
		state.cluster_ids.assign(partition.count, -1);
		if (partition.count == 0) {
			return;
		}
		if (!partition.inputs || !partition.partition_mask || partition.column_ids.size() != 3) {
			throw InternalException("ST_ClusterDBSCAN requires window input and SQL partition boundaries");
		}

		// Only the current SQL partition's points are retained. The result is shared
		// read-only by evaluators, indexed by absolute position in the hash group.
		std::vector<spatial::Point2D> points;
		std::vector<size_t> rows;
		spatial::DBSCANParams params;
		idx_t row_offset = 0;
		for (auto &chunk : partition.inputs->Chunks(partition.column_ids)) {
			if (partition.context.client.IsInterrupted()) {
				throw InterruptException();
			}
			auto &point_vector = chunk.data[0];
			point_vector.Flatten(chunk.size());
			auto &coordinates = StructVector::GetEntries(point_vector);
			coordinates[0]->Flatten(chunk.size());
			coordinates[1]->Flatten(chunk.size());
			const auto x = FlatVector::GetData<double>(*coordinates[0]);
			const auto y = FlatVector::GetData<double>(*coordinates[1]);
			auto &point_validity = FlatVector::Validity(point_vector);
			auto &x_validity = FlatVector::Validity(*coordinates[0]);
			auto &y_validity = FlatVector::Validity(*coordinates[1]);

			for (idx_t i = 0; i < chunk.size(); i++) {
				const auto row = row_offset + i;
				const auto row_params = ReadParameters(chunk, i);
				if (row == 0 || partition.partition_mask->RowIsValid(row)) {
					ClusterPartition(points, rows, params, state, partition.context.client);
					points.clear();
					rows.clear();
					params = row_params;
				} else if (params.eps != row_params.eps || params.min_points != row_params.min_points) {
					throw InvalidInputException("ST_ClusterDBSCAN parameters must be constant within each partition");
				}
				if (!partition.filter_mask.RowIsValid(row) || !point_validity.RowIsValid(i) ||
				    !x_validity.RowIsValid(i) || !y_validity.RowIsValid(i)) {
					continue;
				}
				if (!std::isfinite(x[i]) || !std::isfinite(y[i])) {
					throw InvalidInputException("ST_ClusterDBSCAN coordinates must be finite");
				}
				points.emplace_back(x[i], y[i]);
				rows.push_back(row);
			}
			row_offset += chunk.size();
		}
		ClusterPartition(points, rows, params, state, partition.context.client);
	}

	static void Window(AggregateInputData &, const WindowPartitionInput &partition, const_data_ptr_t g_state,
	                   data_ptr_t, const SubFrames &, Vector &result, idx_t rid) {
		auto &wstate = *reinterpret_cast<const DBSCANWindowState *>(g_state);
		const auto global_row = partition.row_index;
		if (global_row >= wstate.cluster_ids.size() || wstate.cluster_ids[global_row] < 0) {
			FlatVector::SetNull(result, rid, true);
		} else {
			FlatVector::GetData<int32_t>(result)[rid] = wstate.cluster_ids[global_row];
		}
	}
};

} // namespace

void RegisterSpatialWindowFunctions(ExtensionLoader &loader) {
	// Register ST_ClusterDBSCAN for POINT_2D
	AggregateFunction cluster_point2d(
	    "ST_ClusterDBSCAN", {GeoTypes::POINT_2D(), LogicalType::DOUBLE, LogicalType::BIGINT}, LogicalType::INTEGER,
	    ST_ClusterDBSCAN_Point2D::StateSize, ST_ClusterDBSCAN_Point2D::StateInitialize,
	    nullptr, // update (null for window-only aggregate)
	    nullptr, // combine
	    nullptr, // finalize
	    nullptr  // simple_update
	);

	cluster_point2d.destructor = ST_ClusterDBSCAN_Point2D::StateDestructor;
	cluster_point2d.window_init = ST_ClusterDBSCAN_Point2D::WindowInit;
	cluster_point2d.window = ST_ClusterDBSCAN_Point2D::Window;

	FunctionBuilder::RegisterAggregate(loader, "ST_ClusterDBSCAN", [&](AggregateFunctionBuilder &func) {
		func.SetFunction(cluster_point2d);
		func.SetDescription(R"(
            Returns a zero-based DBSCAN cluster ID for each POINT_2D in its SQL partition, or NULL for noise.
            eps must be finite and non-negative; minpoints must be non-negative. Both parameters must be
            non-NULL and constant within each partition. A zero radius clusters coincident points.
            NULL points and NULL coordinates are excluded. Other coordinates must be finite.
            FILTER excludes points and returns NULL for excluded rows. Clustering uses the whole partition,
            regardless of frame or EXCLUDE clauses. DISTINCT is unsupported. Use a window ORDER BY with
            a unique key for reproducible border-point assignments. Distances use the input coordinate units.
        )");
		func.SetExample("SELECT id, ST_ClusterDBSCAN(pt, 0.5, 5) OVER (ORDER BY id) AS cid FROM points;");
		func.CanThrowErrors();
		func.SetTag("ext", "spatial");
		func.SetTag("category", "clustering");
	});
}

} // namespace duckdb
