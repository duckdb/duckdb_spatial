#pragma once

#include "spatial/geometry/flat_rtree.hpp"
#include <cstdint>
#include <vector>
#include <stdexcept>
#include <string>

namespace duckdb {
namespace spatial {

// Point cluster assignment status
enum class ClusterStatus : int32_t {
	UNVISITED = -2,
	NOISE = -1
	// Non-negative values (0, 1, 2, ...) represent valid cluster assignments
};

// Input parameters for DBSCAN clustering
struct DBSCANParams {
	double eps;
	int64_t min_points;

	DBSCANParams() : eps(0.0), min_points(1) {
	}
	DBSCANParams(double eps_p, int64_t min_points_p) : eps(eps_p), min_points(min_points_p) {
	}

	void Validate() const {
		if (!std::isfinite(eps) || eps < 0.0) {
			throw std::invalid_argument("DBSCAN parameter 'eps' must be finite and non-negative");
		}
		if (min_points < 0) {
			throw std::invalid_argument("DBSCAN parameter 'minpoints' must be non-negative");
		}
	}
};

// Result container for DBSCAN clustering execution
class DBSCANResult {
public:
	DBSCANResult() : num_clusters_(0), num_noise_(0) {
	}

	explicit DBSCANResult(size_t point_count)
	    : cluster_ids_(point_count, static_cast<int32_t>(ClusterStatus::UNVISITED)), num_clusters_(0), num_noise_(0) {
	}

	size_t Size() const {
		return cluster_ids_.size();
	}
	size_t NumClusters() const {
		return num_clusters_;
	}
	size_t NumNoise() const {
		return num_noise_;
	}

	int32_t GetClusterId(size_t idx) const {
		return cluster_ids_[idx];
	}

	void SetClusterId(size_t idx, int32_t cid) {
		cluster_ids_[idx] = cid;
	}

	bool IsNoise(size_t idx) const {
		return cluster_ids_[idx] == static_cast<int32_t>(ClusterStatus::NOISE);
	}

	bool IsUnvisited(size_t idx) const {
		return cluster_ids_[idx] == static_cast<int32_t>(ClusterStatus::UNVISITED);
	}

	bool IsClustered(size_t idx) const {
		return cluster_ids_[idx] >= 0;
	}

	const std::vector<int32_t> &GetClusterIds() const {
		return cluster_ids_;
	}

	std::vector<int32_t> &GetClusterIdsMutable() {
		return cluster_ids_;
	}

	void FinalizeMetrics(size_t cluster_count) {
		num_clusters_ = cluster_count;
		num_noise_ = 0;
		for (size_t i = 0; i < cluster_ids_.size(); ++i) {
			if (cluster_ids_[i] == static_cast<int32_t>(ClusterStatus::NOISE)) {
				num_noise_++;
			}
		}
	}

private:
	std::vector<int32_t> cluster_ids_;
	size_t num_clusters_;
	size_t num_noise_;
};

class DBSCANEngine {
public:
	static DBSCANResult Cluster2D(const FlatRTree2D &index, const DBSCANParams &params);
};

} // namespace spatial
} // namespace duckdb
