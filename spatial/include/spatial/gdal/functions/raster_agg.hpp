#pragma once
#include "duckdb/function/aggregate_function.hpp"
#include "spatial/common.hpp"

class GDALDataset;

namespace spatial {

namespace gdal {

struct RasterAggState {
	bool is_set;
	std::vector<GDALDataset *> datasets;
};

struct RasterAggUnaryOperation {

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
		target.datasets.insert(target.datasets.end(), source.datasets.begin(), source.datasets.end());
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		state.is_set = true;
		state.datasets.emplace_back(dataset);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &agg, idx_t) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, agg);
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct RasterAggBinaryOperation {

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
		target.datasets.insert(target.datasets.end(), source.datasets.begin(), source.datasets.end());
	}

	template <class INPUT_TYPE, class OPTS_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, const OPTS_TYPE &opts, AggregateBinaryInput &) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		state.is_set = true;
		state.datasets.emplace_back(dataset);
	}

	template <class INPUT_TYPE, class OPTS_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, const OPTS_TYPE &opts,
	                              AggregateBinaryInput &agg, idx_t) {
		Operation<INPUT_TYPE, OPTS_TYPE, STATE, OP>(state, input, opts, agg);
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct RasterAggBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;
	//! The list of options for the function
	std::vector<std::string> options;

	explicit RasterAggBindData(ClientContext &context, std::vector<std::string> options)
	    : context(context), options(options) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RasterAggBindData>(context, options);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RasterAggBindData>();
		return options == other.options;
	}
};

unique_ptr<FunctionData> BindRasterAggOperation(ClientContext &context, AggregateFunction &function,
                                                vector<unique_ptr<Expression>> &arguments);

} // namespace gdal

} // namespace spatial
