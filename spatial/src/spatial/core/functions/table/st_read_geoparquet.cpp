#define DUCKDB_EXTENSION_MAIN

#include "parquet_extension.hpp"

#include "duckdb.hpp"
#include "parquet_metadata.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table/row_group.hpp"
#endif


namespace spatial {
namespace core {
namespace geoparquet {

enum class ParquetFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

struct BindData : public TableFunctionData {
	shared_ptr<ParquetReader> initial_reader;
	vector<string> files;
	atomic<idx_t> chunk_count;
	atomic<idx_t> cur_file;
	vector<string> names;
	vector<LogicalType> types;

	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParallelStateNext
	vector<shared_ptr<ParquetReader>> union_readers;

	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	ParquetOptions parquet_options;
	MultiFileReaderBindData reader_bind;

	void Initialize(shared_ptr<duckdb::ParquetReader> reader) {
		initial_reader = std::move(reader);
		initial_file_cardinality = initial_reader->NumRows();
		initial_file_row_groups = initial_reader->NumRowGroups();
		parquet_options = initial_reader->parquet_options;
	}
};

struct GlobalState : public GlobalTableFunctionState {
	mutex lock;

	//! The initial reader from the bind phase
	shared_ptr<ParquetReader> initial_reader;
	//! Currently opened readers
	vector<shared_ptr<ParquetReader>> readers;
	//! Flag to indicate a file is being opened
	vector<ParquetFileState> file_states;
	//! Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;
	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	idx_t file_index;
	//! Index of row group within file currently up for scanning
	idx_t row_group_index;
	//! Batch index of the next row group to be scanned
	idx_t batch_index;

	idx_t max_threads;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
	vector<column_t> column_ids;
	TableFilterSet *filters;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const  {
		return !projection_ids.empty();
	}

};

struct LocalState : public LocalTableFunctionState {
	shared_ptr<ParquetReader> reader;
	ParquetReaderScanState scan_state;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
};

static void WaitForFile(idx_t file_index, GlobalState &parallel_state,
                        unique_lock<mutex> &parallel_lock);

static bool ParallelStateNext(ClientContext &context, const BindData &bind_data,
                              LocalState &scan_data, GlobalState &parallel_state);

static bool TryOpenNextFile(ClientContext &context, const BindData &bind_data,
                            LocalState &scan_data, GlobalState &parallel_state,
                            unique_lock<mutex> &parallel_lock);

//static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
//                                                   column_t column_index) {
//	auto &bind_data = bind_data_p->Cast<BindData>();
//
//	if (IsRowIdColumnId(column_index)) {
//		return nullptr;
//	}
//
//	// NOTE: we do not want to parse the Parquet metadata for the sole purpose of getting column statistics
//
//	auto &config = DBConfig::GetConfig(context);
//	if (bind_data.files.size() < 2) {
//		if (bind_data.initial_reader) {
//			// most common path, scanning single parquet file
//			return bind_data.initial_reader->ReadStatistics(bind_data.names[column_index]);
//		} else if (!config.options.object_cache_enable) {
//			// our initial reader was reset
//			return nullptr;
//		}
//	} else if (config.options.object_cache_enable) {
//		// multiple files, object cache enabled: merge statistics
//		unique_ptr<BaseStatistics> overall_stats;
//
//		auto &cache = ObjectCache::GetObjectCache(context);
//		// for more than one file, we could be lucky and metadata for *every* file is in the object cache (if
//		// enabled at all)
//		FileSystem &fs = FileSystem::GetFileSystem(context);
//
//		for (idx_t file_idx = 0; file_idx < bind_data.files.size(); file_idx++) {
//			auto &file_name = bind_data.files[file_idx];
//			auto metadata = cache.Get<ParquetFileMetadataCache>(file_name);
//			if (!metadata) {
//				// missing metadata entry in cache, no usable stats
//				return nullptr;
//			}
//			auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
//			// we need to check if the metadata cache entries are current
//			if (fs.GetLastModifiedTime(*handle) >= metadata->read_time) {
//				// missing or invalid metadata entry in cache, no usable stats overall
//				return nullptr;
//			}
//			ParquetReader reader(context, bind_data.parquet_options, metadata);
//			// get and merge stats for file
//			auto file_stats = reader.ReadStatistics(bind_data.names[column_index]);
//			if (!file_stats) {
//				return nullptr;
//			}
//			if (overall_stats) {
//				overall_stats->Merge(*file_stats);
//			} else {
//				overall_stats = std::move(file_stats);
//			}
//		}
//		// success!
//		return overall_stats;
//	}
//
//	// multiple files and no object cache, no luck!
//	return nullptr;
//}

//static unique_ptr<FunctionData> ParquetScanBindInternal(ClientContext &context, vector<string> files,
//                                                        vector<LogicalType> &return_types, vector<string> &names,
//                                                        ParquetOptions parquet_options) {
//	auto result = make_uniq<BindData>();
//	result->files = std::move(files);
//	result->reader_bind =
//	    MultiFileReader::BindReader<ParquetReader>(context, result->types, result->names, *result, parquet_options);
//	if (return_types.empty()) {
//		// no expected types - just copy the types
//		return_types = result->types;
//		names = result->names;
//	} else {
//		if (return_types.size() != result->types.size()) {
//			throw std::runtime_error(StringUtil::Format(
//			    "Failed to read file \"%s\" - column count mismatch: expected %d columns but found %d",
//			    result->files[0], return_types.size(), result->types.size()));
//		}
//		// expected types - overwrite the types we want to read instead
//		result->types = return_types;
//	}
//	return std::move(result);
//}
//
//static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
//                                                vector<LogicalType> &return_types, vector<string> &names) {
//	auto files = MultiFileReader::GetFileList(context, input.inputs[0], "Parquet");
//	ParquetOptions parquet_options(context);
//	for (auto &kv : input.named_parameters) {
//		auto loption = StringUtil::Lower(kv.first);
//		if (MultiFileReader::ParseOption(kv.first, kv.second, parquet_options.file_options, context)) {
//			continue;
//		}
//		if (loption == "binary_as_string") {
//			parquet_options.binary_as_string = BooleanValue::Get(kv.second);
//		} else if (loption == "file_row_number") {
//			parquet_options.file_row_number = BooleanValue::Get(kv.second);
//		}
//	}
//	parquet_options.file_options.AutoDetectHivePartitioning(files, context);
//	return ParquetScanBindInternal(context, std::move(files), return_types, names, parquet_options);
//}

//static double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
//                              const GlobalTableFunctionState *global_state) {
//	auto &bind_data = bind_data_p->Cast<BindData>();
//	if (bind_data.files.empty()) {
//		return 100.0;
//	}
//	if (bind_data.initial_file_cardinality == 0) {
//		return (100.0 * (bind_data.cur_file + 1)) / bind_data.files.size();
//	}
//	auto percentage = (bind_data.chunk_count * STANDARD_VECTOR_SIZE * 100.0 / bind_data.initial_file_cardinality) /
//	                  bind_data.files.size();
//	percentage += 100.0 * bind_data.cur_file / bind_data.files.size();
//	return percentage;
//}

//static unique_ptr<LocalTableFunctionState>
//ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
//	auto &bind_data = input.bind_data->Cast<BindData>();
//	auto &gstate = gstate_p->Cast<GlobalState>();
//
//	auto result = make_uniq<LocalState>();
//	result->is_parallel = true;
//	result->batch_index = 0;
//	if (input.CanRemoveFilterColumns()) {
//		result->all_columns.Initialize(context.client, gstate.scanned_types);
//	}
//	if (!ParallelStateNext(context.client, bind_data, *result, gstate)) {
//		return nullptr;
//	}
//	return std::move(result);
//}

//static unique_ptr<GlobalTableFunctionState> ParquetScanInitGlobal(ClientContext &context,
//                                                                  TableFunctionInitInput &input) {
//	auto &bind_data = input.bind_data->CastNoConst<BindData>();
//	auto result = make_uniq<GlobalState>();
//
//	result->file_states = vector<ParquetFileState>(bind_data.files.size(), ParquetFileState::UNOPENED);
//	result->file_mutexes = unique_ptr<mutex[]>(new mutex[bind_data.files.size()]);
//	if (bind_data.files.empty()) {
//		result->initial_reader = nullptr;
//	} else {
//		result->readers = std::move(bind_data.union_readers);
//		if (result->readers.size() != bind_data.files.size()) {
//			result->readers = vector<shared_ptr<ParquetReader>>(bind_data.files.size(), nullptr);
//		} else {
//			std::fill(result->file_states.begin(), result->file_states.end(), ParquetFileState::OPEN);
//		}
//		if (bind_data.initial_reader) {
//			result->initial_reader = std::move(bind_data.initial_reader);
//			result->readers[0] = result->initial_reader;
//		} else if (result->readers[0]) {
//			result->initial_reader = result->readers[0];
//		} else {
//			result->initial_reader =
//			    make_shared<ParquetReader>(context, bind_data.files[0], bind_data.parquet_options);
//			result->readers[0] = result->initial_reader;
//		}
//		result->file_states[0] = ParquetFileState::OPEN;
//	}
//	for (auto &reader : result->readers) {
//		if (!reader) {
//			continue;
//		}
//		MultiFileReader::InitializeReader(*reader, bind_data.parquet_options.file_options, bind_data.reader_bind,
//		                                  bind_data.types, bind_data.names, input.column_ids, input.filters,
//		                                  bind_data.files[0], context);
//	}
//
//	result->column_ids = input.column_ids;
//	result->filters = input.filters.get();
//	result->row_group_index = 0;
//	result->file_index = 0;
//	result->batch_index = 0;
//	result->max_threads = ParquetScanMaxThreads(context, input.bind_data.get());
//	if (input.CanRemoveFilterColumns()) {
//		result->projection_ids = input.projection_ids;
//		const auto table_types = bind_data.types;
//		for (const auto &col_idx : input.column_ids) {
//			if (IsRowIdColumnId(col_idx)) {
//				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
//			} else {
//				result->scanned_types.push_back(table_types[col_idx]);
//			}
//		}
//	}
//	return std::move(result);
//}

//static idx_t ParquetScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
//                                      LocalTableFunctionState *local_state,
//                                      GlobalTableFunctionState *global_state) {
//	auto &data = local_state->Cast<LocalState>();
//	return data.batch_index;
//}

//static void ParquetScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
//                                 const TableFunction &function) {
//	auto &bind_data = bind_data_p->Cast<BindData>();
//	serializer.WriteProperty(100, "files", bind_data.files);
//	serializer.WriteProperty(101, "types", bind_data.types);
//	serializer.WriteProperty(102, "names", bind_data.names);
//	serializer.WriteProperty(103, "parquet_options", bind_data.parquet_options);
//}

//static unique_ptr<FunctionData> ParquetScanDeserialize(Deserializer &deserializer, TableFunction &function) {
//	auto &context = deserializer.Get<ClientContext &>();
//	auto files = deserializer.ReadProperty<vector<string>>(100, "files");
//	auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
//	auto names = deserializer.ReadProperty<vector<string>>(102, "names");
//	auto parquet_options = deserializer.ReadProperty<ParquetOptions>(103, "parquet_options");
//	return ParquetScanBindInternal(context, files, types, names, parquet_options);
//}

//static void ParquetScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
//	if (!data_p.local_state) {
//		return;
//	}
//	auto &data = data_p.local_state->Cast<LocalState>();
//	auto &gstate = data_p.global_state->Cast<GlobalState>();
//	auto &bind_data = data_p.bind_data->CastNoConst<BindData>();
//
//	do {
//		if (gstate.CanRemoveFilterColumns()) {
//			data.all_columns.Reset();
//			data.reader->Scan(data.scan_state, data.all_columns);
//			MultiFileReader::FinalizeChunk(bind_data.reader_bind, data.reader->reader_data, data.all_columns);
//			output.ReferenceColumns(data.all_columns, gstate.projection_ids);
//		} else {
//			data.reader->Scan(data.scan_state, output);
//			MultiFileReader::FinalizeChunk(bind_data.reader_bind, data.reader->reader_data, output);
//		}
//
//		bind_data.chunk_count++;
//		if (output.size() > 0) {
//			return;
//		}
//		if (!ParallelStateNext(context, bind_data, data, gstate)) {
//			return;
//		}
//	} while (true);
//}

//static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
//	auto &data = bind_data->Cast<BindData>();
//	return make_uniq<NodeStatistics>(data.initial_file_cardinality * data.files.size());
//}
//
//static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
//	auto &data = bind_data->Cast<BindData>();
//	return data.initial_file_row_groups * data.files.size();
//}

// This function looks for the next available row group. If not available, it will open files from bind_data.files
// until there is a row group available for scanning or the files runs out
static bool ParallelStateNext(ClientContext &context, const BindData &bind_data,
                              LocalState &scan_data, GlobalState &parallel_state) {
	unique_lock<mutex> parallel_lock(parallel_state.lock);

	while (true) {
		if (parallel_state.error_opening_file) {
			return false;
		}

		if (parallel_state.file_index >= parallel_state.readers.size()) {
			return false;
		}

		D_ASSERT(parallel_state.initial_reader);

		if (parallel_state.file_states[parallel_state.file_index] == ParquetFileState::OPEN) {
			if (parallel_state.row_group_index <
			    parallel_state.readers[parallel_state.file_index]->NumRowGroups()) {
				// The current reader has rowgroups left to be scanned
				scan_data.reader = parallel_state.readers[parallel_state.file_index];
				vector<idx_t> group_indexes {parallel_state.row_group_index};
				scan_data.reader->InitializeScan(scan_data.scan_state, group_indexes);
				scan_data.batch_index = parallel_state.batch_index++;
				scan_data.file_index = parallel_state.file_index;
				parallel_state.row_group_index++;
				return true;
			} else {
				// Close current file
				parallel_state.file_states[parallel_state.file_index] = ParquetFileState::CLOSED;
				parallel_state.readers[parallel_state.file_index] = nullptr;

				// Set state to the next file
				parallel_state.file_index++;
				parallel_state.row_group_index = 0;

				if (parallel_state.file_index >= bind_data.files.size()) {
					return false;
				}
				continue;
			}
		}

		if (TryOpenNextFile(context, bind_data, scan_data, parallel_state, parallel_lock)) {
			continue;
		}

		// Check if the current file is being opened, in that case we need to wait for it.
		if (parallel_state.file_states[parallel_state.file_index] == ParquetFileState::OPENING) {
			WaitForFile(parallel_state.file_index, parallel_state, parallel_lock);
		}
	}
}

//static void ParquetComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
//                                         vector<unique_ptr<Expression>> &filters) {
//	auto &data = bind_data_p->Cast<BindData>();
//
//	auto reset_reader = MultiFileReader::ComplexFilterPushdown(context, data.files,
//	                                                           data.parquet_options.file_options, get, filters);
//	if (reset_reader) {
//		MultiFileReader::PruneReaders(data);
//	}
//}

//! Wait for a file to become available. Parallel lock should be locked when calling.
static void WaitForFile(idx_t file_index, GlobalState &parallel_state,
                        unique_lock<mutex> &parallel_lock) {
	while (true) {
		// To get the file lock, we first need to release the parallel_lock to prevent deadlocking
		parallel_lock.unlock();
		unique_lock<mutex> current_file_lock(parallel_state.file_mutexes[file_index]);
		parallel_lock.lock();

		// Here we have both locks which means we can stop waiting if:
		// - the thread opening the file is done and the file is available
		// - the thread opening the file has failed
		// - the file was somehow scanned till the end while we were waiting
		if (parallel_state.file_index >= parallel_state.readers.size() ||
		    parallel_state.file_states[parallel_state.file_index] != ParquetFileState::OPENING ||
		    parallel_state.error_opening_file) {
			return;
		}
	}
}

//! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
static bool TryOpenNextFile(ClientContext &context, const BindData &bind_data,
                            LocalState &scan_data, GlobalState &parallel_state,
                            unique_lock<mutex> &parallel_lock) {
	const auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	const auto file_index_limit = MinValue<idx_t>(parallel_state.file_index + num_threads, bind_data.files.size());
	for (idx_t i = parallel_state.file_index; i < file_index_limit; i++) {
		if (parallel_state.file_states[i] == ParquetFileState::UNOPENED) {
			string file = bind_data.files[i];
			parallel_state.file_states[i] = ParquetFileState::OPENING;
			auto pq_options = parallel_state.initial_reader->parquet_options;

			// Now we switch which lock we are holding, instead of locking the global state, we grab the lock on
			// the file we are opening. This file lock allows threads to wait for a file to be opened.
			parallel_lock.unlock();

			unique_lock<mutex> file_lock(parallel_state.file_mutexes[i]);

			shared_ptr<ParquetReader> reader;
			try {
				reader = make_shared<ParquetReader>(context, file, pq_options);
				MultiFileReader::InitializeReader(*reader, bind_data.parquet_options.file_options,
				                                  bind_data.reader_bind, bind_data.types, bind_data.names,
				                                  parallel_state.column_ids, parallel_state.filters,
				                                  bind_data.files.front(), context);
			} catch (...) {
				parallel_lock.lock();
				parallel_state.error_opening_file = true;
				throw;
			}

			// Now re-lock the state and add the reader
			parallel_lock.lock();
			parallel_state.readers[i] = reader;
			parallel_state.file_states[i] = ParquetFileState::OPEN;

			return true;
		}
	}

	return false;
}

static unique_ptr<FunctionData> Bind(ClientContext& context,
                                     TableFunctionBindInput& input,
                                     vector<LogicalType>& return_types,
                                     vector<string>& names) {
	auto files = MultiFileReader::GetFileList(context, input.inputs[0], "GeoParquet");
	ParquetOptions parquet_options(context);
	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (MultiFileReader::ParseOption(kv.first, kv.second, parquet_options.file_options, context)) {
			continue;
		}
		if (loption == "binary_as_string") {
			parquet_options.binary_as_string = BooleanValue::Get(kv.second);
		} else if (loption == "file_row_number") {
			parquet_options.file_row_number = BooleanValue::Get(kv.second);
		}
	}
	parquet_options.file_options.AutoDetectHivePartitioning(files, context);

	auto result = make_uniq<BindData>();
	result->files = std::move(files);
	result->reader_bind =
	    MultiFileReader::BindReader<ParquetReader>(context, result->types, result->names, *result, parquet_options);
	if (return_types.empty()) {
		// no expected types - just copy the types
		return_types = result->types;
		names = result->names;
	} else {
		if (return_types.size() != result->types.size()) {
			throw std::runtime_error(StringUtil::Format(
			    "Failed to read file \"%s\" - column count mismatch: expected %d columns but found %d",
			    result->files[0], return_types.size(), result->types.size()));
		}
		// expected types - overwrite the types we want to read instead
		result->types = return_types;
	}
	return std::move(result);
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<BindData>();
	auto result = make_uniq<GlobalState>();

	result->file_states = vector<ParquetFileState>(bind_data.files.size(), ParquetFileState::UNOPENED);
	result->file_mutexes = unique_ptr<mutex[]>(new mutex[bind_data.files.size()]);
	if (bind_data.files.empty()) {
		result->initial_reader = nullptr;
	} else {
		result->readers = std::move(bind_data.union_readers);
		if (result->readers.size() != bind_data.files.size()) {
			result->readers = vector<shared_ptr<ParquetReader>>(bind_data.files.size(), nullptr);
		} else {
			std::fill(result->file_states.begin(), result->file_states.end(), ParquetFileState::OPEN);
		}
		if (bind_data.initial_reader) {
			result->initial_reader = std::move(bind_data.initial_reader);
			result->readers[0] = result->initial_reader;
		} else if (result->readers[0]) {
			result->initial_reader = result->readers[0];
		} else {
			result->initial_reader =
			    make_shared<ParquetReader>(context, bind_data.files[0], bind_data.parquet_options);
			result->readers[0] = result->initial_reader;
		}
		result->file_states[0] = ParquetFileState::OPEN;
	}
	for (auto &reader : result->readers) {
		if (!reader) {
			continue;
		}
		MultiFileReader::InitializeReader(*reader, bind_data.parquet_options.file_options, bind_data.reader_bind,
		                                  bind_data.types, bind_data.names, input.column_ids, input.filters,
		                                  bind_data.files[0], context);
	}

	result->column_ids = input.column_ids;
	result->filters = input.filters.get();
	result->row_group_index = 0;
	result->file_index = 0;
	result->batch_index = 0;
	result->max_threads = bind_data.initial_file_row_groups * bind_data.files.size();
	if (input.CanRemoveFilterColumns()) {
		result->projection_ids = input.projection_ids;
		const auto table_types = bind_data.types;
		for (const auto &col_idx : input.column_ids) {
			if (IsRowIdColumnId(col_idx)) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				result->scanned_types.push_back(table_types[col_idx]);
			}
		}
	}
	return std::move(result);
};

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<BindData>();
	auto &gstate = global_state->Cast<GlobalState>();

	auto result = make_uniq<LocalState>();
	result->is_parallel = true;
	result->batch_index = 0;
	if (input.CanRemoveFilterColumns()) {
		result->all_columns.Initialize(context.client, gstate.scanned_types);
	}
	if (!ParallelStateNext(context.client, bind_data, *result, gstate)) {
		return nullptr;
	}
	return std::move(result);
};

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (!input.local_state) {
		return;
	}
	auto &data = input.local_state->Cast<LocalState>();
	auto &gstate = input.global_state->Cast<GlobalState>();
	auto &bind_data = input.bind_data->CastNoConst<BindData>();

	do {
		if (gstate.CanRemoveFilterColumns()) {
			data.all_columns.Reset();
			data.reader->Scan(data.scan_state, data.all_columns);
			MultiFileReader::FinalizeChunk(bind_data.reader_bind, data.reader->reader_data, data.all_columns);
			output.ReferenceColumns(data.all_columns, gstate.projection_ids);
		} else {
			data.reader->Scan(data.scan_state, output);
			MultiFileReader::FinalizeChunk(bind_data.reader_bind, data.reader->reader_data, output);
		}

		bind_data.chunk_count++;
		if (output.size() > 0) {
			return;
		}
		if (!ParallelStateNext(context, bind_data, data, gstate)) {
			return;
		}
	} while (true);
};

static double Progress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) {
    return 0;
};

static idx_t GetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                           LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
    return local_state->Cast<LocalState>().batch_index;
};

static constexpr const char *const table_function_name = "ST_ReadGeoparquet";
//! called in Binder::BindWithReplacementScan
static unique_ptr<TableRef> ReadGeoparquetReplacementScan(ClientContext &context, const string &table_name,
                                                          ReplacementScanData *data) {
    auto canReplace = ReplacementScan::CanReplace(table_name, { "gpq" });
	if (!canReplace) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>(table_function_name, std::move(children));
	return std::move(table_function);
};




}

void CoreTableFunctions::RegisterGeoparquetTableFunction(duckdb::DatabaseInstance &db) {
	TableFunction read("ST_ReadGeoparquet",
	                   {LogicalType::VARCHAR},
	                   geoparquet::Execute,
	                   geoparquet::Bind,
	                   geoparquet::InitGlobal,
	                   geoparquet::InitLocal
	);
	read.get_batch_index = geoparquet::GetBatchIndex;
	read.table_scan_progress = geoparquet::Progress;

	ExtensionUtil::RegisterFunction(db, read);

	auto& config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(geoparquet::ReadGeoparquetReplacementScan);
}

}
}