#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "protozero/pbf_reader.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"
#include "zlib.h"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Utils
//------------------------------------------------------------------------------
namespace pz = protozero;

static int32_t ReadInt32BigEndian(data_ptr_t ptr) {
	return (ptr[0] << 24) | (ptr[1] << 16) | (ptr[2] << 8) | ptr[3];
}

//------------------------------------------------------------------------------
// OSM Table Function
//------------------------------------------------------------------------------

struct BindData : TableFunctionData {
	string file_name;

	BindData(string file_name) : file_name(file_name) {
	}
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {

	// Create an enum type for all geometry types
	// Ensure that these are in the same order as the GeometryType enum
	vector<string_t> enum_values = {"Node", "DenseNode", "Way", "Relation", "ChangeSet"};

	auto varchar_vector = Vector(LogicalType::VARCHAR, enum_values.size());
	auto varchar_data = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t i = 0; i < enum_values.size(); i++) {
		auto str = enum_values[i];
		varchar_data[i] = str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}

	// Set return types
	return_types.push_back(LogicalType::ENUM("OsmEntity", varchar_vector, enum_values.size()));
	names.push_back("kind");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("id");

	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.push_back("tags");

	return_types.push_back(LogicalType::LIST(LogicalType::BIGINT));
	names.push_back("refs");

	// Create bind data
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning OSM files is disabled through configuration");
	}

	auto file_name = StringValue::Get(input.inputs[0]);
	auto result = make_unique<BindData>(file_name);
	return result;
}

enum class FileBlockType { Header, Data };

struct OsmBlob {
	FileBlockType type;
	AllocatedData data;
	idx_t size;
	idx_t blob_idx;

	explicit OsmBlob(FileBlockType type, AllocatedData data, idx_t size, idx_t blob_idx)
	    : type(type), data(std::move(data)), size(size), blob_idx(blob_idx) {
	}
};

struct FileBlock {
	FileBlockType type; // type of block
	AllocatedData data; // raw or decompressed data
	idx_t size;         // size of the data
	idx_t block_idx;    // index of the block in the file

	explicit FileBlock(FileBlockType type, AllocatedData data, idx_t size, idx_t block_idx)
	    : type(type), data(std::move(data)), size(size), block_idx(block_idx) {
	}
};

static unique_ptr<FileBlock> DecompressBlob(ClientContext &context, OsmBlob &blob) {

	auto &buffer_manager = BufferManager::GetBufferManager(context);
	pz::pbf_reader reader((const char *)blob.data.get(), blob.size);

	// TODO: For now we assume they are all zlib compressed
	reader.next(2);
	auto blob_uncompressed_size = reader.get_int32();
	reader.next(3);
	auto view = reader.get_view();

	auto uncompressed_handle = buffer_manager.GetBufferAllocator().Allocate(blob_uncompressed_size);
	auto uncompressed_ptr = uncompressed_handle.get();

	z_stream zstream = {};
	zstream.avail_in = view.size();
	zstream.next_in = (Bytef *)view.data();
	zstream.avail_out = blob_uncompressed_size;
	zstream.next_out = (Bytef *)uncompressed_ptr;
	auto ok = inflateInit(&zstream);
	if (ok != Z_OK) {
		throw ParserException("Failed to initialize zlib");
	}
	ok = inflate(&zstream, Z_FINISH);
	if (ok != Z_STREAM_END) {
		throw ParserException("Failed to inflate zlib");
	}
	ok = inflateEnd(&zstream);
	// Cool, we have the uncompressed data

	return make_unique<FileBlock>(blob.type, std::move(uncompressed_handle), blob_uncompressed_size, blob.blob_idx);
};

class GlobalState : public GlobalTableFunctionState {
	mutex lock;
	unique_ptr<FileHandle> handle;
	idx_t offset;
	idx_t blob_index;
	bool done;
	idx_t file_size;
	idx_t max_threads;

public:
	GlobalState(unique_ptr<FileHandle> handle, idx_t file_size, idx_t max_threads)
	    : handle(std::move(handle)), file_size(file_size), done(false), offset(0) {
	}

	bool IsDone() {
		lock_guard<mutex> glock(lock);
		return done;
	}

	double GetProgress() {
		return (double)offset / (double)file_size;
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

	unique_ptr<OsmBlob> GetNextBlob(ClientContext &context) {
		lock_guard<mutex> glock(lock);

		if (done) {
			return nullptr;
		}
		if (offset >= file_size) {
			done = true;
			return nullptr;
		}

		auto &buffer_manager = BufferManager::GetBufferManager(context);

		// The format is a repeating sequence of:
		//    int4: length of the BlobHeader message in network byte order
		//    serialized BlobHeader message
		//    serialized Blob message (size is given in the header)

		// Read the length of the BlobHeader
		int32_t header_length_be = 0;
		handle->Read((data_ptr_t)&header_length_be, sizeof(int32_t), offset);
		offset += sizeof(int32_t);
		int32_t header_length = ReadInt32BigEndian((data_ptr_t)&header_length_be);

		// Read the BlobHeader
		auto header_buffer = buffer_manager.GetBufferAllocator().Allocate(header_length);
		handle->Read(header_buffer.get(), header_length, offset);

		pz::pbf_reader reader((const char *)header_buffer.get(), header_length);

		// 1 - type of the blob
		reader.next(1);
		auto type_str = reader.get_string();
		FileBlockType type;
		if (type_str == "OSMHeader") {
			type = FileBlockType::Header;
		} else if (type_str == "OSMData") {
			type = FileBlockType::Data;
		} else {
			throw ParserException("Unexpected fileblock type in Blob");
		}
		// 3 - size of the next blob
		reader.next(3);
		auto blob_length = reader.get_int32(); // size of the next blob

		offset += header_length;

		// Read the Blob
		auto blob_buffer = buffer_manager.GetBufferAllocator().Allocate(blob_length);
		handle->Read(blob_buffer.get(), blob_length, offset);

		offset += blob_length;

		return make_unique<OsmBlob>(type, std::move(blob_buffer), blob_length, blob_index++);
	}
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (BindData &)*input.bind_data;

	auto &fs = FileSystem::GetFileSystem(context);
	auto opener = FileSystem::GetFileOpener(context);
	auto file_name = bind_data.file_name;

	auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK,
	                          FileCompressionType::UNCOMPRESSED, opener);
	auto file_size = handle->GetFileSize();

	auto max_threads = context.db->NumberOfThreads();

	auto global_state = make_unique<GlobalState>(std::move(handle), file_size, max_threads);

	// Read the first blob to get the header
	auto blob = global_state->GetNextBlob(context);
	if (blob->type != FileBlockType::Header) {
		throw ParserException("First blob in file is not a header");
	}

	return global_state;
}

struct LocalState : LocalTableFunctionState {
	// The index of the current blob

	unique_ptr<FileBlock> block;
	vector<string> string_table;

	explicit LocalState(unique_ptr<FileBlock> block) : block(std::move(block)) {
		InitializeReader();
	}

	void SetBlock(unique_ptr<FileBlock> block) {
		this->block = std::move(block);
		InitializeReader();
	}

	void InitializeReader() {
		// Reset
		string_table.clear();

		block_reader = pz::pbf_reader((const char *)block->data.get(), block->size);
		block_reader.next(1); // String table
		auto string_table_reader = block_reader.get_message();
		while (string_table_reader.next(1)) {
			string_table.push_back(string_table_reader.get_string());
		}

		state = ParseState::Block;
	}

	pz::pbf_reader block_reader;
	pz::pbf_reader group_reader;

	idx_t dense_node_index;
	vector<idx_t> dense_node_ids;
	vector<uint32_t> dense_node_tags;
	vector<list_entry_t> dense_node_tag_entries;
	vector<idx_t> dense_node_lats;
	vector<idx_t> dense_node_lons;

	enum class ParseState { Block, Group, DenseNodes, End };

	ParseState state = ParseState::Block;

	// Returns false if there is data left to read but we've reached the capacity
	// Returns true if block is empty
	bool TryRead(DataChunk &output, idx_t &index, idx_t capacity) {
		while (index < capacity) {
			switch (state) {
			case ParseState::Block:
				if (block_reader.next(2)) {
					group_reader = block_reader.get_message();
					state = ParseState::Group;
				} else {
					state = ParseState::End;
				}
				break;
			case ParseState::Group:
				if (group_reader.next()) {
					switch (group_reader.tag()) {
					// Nodes
					case 1: {
						auto node = group_reader.get_message();
						node.next(1);
						auto id = node.get_int64();

						FlatVector::GetData<uint8_t>(output.data[0])[index] = 0;
						FlatVector::GetData<int64_t>(output.data[1])[index] = id;
						FlatVector::SetNull(output.data[2], index, true);

						index++;
					} break;
					// Dense nodes
					case 2: {
						dense_node_index = 0;
						dense_node_ids.clear();
						dense_node_tags.clear();
						dense_node_tag_entries.clear();
						dense_node_lats.clear();
						dense_node_lons.clear();

						auto dense_nodes = group_reader.get_message();
						dense_nodes.next(1);

						auto ids = dense_nodes.get_packed_sint64();
						auto last_id = 0;
						for (auto id : ids) {
							last_id += id;
							dense_node_ids.push_back(last_id);
						}

						if(dense_nodes.next(10)) {
							auto tags = dense_nodes.get_packed_uint32();
							auto current_entry = list_entry_t(0, 0);
							idx_t idx = 0;
							for (auto tag : tags) {
								// 0 are used as delimiters
								if(tag == 0) {
									current_entry.length = idx - current_entry.offset;
									dense_node_tag_entries.push_back(current_entry);
									current_entry.offset = idx;
								} else {
									dense_node_tags.push_back(tag);
								}
								idx++;
							}
						}

						state = ParseState::DenseNodes;
					} break;
					// Way
					case 3: {
						auto way = group_reader.get_message();
						way.next(1);
						auto id = way.get_int64();

						FlatVector::GetData<uint8_t>(output.data[0])[index] = 2;
						FlatVector::GetData<int64_t>(output.data[1])[index] = id;

						// Read tags
						if (way.next(2)) {

							auto key_iter = way.get_packed_uint32();
							//vector<uint32_t> keys(key_iter.begin(), key_iter.end());

							way.next(3);
							auto val_iter = way.get_packed_uint32();
							//vector<uint32_t> vals(val_iter.begin(), val_iter.end());

							auto tag_count = key_iter.size();

							auto total_tags = ListVector::GetListSize(output.data[2]);
							ListVector::Reserve(output.data[2], total_tags + tag_count);
							ListVector::SetListSize(output.data[2], total_tags + tag_count);
							auto &tag_entry = ListVector::GetData(output.data[2])[index];

							tag_entry.offset = total_tags;
							tag_entry.length = tag_count;

							auto &key_vector = MapVector::GetKeys(output.data[2]);
							auto &value_vector = MapVector::GetValues(output.data[2]);

							auto keys = key_iter.begin();
							auto vals = val_iter.begin();
							for (idx_t i = tag_entry.offset; i < tag_entry.offset + tag_count; i++) {
								FlatVector::GetData<string_t>(key_vector)[i] =
								    StringVector::AddString(key_vector, string_table[*keys++]);
								FlatVector::GetData<string_t>(value_vector)[i] =
								    StringVector::AddString(value_vector, string_table[*vals++]);
							}
						} else {
							FlatVector::SetNull(output.data[2], index, true);
						}

						// Refs
						if(way.next(8)) {
	                        auto ref_iter = way.get_packed_sint64();
							auto ref_count = ref_iter.size();

							auto total_refs = ListVector::GetListSize(output.data[3]);
							ListVector::Reserve(output.data[3], total_refs + ref_count);
							ListVector::SetListSize(output.data[3], total_refs + ref_count);
							auto &ref_entry = ListVector::GetData(output.data[3])[index];
							auto &ref_vector = ListVector::GetEntry(output.data[3]);
							ref_entry.offset = total_refs;
							ref_entry.length = ref_count;

							auto ref_data = FlatVector::GetData<int64_t>(ref_vector);

	                        int64_t last_ref = 0;
	                        for (auto ref : ref_iter) {
	                            last_ref += ref;
	                            ref_data[total_refs++] = last_ref;
	                        }
						} else {
							FlatVector::SetNull(output.data[3], index, true);
						}
						index++;
					} break;
					// Relation
					case 4: {
						auto relation = group_reader.get_message();
						relation.next(1);
						auto id = relation.get_int64();

						FlatVector::GetData<uint8_t>(output.data[0])[index] = 3;
						FlatVector::GetData<int64_t>(output.data[1])[index] = id;
						
						// Read tags
						if (relation.next(2)) {

							auto key_iter = relation.get_packed_uint32();

							relation.next(3);
							auto val_iter = relation.get_packed_uint32();

							auto tag_count = key_iter.size();

							auto total_tags = ListVector::GetListSize(output.data[2]);
							ListVector::Reserve(output.data[2], total_tags + tag_count);
							ListVector::SetListSize(output.data[2], total_tags + tag_count);
							auto &tag_entry = ListVector::GetData(output.data[2])[index];

							tag_entry.offset = total_tags;
							tag_entry.length = tag_count;

							auto &key_vector = MapVector::GetKeys(output.data[2]);
							auto &value_vector = MapVector::GetValues(output.data[2]);

							auto keys = key_iter.begin();
							auto vals = val_iter.begin();
							for (idx_t i = tag_entry.offset; i < tag_entry.offset + tag_count; i++) {
								FlatVector::GetData<string_t>(key_vector)[i] =
								    StringVector::AddString(key_vector, string_table[*keys++]);
								FlatVector::GetData<string_t>(value_vector)[i] =
								    StringVector::AddString(value_vector, string_table[*vals++]);
							}
						} else {
							FlatVector::SetNull(output.data[2], index, true);
						}

						// Refs
						if(relation.next(9)) {
	                        auto ref_iter = relation.get_packed_sint64();
							auto ref_count = ref_iter.size();

							auto total_refs = ListVector::GetListSize(output.data[3]);
							ListVector::Reserve(output.data[3], total_refs + ref_count);
							ListVector::SetListSize(output.data[3], total_refs + ref_count);
							auto &ref_entry = ListVector::GetData(output.data[3])[index];
							auto &ref_vector = ListVector::GetEntry(output.data[3]);
							ref_entry.offset = total_refs;
							ref_entry.length = ref_count;

							auto ref_data = FlatVector::GetData<int64_t>(ref_vector);

	                        int64_t last_ref = 0;
	                        for (auto ref : ref_iter) {
	                            last_ref += ref;
	                            ref_data[total_refs++] = last_ref;
	                        }
						} else {
							FlatVector::SetNull(output.data[3], index, true);
						}

						index++;

					} break;
					// Changeset
					case 5: {
						group_reader.skip();
					} break;
					default: {
						group_reader.skip();
					} break;
					}
				} else {
					state = ParseState::Block;
				}
				break;
			case ParseState::DenseNodes: {
				// TODO: Write multiple nodes at once as long as we have capacity
				auto nodes_to_write = capacity - index;
				auto nodes_to_read = std::min(nodes_to_write, dense_node_ids.size() - dense_node_index);

				auto kind_data = FlatVector::GetData<uint8_t>(output.data[0]);
				auto id_data = FlatVector::GetData<int64_t>(output.data[1]);
				for (idx_t i = 0; i < nodes_to_read; i++) {
					auto id = dense_node_ids[dense_node_index];

					id_data[index] = id;
					kind_data[index] = 1;
					
					FlatVector::SetNull(output.data[2], index, true);
					FlatVector::SetNull(output.data[3], index, true);

					dense_node_index++;
					index++;
				}
				if(dense_node_index >= dense_node_ids.size()) {
					state = ParseState::Group;
				}
			} break;
			case ParseState::End:
			default:
				return true;
			}
		}
		return false;
	}
	/*
	bool TryRead(DataChunk &output, idx_t index) {
	    // TODO: Manage state machine here

	    //output.data[0].SetValue(index, Value::CreateValue(block->type == FileBlockType::Data ? "Data" : "Header"));
	    //output.data[1].SetValue(index, Value::CreateValue(block->size));

	    while(block_reader.next(2)) {
	        group_reader = block_reader.get_message();

	        while(group_reader.next()) {
	            switch(group_reader.tag()) {
	                case 1: {
	                    // Nodes
	                    auto node = group_reader.get_message();

	                    output.data[0].SetValue(index, Value::CreateValue("Node"));
	                    FlatVector::SetNull(output.data[1], index, true);
	                    FlatVector::SetNull(output.data[2], index, true);
	                    FlatVector::SetNull(output.data[3], index, true);
	                } break;
	                case 2: {
	                    // Dense nodes
	                    auto dense_node = group_reader.get_message();
	                    output.data[0].SetValue(index, Value::CreateValue("Dense Node"));
	                    FlatVector::SetNull(output.data[1], index, true);
	                    FlatVector::SetNull(output.data[2], index, true);
	                    FlatVector::SetNull(output.data[3], index, true);

	                } break;
	                case 3: {
	                    // Ways
	                    output.data[0].SetValue(index, Value::CreateValue("Way"));
	                    auto way = group_reader.get_message();
	                    way.next(1);
	                    auto id = way.get_int64();
	                    output.data[1].SetValue(index, id);


	                    vector<Value> pairs;
	                    if(way.next(2)) {
	                        vector<string> keys;
	                        vector<string> vals;
	                        auto key_iter = way.get_packed_uint32();
	                        for (auto it = key_iter.begin(); it != key_iter.end(); ++it) {
	                            auto idx = *it;
	                            keys.push_back(string_table[idx]);
	                        }
	                        way.next(3);
	                        auto val_iter = way.get_packed_uint32();
	                        for (auto val_idx : val_iter) {
	                            vals.push_back(string_table[val_idx]);
	                        }

	                        for(idx_t i = 0; i < keys.size(); i++) {
	                            pairs.push_back(Value::STRUCT({{ "key", keys[i] }, {"value", vals[i]}}));
	                        }
	                    }
	                    output.data[2].SetValue(index, Value::MAP(LogicalType::STRUCT({{"key", LogicalType::VARCHAR},
	{"value", LogicalType::VARCHAR}}), pairs));


	                    vector<Value> refs;
	                    if(way.next(8)) {
	                        auto ref_iter = way.get_packed_sint64();
	                        int64_t last_ref = 0;
	                        for (auto ref : ref_iter) {
	                            last_ref += ref;
	                            refs.push_back(Value::BIGINT(last_ref));
	                        }
	                    }
	                    output.data[3].SetValue(index, Value::LIST(LogicalType::BIGINT, refs));
	                } break;
	                case 4: {
	                    // Relations
	                    output.data[0].SetValue(index, Value::CreateValue("Relation"));
	                    auto relation = group_reader.get_message();
	                    relation.next(1);
	                    auto id = relation.get_int64();
	                    output.data[1].SetValue(index, id);


	                    vector<Value> pairs;
	                    if(relation.next(2)) {

	                        vector<string> keys;
	                        vector<string> vals;
	                        auto key_iter = relation.get_packed_uint32();
	                        for (auto it = key_iter.begin(); it != key_iter.end(); ++it) {
	                            auto idx = *it;
	                            keys.push_back(string_table[idx]);
	                        }
	                        relation.next(3);
	                        auto val_iter = relation.get_packed_uint32();
	                        for (auto val_idx : val_iter) {
	                            vals.push_back(string_table[val_idx]);
	                        }
	                        for(idx_t i = 0; i < keys.size(); i++) {
	                            pairs.push_back(Value::STRUCT({{ "key", keys[i] }, {"value", vals[i]}}));
	                        }
	                    }

	                    output.data[2].SetValue(index, Value::MAP(LogicalType::STRUCT({{"key", LogicalType::VARCHAR},
	{"value", LogicalType::VARCHAR}}), pairs)); FlatVector::SetNull(output.data[3], index, true);

	                } break;
	                case 5: {
	                    // Changesets
	                    output.data[0].SetValue(index, Value::CreateValue("Changeset"));
	                    group_reader.skip();
	                    FlatVector::SetNull(output.data[1], index, true);
	                    FlatVector::SetNull(output.data[2], index, true);
	                    FlatVector::SetNull(output.data[3], index, true);


	                } break;
	                default: {
	                    output.data[0].SetValue(index, Value::CreateValue("Unknown"));
	                    group_reader.skip();
	                    FlatVector::SetNull(output.data[1], index, true);
	                    FlatVector::SetNull(output.data[2], index, true);
	                    FlatVector::SetNull(output.data[3], index, true);


	                } break;
	            }
	        }
	    }
	    return true;
	}
	*/
};

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
	auto &bind_data = (BindData &)*input.bind_data;
	auto &global = (GlobalState &)*global_state;

	auto blob = global.GetNextBlob(context.client);
	if (blob == nullptr) {
		return nullptr;
	}
	auto block = DecompressBlob(context.client, *blob);

	return make_unique<LocalState>(std::move(block));
}

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (input.local_state == nullptr) {
		return;
	}

	auto &bind_data = (BindData &)*input.bind_data;
	auto &global_state = (GlobalState &)*input.global_state;
	auto &local_state = (LocalState &)*input.local_state;

	idx_t row_id = 0;
	idx_t capacity = STANDARD_VECTOR_SIZE;

	while (row_id < capacity) {
		bool done = local_state.TryRead(output, row_id, capacity);
		if (done) {
			auto next = global_state.GetNextBlob(context);
			if (next.get() == nullptr) {
				break;
			}
			auto next_block = DecompressBlob(context, *next);
			local_state.SetBlock(std::move(next_block));
		}
	}
	output.SetCardinality(row_id);
}

static double Progress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) {
	auto &state = (GlobalState &)*global_state;
	return state.GetProgress();
}

static idx_t GetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                           LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &state = (LocalState &)*local_state;
	return state.block->block_idx;
}
//------------------------------------------------------------------------------
//  Register
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterOsmTableFunction(ClientContext &context) {
	TableFunction read("ST_ReadOSM", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal, InitLocal);

	read.get_batch_index = GetBatchIndex;
	read.table_scan_progress = Progress;
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo info(read);
	catalog.CreateTableFunction(context, &info);
}

} // namespace core

} // namespace spatial