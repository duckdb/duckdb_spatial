#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

#include "protozero/pbf_reader.hpp"
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

	// Create an enum type for all osm kinds
	vector<string_t> enum_values = {"node", "way", "relation", "changeset"};
	auto varchar_vector = Vector(LogicalType::VARCHAR, enum_values.size());
	auto varchar_data = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t i = 0; i < enum_values.size(); i++) {
		auto str = enum_values[i];
		varchar_data[i] = str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}

	// Set return types
	return_types.push_back(LogicalType::ENUM("OSM_ENTITY_TYPE", varchar_vector, enum_values.size()));
	names.push_back("kind");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("id");

	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.push_back("tags");

	return_types.push_back(LogicalType::LIST(LogicalType::BIGINT));
	names.push_back("refs");

	return_types.push_back(LogicalType::DOUBLE);
	names.push_back("lat");

	return_types.push_back(LogicalType::DOUBLE);
	names.push_back("lon");

	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));
	names.push_back("ref_roles");

	// Create an enum type for the member kind
	vector<string_t> member_enum_values = {"node", "way", "relation"};
	auto member_varchar_vector = Vector(LogicalType::VARCHAR, member_enum_values.size());
	auto member_varchar_data = FlatVector::GetData<string_t>(member_varchar_vector);
	for (idx_t i = 0; i < member_enum_values.size(); i++) {
		auto str = member_enum_values[i];
		member_varchar_data[i] = str.IsInlined() ? str : StringVector::AddString(member_varchar_vector, str);
	}

	return_types.push_back(
	    LogicalType::LIST(LogicalType::ENUM("OSM_REF_TYPE", member_varchar_vector, member_enum_values.size())));
	names.push_back("ref_types");

	// Create bind data
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning OSM files is disabled through configuration");
	}

	auto file_name = StringValue::Get(input.inputs[0]);
	auto result = make_uniq<BindData>(file_name);
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

	return make_uniq<FileBlock>(blob.type, std::move(uncompressed_handle), blob_uncompressed_size, blob.blob_idx);
};

class GlobalState : public GlobalTableFunctionState {
	mutex lock;
	unique_ptr<FileHandle> handle;
	idx_t file_size;
	idx_t offset;
	bool done;
	idx_t blob_index;
	atomic<idx_t> bytes_read;
	idx_t max_threads;

public:
	GlobalState(unique_ptr<FileHandle> handle, idx_t file_size, idx_t max_threads)
	    : handle(std::move(handle)), file_size(file_size), offset(0), done(false), blob_index(0), bytes_read(0),
	      max_threads(max_threads) {
	}

	double GetProgress() {
		return 100 * ((double)bytes_read / (double)file_size);
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
		bytes_read = offset;

		return make_uniq<OsmBlob>(type, std::move(blob_buffer), blob_length, blob_index++);
	}
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (BindData &)*input.bind_data;

	auto &fs = FileSystem::GetFileSystem(context);
	auto file_name = bind_data.file_name;

	auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK);
	auto file_size = handle->GetFileSize();

	auto max_threads = context.db->NumberOfThreads();

	auto global_state = make_uniq<GlobalState>(std::move(handle), file_size, max_threads);

	// Read the first blob to get the header
	auto blob = global_state->GetNextBlob(context);
	if (blob->type != FileBlockType::Header) {
		throw ParserException("First blob in file is not a header");
	}

	return global_state;
}

struct LocalState : LocalTableFunctionState {
	unique_ptr<FileBlock> block;
	vector<string> string_table;
	int32_t granularity;
	int64_t lat_offset;
	int64_t lon_offset;

	explicit LocalState(unique_ptr<FileBlock> block) : block(std::move(block)) {
		Reset();
	}

	void SetBlock(unique_ptr<FileBlock> block) {
		this->block = std::move(block);
		Reset();
	}

	void Reset() {
		string_table.clear();
		granularity = 100;
		lat_offset = 0;
		lon_offset = 0;

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
	vector<int64_t> dense_node_ids;
	vector<uint32_t> dense_node_tags;
	vector<list_entry_t> dense_node_tag_entries;
	vector<int64_t> dense_node_lats;
	vector<int64_t> dense_node_lons;

	enum class ParseState { Block, Group, DenseNodes, End };

	ParseState state = ParseState::Block;

	// Returns false if there is data left to read but we've reached the capacity
	// Returns true if block is empty and we are done
	bool TryRead(DataChunk &output, idx_t &index, idx_t capacity) {
		// Main finite state machine
		while (index < capacity) {
			switch (state) {
			case ParseState::Block:
				if (block_reader.next(2)) {
					group_reader = block_reader.get_message();

					// Read the granularity and optional offsets
					if (block_reader.next(17)) {
						granularity = block_reader.get_int32();
					}
					if (block_reader.next(19)) {
						lat_offset = block_reader.get_int64();
					}
					if (block_reader.next(20)) {
						lon_offset = block_reader.get_int64();
					}
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
						ScanNode(output, index, capacity);
					} break;
					// Dense nodes
					case 2: {
						PrepareDenseNodes(output, index, capacity);
						state = ParseState::DenseNodes;
					} break;
					// Way
					case 3: {
						ScanWay(output, index, capacity);
					} break;
					// Relation
					case 4: {
						ScanRelation(output, index, capacity);
					} break;
					// Changeset
					case 5: {
						// Skip for now.
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
				auto done = ScanDenseNodes(output, index, capacity);
				if (done) {
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

	void ScanNode(DataChunk &output, idx_t &index, idx_t capacity) {
		// TODO: Implement this properly
		auto node = group_reader.get_message();
		node.next(1);
		auto id = node.get_int64();

		// Set kind
		FlatVector::GetData<uint8_t>(output.data[0])[index] = 0;
		// Set id
		FlatVector::GetData<int64_t>(output.data[1])[index] = id;

		// Read tags
		if (node.next(2)) {

			auto key_iter = node.get_packed_uint32();

			node.next(3);

			auto val_iter = node.get_packed_uint32();

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

		// Read lat/lon
		node.next(8);
		auto lat = node.get_sint64();
		FlatVector::GetData<double>(output.data[4])[index] = 0.000000001 * (lat_offset + (granularity * lat));

		node.next(9);
		auto lon = node.get_sint64();
		FlatVector::GetData<double>(output.data[5])[index] = 0.000000001 * (lon_offset + (granularity * lon));

		// Node has no refs, ref_roles or ref_types
		FlatVector::SetNull(output.data[3], index, true);
		FlatVector::SetNull(output.data[6], index, true);
		FlatVector::SetNull(output.data[7], index, true);

		index++;
	}

	void PrepareDenseNodes(DataChunk &output, idx_t &index, idx_t capacity) {
		dense_node_index = 0;
		dense_node_ids.clear();
		dense_node_tags.clear();
		dense_node_tag_entries.clear();
		dense_node_lats.clear();
		dense_node_lons.clear();

		auto dense_nodes = group_reader.get_message();
		dense_nodes.next(1);

		auto ids = dense_nodes.get_packed_sint64();
		int64_t last_id = 0;
		for (auto id : ids) {
			last_id += id;
			dense_node_ids.push_back(last_id);
		}

		if (dense_nodes.next(8)) {
			auto lats = dense_nodes.get_packed_sint64();
			int64_t last_lat = 0;
			for (auto lat : lats) {
				last_lat += lat;
				dense_node_lats.push_back(last_lat);
			}
		}

		if (dense_nodes.next(9)) {
			auto lons = dense_nodes.get_packed_sint64();
			int64_t last_lon = 0;
			for (auto lon : lons) {
				last_lon += lon;
				dense_node_lons.push_back(last_lon);
			}
		}

		if (dense_nodes.next(10)) {
			auto tags = dense_nodes.get_packed_uint32();

			idx_t entry_offset = 0;

			for (auto tag : tags) {
				if (tag == 0) {
					auto len = dense_node_tags.size() - entry_offset;
					dense_node_tag_entries.push_back(list_entry_t {entry_offset, len});
					entry_offset = dense_node_tags.size();
				} else {
					dense_node_tags.push_back(tag);
				}
			}
		}
	}

	void ScanWay(DataChunk &output, idx_t &index, idx_t capacity) {
		auto way = group_reader.get_message();
		way.next(1);
		auto id = way.get_int64();

		FlatVector::GetData<uint8_t>(output.data[0])[index] = 1;
		FlatVector::GetData<int64_t>(output.data[1])[index] = id;

		FlatVector::SetNull(output.data[4], index, true);
		FlatVector::SetNull(output.data[5], index, true);
		FlatVector::SetNull(output.data[6], index, true);
		FlatVector::SetNull(output.data[7], index, true);

		// Read tags
		if (way.next(2)) {

			auto key_iter = way.get_packed_uint32();

			way.next(3);

			auto val_iter = way.get_packed_uint32();

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
		if (way.next(8)) {
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
	}

	void ScanRelation(DataChunk &output, idx_t &index, idx_t capacity) {
		auto relation = group_reader.get_message();
		relation.next(1);
		auto id = relation.get_int64();

		FlatVector::GetData<uint8_t>(output.data[0])[index] = 2;
		FlatVector::GetData<int64_t>(output.data[1])[index] = id;

		FlatVector::SetNull(output.data[4], index, true);
		FlatVector::SetNull(output.data[5], index, true);

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

		// Roles
		if (relation.next(8)) {
			auto role_iter = relation.get_packed_int32();
			auto role_count = role_iter.size();

			auto total_roles = ListVector::GetListSize(output.data[6]);
			ListVector::Reserve(output.data[6], total_roles + role_count);
			ListVector::SetListSize(output.data[6], total_roles + role_count);
			auto &role_entry = ListVector::GetData(output.data[6])[index];
			auto &role_vector = ListVector::GetEntry(output.data[6]);
			role_entry.offset = total_roles;
			role_entry.length = role_count;

			auto roles = role_iter.begin();
			for (idx_t i = role_entry.offset; i < role_entry.offset + role_count; i++) {
				auto &role_str = string_table[*roles++];
				if (role_str.size() == 0) {
					FlatVector::SetNull(role_vector, i, true);
				} else {
					FlatVector::GetData<string_t>(role_vector)[i] = StringVector::AddString(role_vector, role_str);
				}
			}
		} else {
			FlatVector::SetNull(output.data[6], index, true);
		}

		// Refs
		if (relation.next(9)) {
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

		// Types
		if (relation.next(10)) {
			auto type_iter = relation.get_packed_int32();
			auto type_count = type_iter.size();

			auto total_types = ListVector::GetListSize(output.data[7]);
			ListVector::Reserve(output.data[7], total_types + type_count);
			ListVector::SetListSize(output.data[7], total_types + type_count);
			auto &type_entry = ListVector::GetData(output.data[7])[index];
			auto &type_vector = ListVector::GetEntry(output.data[7]);
			type_entry.offset = total_types;
			type_entry.length = type_count;

			auto type_data = FlatVector::GetData<uint8_t>(type_vector);
			for (auto type : type_iter) {
				type_data[total_types++] = (uint8_t)type;
			}
		} else {
			FlatVector::SetNull(output.data[7], index, true);
		}

		index++;
	}

	// Returns true if done (all dense nodes have been read)
	bool ScanDenseNodes(DataChunk &output, idx_t &index, idx_t capacity) {
		// Write multiple nodes at once as long as we have capacity
		auto nodes_to_write = capacity - index;
		auto nodes_to_read = std::min(nodes_to_write, dense_node_ids.size() - dense_node_index);

		auto kind_data = FlatVector::GetData<uint8_t>(output.data[0]);
		auto id_data = FlatVector::GetData<int64_t>(output.data[1]);
		auto lat_data = FlatVector::GetData<double>(output.data[4]);
		auto lon_data = FlatVector::GetData<double>(output.data[5]);

		for (idx_t i = 0; i < nodes_to_read; i++) {
			auto id = dense_node_ids[dense_node_index];

			id_data[index] = id;
			kind_data[index] = 0;
			lat_data[index] = 0.000000001 * (lat_offset + (granularity * dense_node_lats[dense_node_index]));
			lon_data[index] = 0.000000001 * (lon_offset + (granularity * dense_node_lons[dense_node_index]));

			// Do we have tags in this block?
			if (!dense_node_tags.empty()) {
				auto entry = dense_node_tag_entries[dense_node_index];
				if (entry.length != 0) {
					// Dense nodes tags are stored as a list of key/value pairs,
					// therefore we need to divide the length by 2 to get the number of tags
					auto tag_count = entry.length / 2;

					auto total_tags = ListVector::GetListSize(output.data[2]);
					ListVector::Reserve(output.data[2], total_tags + tag_count);
					ListVector::SetListSize(output.data[2], total_tags + tag_count);
					auto &tag_entry = ListVector::GetData(output.data[2])[index];

					tag_entry.offset = total_tags;
					tag_entry.length = tag_count;

					auto &key_vector = MapVector::GetKeys(output.data[2]);
					auto &value_vector = MapVector::GetValues(output.data[2]);

					idx_t t = entry.offset;
					idx_t r = tag_entry.offset;
					for (idx_t i = 0; i < tag_count; i++) {

						auto key_id = dense_node_tags[t];
						auto val_id = dense_node_tags[t + 1];

						FlatVector::GetData<string_t>(key_vector)[r] =
						    StringVector::AddString(key_vector, string_table[key_id]);
						FlatVector::GetData<string_t>(value_vector)[r] =
						    StringVector::AddString(value_vector, string_table[val_id]);

						t += 2;
						r += 1;
					}
				} else {
					FlatVector::SetNull(output.data[2], index, true);
				}
			} else {
				FlatVector::SetNull(output.data[2], index, true);
			}
			FlatVector::SetNull(output.data[3], index, true);

			// No ref types or roles for dense nodes
			FlatVector::SetNull(output.data[6], index, true);
			FlatVector::SetNull(output.data[7], index, true);

			dense_node_index++;
			index++;
		}
		if (dense_node_index >= dense_node_ids.size()) {
			return true;
		}
		return false;
	}
};

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
	// auto &bind_data = (BindData &)*input.bind_data;
	auto &global = (GlobalState &)*global_state;

	auto blob = global.GetNextBlob(context.client);
	if (blob == nullptr) {
		return nullptr;
	}
	auto block = DecompressBlob(context.client, *blob);

	return make_uniq<LocalState>(std::move(block));
}

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (input.local_state == nullptr) {
		return;
	}

	// auto &bind_data = (BindData &)*input.bind_data;
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

static unique_ptr<TableRef> ReadOsmPBFReplacementScan(ClientContext &context, const string &table_name,
                                                      ReplacementScanData *data) {
	// Check if the table name ends with .osm.pbf
	if (!StringUtil::EndsWith(StringUtil::Lower(table_name), ".osm.pbf")) {
		return nullptr;
	}

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("ST_ReadOSM", std::move(children));
	return std::move(table_function);
}

//------------------------------------------------------------------------------
//  Register
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterOsmTableFunction(DatabaseInstance &db) {
	TableFunction read("ST_ReadOSM", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal, InitLocal);

	read.get_batch_index = GetBatchIndex;
	read.table_scan_progress = Progress;

	ExtensionUtil::RegisterFunction(db, read);

	// Replacement scan
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(ReadOsmPBFReplacementScan);
}

} // namespace core

} // namespace spatial