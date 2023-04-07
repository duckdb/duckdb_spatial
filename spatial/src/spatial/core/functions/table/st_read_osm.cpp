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
// OSM PBF Parsing
//------------------------------------------------------------------------------
namespace pz = protozero;

static int32_t ReadInt32BigEndian(data_ptr_t ptr) {
	return (ptr[0] << 24) | (ptr[1] << 16) | (ptr[2] << 8) | ptr[3];
}

enum class OsmFileBlockType {
	Header,
	Data,
};

struct OsmBlobHeader {
	OsmFileBlockType type;
	int32_t blob_size;

	explicit OsmBlobHeader(OsmFileBlockType type, int32_t blob_size) : type(type), blob_size(blob_size) {
	}
};

static OsmBlobHeader GetNextBlobHeader(ClientContext &context, FileHandle &file_handle, idx_t &offset) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	// Read the length of the BlobHeader
	int32_t header_length_be = 0;
	file_handle.Read((data_ptr_t)&header_length_be, sizeof(int32_t), offset);
	offset += sizeof(int32_t);

	int32_t header_length = ReadInt32BigEndian((data_ptr_t)&header_length_be);

	// Read the BlobHeader
	auto header_handle = buffer_manager.Allocate(header_length);
	file_handle.Read(header_handle.Ptr(), header_length, offset);

	pz::pbf_reader reader((const char *)header_handle.Ptr(), header_length);

	OsmFileBlockType type;
	reader.next();
	if (reader.tag() != 1) {
		throw ParserException("Unexpected tag in Blob");
	}
	auto type_str = reader.get_string();
	if (type_str == "OSMHeader") {
		type = OsmFileBlockType::Header;
	} else if (type_str == "OSMData") {
		type = OsmFileBlockType::Data;
	} else {
		throw ParserException("Unexpected fileblock type in Blob");
	}
	reader.next();
	if (reader.tag() == 2) {
		// indexdata, skip
		reader.skip();
		reader.next();
	}
	if (reader.tag() != 3) {
		throw ParserException("Unexpected tag in Blob");
	}
	// size of the next blob
	auto blob_size = reader.get_int32();

	offset += header_length;

	return OsmBlobHeader(type, blob_size);
}

struct OsmHeaderBlock {
	// todo: bbox & metadata
};

static OsmHeaderBlock GetOsmHeaderBlock(ClientContext &context, FileHandle &file_handle, idx_t &offset) {
	auto header = GetNextBlobHeader(context, file_handle, offset);
	if (header.type != OsmFileBlockType::Header) {
		throw ParserException("Expected OSMHeader, got OSMData");
	}

	auto &buffer_manager = BufferManager::GetBufferManager(context);

	auto header_handle = buffer_manager.Allocate(header.blob_size);
	file_handle.Read(header_handle.Ptr(), header.blob_size, offset);

	pz::pbf_reader reader((const char *)header_handle.Ptr(), header.blob_size);

	OsmHeaderBlock header_block;
	while (reader.next()) {
		switch (reader.tag()) {
		case 1: // optional HeaderBBox bbox = 1;
			reader.skip();
			break;
		case 2: // optional string required_features = 2;
			reader.skip();
			break;
		case 3: // optional string optional_features = 3;
			reader.skip();
			break;
		case 4: // optional string writingprogram = 4;
			reader.skip();
			break;
		case 5: // optional string source = 5;
			reader.skip();
			break;
		case 16: // optional int32 osmosis_replication_timestamp = 16;
			reader.skip();
			break;
		case 17: // optional int64 osmosis_replication_sequence_number = 17;
			reader.skip();
			break;
		case 32: // optional int32 osmosis_replication_timestamp = 32;
			reader.skip();
			break;
		case 33: // optional int32 osmosis_replication_sequence_number = 33;
			reader.skip();
			break;
		case 34: // optional int32 osmosis_replication_base_url = 34;
			reader.skip();
			break;
		default:
			reader.skip();
		}
	}
	offset += header.blob_size;

	return header_block;
}

struct OsmPrimitiveBlock {
	BufferHandle data_handle;
	idx_t size;

	OsmPrimitiveBlock(BufferHandle data_handle, idx_t size) : data_handle(std::move(data_handle)), size(size) {
	}
};

static OsmPrimitiveBlock GetOsmPrimitiveBlock(ClientContext &context, FileHandle &file_handle, idx_t &offset) {
	auto header = GetNextBlobHeader(context, file_handle, offset);
	if (header.type != OsmFileBlockType::Data) {
		throw ParserException("Expected OSMData, got OSMHeader");
	}

	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto data_handle = buffer_manager.Allocate(header.blob_size);
	file_handle.Read(data_handle.Ptr(), header.blob_size, offset);

	offset += header.blob_size;

	return OsmPrimitiveBlock(std::move(data_handle), header.blob_size);
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

	// Set return types
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("type");

	return_types.push_back(LogicalType::INTEGER);
	names.push_back("size");

    return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));
    names.push_back("groups");

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
	atomic<idx_t> offset;
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
    if(blob->type != FileBlockType::Header) {
        throw ParserException("First blob in file is not a header");
    }

	return global_state;
}

struct LocalState : LocalTableFunctionState {
	// The index of the current blob

    unique_ptr<FileBlock> block;
    pz::pbf_reader reader;

	explicit LocalState(unique_ptr<FileBlock> block) : block(std::move(block)) {
        InitializeReader();
    }

    void SetBlock(unique_ptr<FileBlock> block) {
        this->block = std::move(block);
        InitializeReader();
    }

    void InitializeReader() {
        reader = pz::pbf_reader((const char *)block->data.get(), block->size);
        reader.next(1); // String table - skip for now
        reader.skip();
    }

    bool TryRead(DataChunk &output, idx_t index) {
        // TODO: Manage state machine here
        
        output.data[0].SetValue(index, Value::CreateValue(block->type == FileBlockType::Data ? "Data" : "Header"));
		output.data[1].SetValue(index, Value::CreateValue(block->size));
        
        vector<Value> values;
        while(reader.next(2)) {
            auto group = reader.get_message();
            
            group.next();
            switch(group.tag()) {
                case 1: { 
                    // Nodes
                    values.push_back(Value::CreateValue("Node"));
                } break;
                case 2: {
                    // Dense nodes
                    values.push_back(Value::CreateValue("DenseNode"));
                } break;
                case 3: {
                    // Ways
                    values.push_back(Value::CreateValue("Way"));

                } break;
                case 4: {
                    // Relations
                    values.push_back(Value::CreateValue("Relation"));

                } break;
                case 5: {
                    // Changesets
                    values.push_back(Value::CreateValue("Changeset"));
                } break;
            }
        }
        output.data[2].SetValue(index, Value::LIST(LogicalType::VARCHAR, std::move(values)));
        return true;
    }
};

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
    auto &bind_data = (BindData &)*input.bind_data;
    auto &global = (GlobalState &)*global_state;

    auto blob = global.GetNextBlob(context.client);
    if(blob == nullptr) {
        return nullptr;
    }
    auto block = DecompressBlob(context.client, *blob);

    return make_unique<LocalState>(std::move(block));
}

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    if(input.local_state == nullptr) {
        return;
    }
    
	auto &bind_data = (BindData &)*input.bind_data;
	auto &global_state = (GlobalState &)*input.global_state;
	auto &local_state = (LocalState &)*input.local_state;

	idx_t i = 0;

    bool done = false;
    while (!done && i < STANDARD_VECTOR_SIZE) {
        local_state.TryRead(output, i);
    
		auto next = global_state.GetNextBlob(context);
        if(next.get() == nullptr) {
            done = true;
            break;
        }
        auto next_block = DecompressBlob(context, *next);
        local_state.SetBlock(std::move(next_block));
		i++;
	}
	output.SetCardinality(i);
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