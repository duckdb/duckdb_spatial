#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file_reader.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

#include "shapefil.h"
#include "utf8proc_wrapper.hpp"

#include <cstdlib>

void SASetupDefaultHooks(SAHooks *hooks) {
	// Should never be called, use OpenLL and pass in the hooks
	throw duckdb::InternalException("SASetupDefaultHooks");
}

namespace spatial {

namespace core {

struct EncodingUtil {
	static inline uint8_t GetUTF8ByteLength (data_t first_char) {
		if (first_char < 0x80) return 1;
		if (!(first_char & 0x20)) return 2;
		if (!(first_char & 0x10)) return 3;
		if (!(first_char & 0x08)) return 4;
		if (!(first_char & 0x04)) return 5;
		return 6;
	}
	static inline data_t UTF8ToLatin1Char (const_data_ptr_t ptr) {
		auto len = GetUTF8ByteLength(*ptr);
		if (len == 1) {
			return *ptr;
		}
		uint32_t res = static_cast<data_t>(*ptr & (0xff >> (len + 1))) << ((len - 1) * 6);
		while (--len) {
			res |= (*(++ptr) - 0x80) << ((len - 1) * 6);
		}
		// TODO: Throw exception instead if character can't be encoded?
		return res > 0xff ? '?' : static_cast<data_t>(res);
	}

	// Convert UTF-8 to ISO-8859-1
	// out must be at least the size of in
	static void UTF8ToLatin1Buffer (const_data_ptr_t in, data_ptr_t out) {
		while (*in) {
			*out++ = UTF8ToLatin1Char(in);
		}
		*out = 0;
	}

	// convert ISO-8859-1 to UTF-8
	// mind = blown
	// out must be at least 2x the size of in
	static idx_t LatinToUTF8Buffer(const_data_ptr_t in, data_ptr_t out) {
		idx_t len = 0;
		while (*in) {
			if (*in < 128) {
				*out++ = *in++;
				len += 1;
			} else {
				*out++ = 0xc2 + (*in > 0xbf);
				*out++ = (*in++ & 0x3f) + 0x80;
				len += 2;
			}
		}
		return len;
	}
};

//------------------------------------------------------------------------------
// Shapefile filesystem abstractions
//------------------------------------------------------------------------------
static SAFile DuckDBShapefileOpen(void* userData, const char *filename, const char *access_mode) {
	try {
		auto &fs = *reinterpret_cast<FileSystem *>(userData);
		auto file_handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);
		if (!file_handle) {
			return nullptr;
		}
		return reinterpret_cast<SAFile>(file_handle.release());
	} catch(...) {
	    return nullptr;
	}
}

static SAOffset DuckDBShapefileRead(void *p, SAOffset size, SAOffset nmemb, SAFile file) {
	auto handle = reinterpret_cast<FileHandle*>(file);
	auto read_bytes = handle->Read(p, size * nmemb);
	return read_bytes / size;
}

static SAOffset DuckDBShapefileWrite(const void *p, SAOffset size, SAOffset nmemb, SAFile file) {
	auto handle = reinterpret_cast<FileHandle*>(file);
	auto written_bytes = handle->Write(const_cast<void*>(p), size * nmemb);
	return written_bytes / size;
}

static SAOffset DuckDBShapefileSeek(SAFile file, SAOffset offset, int whence) {
	auto file_handle = reinterpret_cast<FileHandle*>(file);
	switch (whence) {
	case SEEK_SET:
		file_handle->Seek(offset);
		break;
	case SEEK_CUR:
		file_handle->Seek(file_handle->SeekPosition() + offset);
		break;
	case SEEK_END:
		file_handle->Seek(file_handle->GetFileSize() + offset);
		break;
	default:
		throw InternalException("Unknown seek type");
	}
	return 0;
}

static SAOffset DuckDBShapefileTell(SAFile file) {
	auto handle = reinterpret_cast<FileHandle*>(file);
	return handle->SeekPosition();
}

static int DuckDBShapefileFlush(SAFile file) {
	try {
		auto handle = reinterpret_cast<FileHandle *>(file);
		handle->Sync();
		return 0;
	} catch(...) {
	    return -1;
	}
}

static int DuckDBShapefileClose(SAFile file) {
	try {
		auto handle = reinterpret_cast<FileHandle*>(file);
		handle->Close();
		delete handle;
		return 0;
	} catch(...) {
	    return -1;
	}
}

static int DuckDBShapefileRemove(void *userData, const char *filename) {
	try {
		auto &fs = *reinterpret_cast<FileSystem *>(userData);
		auto file = fs.OpenFile(filename, FileFlags::FILE_FLAGS_WRITE);
		auto file_type = fs.GetFileType(*file);
		if (file_type == FileType::FILE_TYPE_DIR) {
			fs.RemoveDirectory(filename);
		} else {
			fs.RemoveFile(filename);
		}
		return 0;
	} catch(...) {
	    return -1;
	}
}

static void DuckDBShapefileError(const char *message) {
	// We cant throw an exception here because the shapefile library is not
	// exception safe. Instead we should store it somewhere...
	// Maybe another client context cache?

	// Note that we need to copy the message

	fprintf(stderr, "%s\n", message);
}

//------------------------------------------------------------------------------
// RAII Wrappers
//------------------------------------------------------------------------------

static SAHooks GetDuckDBHooks(FileSystem &fs) {
	SAHooks hooks;
	hooks.FOpen = DuckDBShapefileOpen;
	hooks.FRead = DuckDBShapefileRead;
	hooks.FWrite = DuckDBShapefileWrite;
	hooks.FSeek = DuckDBShapefileSeek;
	hooks.FTell = DuckDBShapefileTell;
	hooks.FFlush = DuckDBShapefileFlush;
	hooks.FClose = DuckDBShapefileClose;
	hooks.Remove = DuckDBShapefileRemove;

	hooks.Error = DuckDBShapefileError;
	hooks.Atof = std::atof;
	hooks.userData = &fs;
	return hooks;
}

struct DBFHandleDeleter {
	void operator()(DBFInfo *info) {
		if (info) {
			DBFClose(info);
		}
	}
};
using DBFHandlePtr = unique_ptr<DBFInfo, DBFHandleDeleter>;

DBFHandlePtr OpenDBFFile(FileSystem &fs, const string &filename) {
	auto hooks = GetDuckDBHooks(fs);
	auto handle = DBFOpenLL(filename.c_str(), "rb", &hooks);

	if (!handle) {
		throw IOException("Failed to open DBF file %s", filename.c_str());
	}

	return DBFHandlePtr(handle);
}


struct SHPHandleDeleter {
	void operator()(SHPInfo *info) {
		if (info) {
			SHPClose(info);
		}
	}
};
using SHPHandlePtr = unique_ptr<SHPInfo, SHPHandleDeleter>;

static SHPHandlePtr OpenSHPFile(FileSystem& fs, const char *filename) {
	auto hooks = GetDuckDBHooks(fs);
	auto handle = SHPOpenLL(filename, "rb", &hooks);
	if (!handle) {
		throw IOException("Failed to open SHP file %s", filename);
	}
	return SHPHandlePtr(handle);
}

struct SHPObjectDeleter {
	void operator()(SHPObject *obj) {
		if (obj) {
			SHPDestroyObject(obj);
		}
	}
};

using SHPObjectPtr = unique_ptr<SHPObject, SHPObjectDeleter>;

enum class AttributeEncoding {
	UTF8,
	LATIN1,
	BLOB,
};

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

struct ShapefileBindData : TableFunctionData {
	string file_name;
	int shape_count;
	int shape_type;
	double min_bound[4];
	double max_bound[4];
	AttributeEncoding attribute_encoding;
	vector<LogicalType> attribute_types;

	explicit ShapefileBindData(string file_name_p)
	    : file_name(std::move(file_name_p)),
		  shape_count(0),
	      shape_type(0),
	      min_bound{0, 0, 0, 0},
	      max_bound{0, 0, 0, 0},
	      attribute_encoding(AttributeEncoding::LATIN1)
	{ }
};


static unique_ptr<FunctionData> Bind(ClientContext &context,
                                     TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types,
                                     vector<string> &names) {

	auto file_name = StringValue::Get(input.inputs[0]);
	auto result = make_uniq<ShapefileBindData>(file_name);

	auto &fs = FileSystem::GetFileSystem(context);
	auto shp_handle = OpenSHPFile(fs, file_name.c_str());

	// Get info about the geometry
	SHPGetInfo(shp_handle.get(), &result->shape_count, &result->shape_type, result->min_bound, result->max_bound);

	// Ensure we have a supported shape type
	auto valid_types = { SHPT_NULL, SHPT_POINT, SHPT_ARC, SHPT_POLYGON, SHPT_MULTIPOINT };
	bool is_valid_type = false;
	for(auto type : valid_types) {
		if (result->shape_type == type) {
			is_valid_type = true;
			break;
		}
	}
	if (!is_valid_type) {
		throw InvalidInputException("Invalid shape type %d", result->shape_type);
	}

	auto base_name = file_name.substr(0, file_name.find_last_of('.'));

	// A standards compliant shapefile should use ISO-8859-1 encoding for attributes, but it can be overridden
	// by a .cpg file. So check if there is a .cpg file, if so use that to determine the encoding
	auto cpg_file = base_name + ".cpg";
	if (fs.FileExists(cpg_file)) {
		auto cpg_handle = fs.OpenFile(cpg_file, FileFlags::FILE_FLAGS_READ);
		auto cpg_type = StringUtil::Lower(cpg_handle->ReadLine());
		if (cpg_type == "utf-8") {
			result->attribute_encoding = AttributeEncoding::UTF8;
		} else if (cpg_type == "iso-8859-1") {
			result->attribute_encoding = AttributeEncoding::LATIN1;
		} else {
			// Otherwise, parse as blob
			result->attribute_encoding = AttributeEncoding::BLOB;
		}
	}

	for(auto &kv : input.named_parameters) {
		if(kv.first == "encoding") {
			auto encoding = StringUtil::Lower(StringValue::Get(kv.second));
			if(encoding == "utf-8") {
				result->attribute_encoding = AttributeEncoding::UTF8;
			} else if(encoding == "iso-8859-1") {
				result->attribute_encoding = AttributeEncoding::LATIN1;
			} else if(encoding == "blob") {
				// Otherwise, parse as blob
				result->attribute_encoding = AttributeEncoding::BLOB;
			} else {
				vector<string> candidates = { "utf-8", "iso-8859-1", "blob" };
				auto msg = StringUtil::CandidatesErrorMessage(candidates, encoding, "encoding");
				throw InvalidInputException("Invalid encoding %s", encoding.c_str());
			}
		}
		if(kv.first == "spatial_filter_box") {
			auto filter_box = StructValue::GetChildren(kv.second);
		}
	}

	// Get info about the attributes
	// Remove file extension and replace with .dbf
	auto dbf_handle = OpenDBFFile(fs, base_name + ".dbf");

	// TODO: Try to get the encoding from the dbf if there is no .cpg file
	// auto code_page = DBFGetCodePage(dbf_handle.get());
	// if(!has_cpg_file && code_page != 0) { }

	// Then return the attributes
	auto field_count = DBFGetFieldCount(dbf_handle.get());
	char field_name[12]; // Max field name length is 11 + null terminator
	int field_width = 0;
	int field_precision = 0;
	memset(field_name, 0, sizeof(field_name));

	for (int i = 0; i < field_count; i++) {
		auto field_type = DBFGetFieldInfo(dbf_handle.get(), i, field_name, &field_width, &field_precision);

		LogicalType type;
		switch (field_type) {
		case FTString:
			type = result->attribute_encoding == AttributeEncoding::BLOB ? LogicalType::BLOB : LogicalType::VARCHAR;
			break;
		case FTInteger:
			type = LogicalType::INTEGER;
			break;
		case FTDouble:
			if(field_precision == 0 && field_width < 19) {
				type = LogicalType::BIGINT;
			} else {
				type = LogicalType::DOUBLE;
			}
			break;
		case FTDate:
			// Dates are stored as 8-char strings
			// YYYYMMDD
			type = LogicalType::DATE;
			break;
		case FTLogical:
			type = LogicalType::BOOLEAN;
			break;
		default:
			throw InvalidInputException("DBF field type %d not supported", field_type);
		}
		names.emplace_back(field_name);
		return_types.push_back(type);
		result->attribute_types.push_back(type);
	}

	// Always return geometry last
	return_types.push_back(GeoTypes::GEOMETRY());
	names.push_back("geom");

	// Deduplicate field names if necessary
	for(size_t i = 0; i < names.size(); i++) {
		idx_t count = 1;
		for(size_t j = i + 1; j < names.size(); j++) {
			if(names[i] == names[j]) {
				names[j] += "_" + std::to_string(count++);
			}
		}
	}

	return std::move(result);
}

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------

struct ShapefileGlobalState : public GlobalTableFunctionState {
	int shape_idx;
	SHPHandlePtr shp_handle;
	DBFHandlePtr dbf_handle;
	GeometryFactory factory;
	vector<idx_t> column_ids;

	explicit ShapefileGlobalState(ClientContext &context, const string &file_name, vector<idx_t> column_ids_p)
	    : shape_idx(0), factory(BufferAllocator::Get(context)), column_ids(std::move(column_ids_p)) {
		auto &fs = FileSystem::GetFileSystem(context);

		shp_handle = OpenSHPFile(fs, file_name.c_str());

		// Remove file extension and replace with .dbf
		auto dot_idx = file_name.find_last_of('.');
		auto base_name = file_name.substr(0, dot_idx);
		dbf_handle = OpenDBFFile(fs, base_name + ".dbf");
	}
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ShapefileBindData>();
	auto result = make_uniq<ShapefileGlobalState>(context, bind_data.file_name, input.column_ids);
	return std::move(result);
}

//------------------------------------------------------------------------------
// Geometry Conversion
//------------------------------------------------------------------------------

struct ConvertPoint {
	static Geometry Convert(SHPObjectPtr &shape, GeometryFactory &factory) {
		return factory.CreatePoint(shape->padfX[0], shape->padfY[0]);
	}
};

struct ConvertLineString {
	static Geometry Convert(SHPObjectPtr &shape, GeometryFactory &factory) {
		if(shape->nParts == 1) {
			// Single LineString
			auto line_string = factory.CreateLineString(shape->nVertices);
			for (int i = 0; i < shape->nVertices; i++) {
				line_string.Vertices().Add(Vertex(shape->padfX[i], shape->padfY[i]));
			}
			return line_string;
		} else {
			// MultiLineString
			auto multi_line_string = factory.CreateMultiLineString(shape->nParts);
			auto start = shape->panPartStart[0];
			for (int i = 0; i < shape->nParts; i++) {
				auto end = i == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[i + 1];
				auto &line_string = multi_line_string[i];
				line_string = factory.CreateLineString(end - start);
				for (int j = start; j < end; j++) {
					line_string.Vertices().Add(Vertex(shape->padfX[j], shape->padfY[j]));
				}
				start = end;
			}
			return multi_line_string;
		}
	}
};

struct ConvertPolygon {
	static Geometry Convert(SHPObjectPtr &shape, GeometryFactory &factory) {
		// First off, check if there are more than one polygon.
		// Each polygon is identified by a part with clockwise winding order
		// we calculate the winding order by checking the sign of the area
		vector<int> polygon_part_starts;
		for(int i = 0; i < shape->nParts; i++) {
			auto start = shape->panPartStart[i];
			auto end = i == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[i + 1];
			double area = 0;
			for(int j = start; j < end - 1; j++) {
				area += (shape->padfX[j] * shape->padfY[j + 1]) - (shape->padfX[j + 1] * shape->padfY[j]);
			}
			if(area < 0) {
				polygon_part_starts.push_back(i);
			}
		}
		if(polygon_part_starts.size() < 2) {
			// Single polygon, every part is an interior ring
			// Even if the polygon is counter-clockwise (which should not happen for shapefiles).
			// we still fall back and convert it to a single polygon.
			auto polygon = factory.CreatePolygon(shape->nParts);
			auto start = shape->panPartStart[0];
			for (int i = 0; i < shape->nParts; i++) {
				auto end = i == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[i + 1];
				auto &ring = polygon.Ring(i);
				ring = factory.AllocateVertexVector(end - start);
				for (int j = start; j < end; j++) {
					ring.Add(Vertex(shape->padfX[j], shape->padfY[j]));
				}
				start = end;
			}
			return polygon;
		}
		else {
			// MultiPolygon
			auto multi_polygon = factory.CreateMultiPolygon(polygon_part_starts.size());
			for(size_t polygon_idx = 0; polygon_idx < polygon_part_starts.size(); polygon_idx++) {
				auto part_start = polygon_part_starts[polygon_idx];
				auto part_end = polygon_idx == polygon_part_starts.size() - 1
				                    ? shape->nParts
				                    : polygon_part_starts[polygon_idx + 1];
				auto polygon = factory.CreatePolygon(part_end - part_start);

				for (auto ring_idx = part_start; ring_idx < part_end; ring_idx++) {
					auto start = shape->panPartStart[ring_idx];
					auto end = ring_idx == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[ring_idx + 1];
					auto &ring = polygon.Ring(ring_idx - part_start);
					ring = factory.AllocateVertexVector(end - start);
					for (int j = start; j < end; j++) {
						ring.Add(Vertex(shape->padfX[j], shape->padfY[j]));
					}
				}
				multi_polygon[polygon_idx] = polygon;
			}
			return multi_polygon;
		}
	}
};

struct ConvertMultiPoint {
	static Geometry Convert(SHPObjectPtr &shape, GeometryFactory &factory) {
		auto multi_point = factory.CreateMultiPoint(shape->nVertices);
		for (int i = 0; i < shape->nVertices; i++) {
			multi_point[i] = factory.CreatePoint(shape->padfX[i], shape->padfY[i]);
		}
		return multi_point;
	}
};

template<class OP>
static void ConvertGeomLoop(Vector &result, int record_start, idx_t count, SHPHandle &shp_handle, GeometryFactory &factory) {
	for(idx_t result_idx = 0; result_idx < count; result_idx++) {
		auto shape = SHPObjectPtr(SHPReadObject(shp_handle, record_start++));
		if(shape->nSHPType == SHPT_NULL) {
			FlatVector::SetNull(result, result_idx, true);
		} else {
			FlatVector::GetData<string_t>(result)[result_idx] = factory.Serialize(result, OP::Convert(shape, factory));
		}
	}
}

static void ConvertGeometryVector(Vector &result, int record_start, idx_t count, SHPHandle shp_handle, GeometryFactory &factory, int geom_type) {
	switch(geom_type) {
		case SHPT_NULL:
			FlatVector::Validity(result).SetAllInvalid(count);
			break;
		case SHPT_POINT:
		    ConvertGeomLoop<ConvertPoint>(result, record_start, count, shp_handle, factory);
		break;
		case SHPT_ARC:
			ConvertGeomLoop<ConvertLineString>(result, record_start, count, shp_handle, factory);
		break;
		case SHPT_POLYGON:
			ConvertGeomLoop<ConvertPolygon>(result, record_start, count, shp_handle, factory);
		break;
		case SHPT_MULTIPOINT:
		 	ConvertGeomLoop<ConvertMultiPoint>(result, record_start, count, shp_handle, factory);
		break;
	    default:
		    throw InvalidInputException("Shape type %d not supported", geom_type);
	}
}

//------------------------------------------------------------------------------
// Attribute Conversion
//------------------------------------------------------------------------------

struct ConvertBlobAttribute {
	using TYPE = string_t;
	static string_t Convert(Vector& result, DBFHandle dbf_handle, int record_idx, int field_idx) {
		auto value = DBFReadStringAttribute(dbf_handle, record_idx, field_idx);
		return StringVector::AddString(result, const_char_ptr_cast(value));
	}
};

struct ConvertIntegerAttribute {
	using TYPE = int32_t;
	static int32_t Convert(Vector&, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return DBFReadIntegerAttribute(dbf_handle, record_idx, field_idx);
	}
};

struct ConvertBigIntAttribute {
	using TYPE = int64_t;
	static int64_t Convert(Vector&, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return static_cast<int64_t>(DBFReadDoubleAttribute(dbf_handle, record_idx, field_idx));
	}
};

struct ConvertDoubleAttribute {
	using TYPE = double;
	static double Convert(Vector&, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return DBFReadDoubleAttribute(dbf_handle, record_idx, field_idx);
	}
};

struct ConvertDateAttribute {
	using TYPE = date_t;
	static date_t Convert(Vector&, DBFHandle dbf_handle, int record_idx, int field_idx) {
		// XBase stores dates as 8-char strings (without separators)
		// but DuckDB expects a date string with separators.
		auto value = DBFReadStringAttribute(dbf_handle, record_idx, field_idx);
		char date_with_separator[11];
		memcpy(date_with_separator, value, 4);
		date_with_separator[4] = '-';
		memcpy(date_with_separator + 5, value + 4, 2);
		date_with_separator[7] = '-';
		memcpy(date_with_separator + 8, value + 6, 2);
		date_with_separator[10] = '\0';
		return Date::FromString(date_with_separator);
	}
};

struct ConvertBooleanAttribute {
	using TYPE = bool;
	static bool Convert(Vector& result, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return *DBFReadLogicalAttribute(dbf_handle, record_idx, field_idx) == 'T';
	}
};

template<class OP>
static void ConvertAttributeLoop(Vector &result, int record_start, idx_t count, DBFHandle dbf_handle, int field_idx) {
	int record_idx = record_start;
	for(idx_t row_idx = 0; row_idx < count; row_idx++) {
		if(DBFIsAttributeNULL(dbf_handle, record_idx, field_idx)) {
			FlatVector::SetNull(result, row_idx, true);
		} else {
			FlatVector::GetData<typename OP::TYPE>(result)[row_idx] = OP::Convert(result, dbf_handle, record_idx, field_idx);
		}
		record_idx++;
	}
}

static void ConvertStringAttributeLoop(Vector &result, int record_start, idx_t count, DBFHandle dbf_handle, int field_idx, AttributeEncoding attribute_encoding) {
	int record_idx = record_start;
	vector<data_t> conversion_buffer;
	for(idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (DBFIsAttributeNULL(dbf_handle, record_idx, field_idx)) {
			FlatVector::SetNull(result, row_idx, true);
		} else {
			auto string_bytes = DBFReadStringAttribute(dbf_handle, record_idx, field_idx);
			string_t result_str;
			if (attribute_encoding == AttributeEncoding::LATIN1) {
				conversion_buffer.reserve(strlen(string_bytes) * 2 + 1); // worst case (all non-ascii chars)
				auto out_len = EncodingUtil::LatinToUTF8Buffer(const_data_ptr_cast(string_bytes), conversion_buffer.data());
				result_str = StringVector::AddString(result, const_char_ptr_cast(conversion_buffer.data()), out_len);
			} else {
				result_str = StringVector::AddString(result, const_char_ptr_cast(string_bytes));
			}
			if (!Utf8Proc::IsValid(result_str.GetDataUnsafe(), result_str.GetSize())) {
				throw InvalidInputException("Could not decode VARCHAR field as valid UTF-8, try passing encoding='blob' to skip decoding of string attributes");
			}
			FlatVector::GetData<string_t>(result)[row_idx] = result_str;
		}
		record_idx++;
	}
}

static void ConvertAttributeVector(Vector &result, int record_start, idx_t count, DBFHandle dbf_handle, int field_idx,
                                   AttributeEncoding attribute_encoding) {
	switch(result.GetType().id()) {
		case LogicalTypeId::BLOB:
			ConvertAttributeLoop<ConvertBlobAttribute>(result, record_start, count, dbf_handle, field_idx);
			break;
		case LogicalTypeId::VARCHAR:
		    ConvertStringAttributeLoop(result, record_start, count, dbf_handle, field_idx, attribute_encoding);
			break;
		case LogicalTypeId::INTEGER:
			ConvertAttributeLoop<ConvertIntegerAttribute>(result, record_start, count, dbf_handle, field_idx);
			break;
		case LogicalTypeId::BIGINT:
			ConvertAttributeLoop<ConvertBigIntAttribute>(result, record_start, count, dbf_handle, field_idx);
			break;
		case LogicalTypeId::DOUBLE:
			ConvertAttributeLoop<ConvertDoubleAttribute>(result, record_start, count, dbf_handle, field_idx);
			break;
		case LogicalTypeId::DATE:
			ConvertAttributeLoop<ConvertDateAttribute>(result, record_start, count, dbf_handle, field_idx);
			break;
		case LogicalTypeId::BOOLEAN:
			ConvertAttributeLoop<ConvertBooleanAttribute>(result, record_start, count, dbf_handle, field_idx);
			break;
		default:
			throw InvalidInputException("Attribute type %s not supported", result.GetType().ToString());
	}
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ShapefileBindData>();
	auto &gstate = input.global_state->Cast<ShapefileGlobalState>();

	// Reset the buffer allocator
	gstate.factory.allocator.Reset();

	// Calculate how many record we can fit in the output
	auto output_size = std::min<int>(STANDARD_VECTOR_SIZE, bind_data.shape_count - gstate.shape_idx);
	int record_start = gstate.shape_idx;
	for(auto col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {

		// Projected column indices
		auto projected_col_idx = gstate.column_ids[col_idx];

		auto &col_vec = output.data[col_idx];
		if(col_vec.GetType() == GeoTypes::GEOMETRY()) {
			ConvertGeometryVector(col_vec, record_start, output_size, gstate.shp_handle.get(), gstate.factory, bind_data.shape_type);
		} else {
			// The geometry is always last, so we can use the projected column index directly
			auto field_idx = projected_col_idx;
			ConvertAttributeVector(col_vec, record_start, output_size, gstate.dbf_handle.get(), (int)field_idx, bind_data.attribute_encoding);
		}
	}
	// Update the shape index
	gstate.shape_idx += output_size;

	// Set the cardinality of the output
	output.SetCardinality(output_size);
}

//------------------------------------------------------------------------------
// Progress, Cardinality and Replacement Scans
//------------------------------------------------------------------------------

static double GetProgress(ClientContext &context,
                          const FunctionData *bind_data_p,
                          const GlobalTableFunctionState *global_state) {

	auto &gstate = global_state->Cast<ShapefileGlobalState>();
	auto &bind_data = bind_data_p->Cast<ShapefileBindData>();

	return (double)gstate.shape_idx / (double)bind_data.shape_count;
}

static unique_ptr<NodeStatistics> GetCardinality(ClientContext &context, const FunctionData *data) {
	auto &bind_data = data->Cast<ShapefileBindData>();
	auto result = make_uniq<NodeStatistics>();

	// This is the maximum number of shapes in a single file
	result->has_max_cardinality = true;
	result->max_cardinality = bind_data.shape_count;

	return result;
}

static unique_ptr<TableRef> GetReplacementScan(ClientContext &context, const string &table_name, ReplacementScanData *data) {
	// Check if the table name ends with .shp
	if (!StringUtil::EndsWith(StringUtil::Lower(table_name), ".shp")) {
		return nullptr;
	}

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("ST_ReadSHP", std::move(children));
	return std::move(table_function);
}


//------------------------------------------------------------------------------
// Shapefile Metadata Function
//------------------------------------------------------------------------------
struct ShapeFileMetaBindData : public TableFunctionData {
	vector<string> files;
};

struct ShapeTypeEntry {
	int shp_type;
	const char* shp_name;
};

static ShapeTypeEntry shape_type_map[] = {
	{ SHPT_NULL, "NULL" },
	{ SHPT_POINT, "POINT" },
	{ SHPT_ARC, "LINESTRING" },
	{ SHPT_POLYGON, "POLYGON" },
	{ SHPT_MULTIPOINT, "MULTIPOINT" },
	{ SHPT_POINTZ, "POINTZ" },
	{ SHPT_ARCZ, "LINESTRINGZ" },
	{ SHPT_POLYGONZ, "POLYGONZ" },
	{ SHPT_MULTIPOINTZ, "MULTIPOINTZ" },
	{ SHPT_POINTM, "POINTM" },
	{ SHPT_ARCM, "LINESTRINGM" },
	{ SHPT_POLYGONM, "POLYGONM" },
	{ SHPT_MULTIPOINTM, "MULTIPOINTM" },
	{ SHPT_MULTIPATCH, "MULTIPATCH" },
};

static unique_ptr<FunctionData> ShapeFileMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ShapeFileMetaBindData>();
	auto files = MultiFileReader::GetFileList(context, input.inputs[0], "ShapeFiles", FileGlobOptions::ALLOW_EMPTY);
	for (auto &file : files) {
		if(StringUtil::EndsWith(StringUtil::Lower(file), ".shp")) {
			result->files.push_back(file);
		}
	}

	auto shape_type_count = sizeof(shape_type_map) / sizeof(ShapeTypeEntry);
	auto varchar_vector = Vector(LogicalType::VARCHAR, shape_type_count);
	auto varchar_data = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t i = 0; i < shape_type_count; i++) {
		auto str = string_t(shape_type_map[i].shp_name);
		varchar_data[i] = str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}
	auto shape_type_enum = LogicalType::ENUM("SHAPE_TYPE", varchar_vector, shape_type_count);
	shape_type_enum.SetAlias("SHAPE_TYPE");

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(shape_type_enum);
	return_types.push_back(GeoTypes::BOX_2D());
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("name");
	names.push_back("shape_type");
	names.push_back("bounds");
	names.push_back("count");
	return std::move(result);
}

struct ShapeFileMetaGlobalState : public GlobalTableFunctionState {
	ShapeFileMetaGlobalState() : current_file_idx(0) {
	}
	idx_t current_file_idx;
	vector<string> files;
};

static unique_ptr<GlobalTableFunctionState> ShapeFileMetaInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ShapeFileMetaBindData>();
	auto result = make_uniq<ShapeFileMetaGlobalState>();

	result->files = bind_data.files;
	result->current_file_idx = 0;

	return std::move(result);
}

static void ShapeFileMetaExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ShapeFileMetaBindData>();
	auto &state = input.global_state->Cast<ShapeFileMetaGlobalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	auto &file_name_vector = output.data[0];
	auto file_name_data = FlatVector::GetData<string_t>(file_name_vector);
	auto &shape_type_vector = output.data[1];
	auto shape_type_data = FlatVector::GetData<uint8_t>(shape_type_vector);
	auto &bounds_vector = output.data[2];
	auto &bounds_vector_children = StructVector::GetEntries(bounds_vector);
	auto minx_data = FlatVector::GetData<double>(*bounds_vector_children[0]);
	auto miny_data = FlatVector::GetData<double>(*bounds_vector_children[1]);
	auto maxx_data = FlatVector::GetData<double>(*bounds_vector_children[2]);
	auto maxy_data = FlatVector::GetData<double>(*bounds_vector_children[3]);
	auto record_count_vector = output.data[3];
	auto record_count_data = FlatVector::GetData<int32_t>(record_count_vector);

	auto output_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.files.size() - state.current_file_idx);

	for (idx_t out_idx = 0; out_idx < output_count; out_idx++) {
		auto &file_name = bind_data.files[state.current_file_idx + out_idx];

		auto file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
		auto shp_handle = OpenSHPFile(fs, file_name.c_str());

		double min_bound[4];
		double max_bound[4];
		int shape_type;
		int record_count;
		SHPGetInfo(shp_handle.get(), &record_count, &shape_type, min_bound, max_bound);
		file_name_data[out_idx] = StringVector::AddString(file_name_vector, file_name);
		shape_type_data[out_idx] = 0;
		for(auto shape_type_idx = 0; shape_type_idx < sizeof(shape_type_map) / sizeof(ShapeTypeEntry); shape_type_idx++) {
			if(shape_type_map[shape_type_idx].shp_type == shape_type) {
				shape_type_data[out_idx] = shape_type_idx;
				break;
			}
		}
		minx_data[out_idx] = min_bound[0];
		miny_data[out_idx] = min_bound[1];
		maxx_data[out_idx] = max_bound[0];
		maxy_data[out_idx] = max_bound[1];
		record_count_data[out_idx] = record_count;
	}

	state.current_file_idx += output_count;
	output.SetCardinality(output_count);
}

static double ShapeFileMetaProgress(ClientContext &context, const FunctionData *bind_data,
                                    const GlobalTableFunctionState *gstate) {
	auto &state = gstate->Cast<ShapeFileMetaGlobalState>();
	return static_cast<double>(state.current_file_idx) / static_cast<double>(state.files.size());
}

static unique_ptr<NodeStatistics> ShapeFileMetaCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ShapeFileMetaBindData>();
	auto result = make_uniq<NodeStatistics>();
	result->has_max_cardinality = true;
	result->max_cardinality = bind_data.files.size();
	result->has_estimated_cardinality = true;
	result->estimated_cardinality = bind_data.files.size();
	return result;
}

//------------------------------------------------------------------------------
// Register table function(s)
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterShapefileTableFunction(DatabaseInstance &db) {
	TableFunction read_func("ST_ReadSHP", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal);

	read_func.named_parameters["encoding"] = LogicalType::VARCHAR;
	read_func.table_scan_progress = GetProgress;
	read_func.cardinality = GetCardinality;
	read_func.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, read_func);

	TableFunction meta_func("shapefile_meta", {LogicalType::VARCHAR}, ShapeFileMetaExecute, ShapeFileMetaBind, ShapeFileMetaInitGlobal);
	meta_func.table_scan_progress = ShapeFileMetaProgress;
	meta_func.cardinality = ShapeFileMetaCardinality;
	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(meta_func));

	// Replacement scan
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(GetReplacementScan);
}



}

}

