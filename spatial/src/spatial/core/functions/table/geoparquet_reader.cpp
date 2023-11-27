//
// Created by Aske Wachs on 17/11/2023.
//

#include "spatial/core/functions/geoparquet_reader.hpp"

#include "callback_column_reader.hpp"
#include "cast_column_reader.hpp"
#include "column_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "list_column_reader.hpp"
#include "row_number_column_reader.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"

#include <spatial/core/geometry/geometry_factory.hpp>

namespace spatial {
namespace core {
namespace geoparquet {

using duckdb::make_uniq;
using duckdb::IOException;
using duckdb::FieldRepetitionType;
using duckdb_parquet::format::ConvertedType;
using duckdb::ListColumnReader;
using duckdb::StructColumnReader;


GeoparquetReader::GeoparquetReader(ClientContext &context, string file_name, ParquetOptions parquet_options)
    : ParquetReader(context, file_name, parquet_options) {
	GeoparquetReader::InitializeSchema();
}
GeoparquetReader::GeoparquetReader(ClientContext &context, ParquetOptions parquet_options,
                                   shared_ptr<ParquetFileMetadataCache> metadata)
	: ParquetReader(context, parquet_options, metadata) {
	GeoparquetReader::InitializeSchema();
}

inline static bool HasGeometryColumnName(const std::string& column_name) {
	return column_name == "geometry" || column_name == "GEOMETRY"
	       || column_name == "geom"  || column_name == "GEOM"
	       || column_name == "wkb" 	|| column_name == "WKB";

}

void GeoparquetReader::InitializeSchema() {
	names = vector<string>();
	return_types = vector<LogicalType>();
	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		throw Exception("Encrypted Parquet files are not supported");
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw Exception("Need at least one non-root column in the file");
	}
	root_reader = CreateReader();
	auto &root_type = root_reader->Type();
	auto &child_types = StructType::GetChildTypes(root_type);
	D_ASSERT(root_type.id() == LogicalTypeId::STRUCT);
	for (auto &type_pair : child_types) {
		auto col_name = type_pair.first;
		names.push_back(col_name);
		if (HasGeometryColumnName(col_name)) {
			return_types.push_back(spatial::core::GeoTypes::GEOMETRY());
		} else {
			return_types.push_back(type_pair.second);
		}
	}

	// Add generated constant column for row number
	if (parquet_options.file_row_number) {
		if (std::find(names.begin(), names.end(), "file_row_number") != names.end()) {
			throw BinderException(
			    "Using file_row_number option on file with column named file_row_number is not supported");
		}
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("file_row_number");
	}
}

unique_ptr<ColumnReader> GeoparquetReader::CreateReader() {
	auto file_meta_data = GetFileMetadata();
	idx_t next_schema_idx = 0;
	idx_t next_file_idx = 0;

	if (file_meta_data->schema.empty()) {
		throw IOException("Parquet reader: no schema elements found");
	}
	if (file_meta_data->schema[0].num_children == 0) {
		throw IOException("Parquet reader: root schema element has no children");
	}
	auto ret = CreateReaderRecursive(0, 0, 0, next_schema_idx, next_file_idx);
	if (ret->Type().id() != duckdb::LogicalTypeId::STRUCT) {
		throw duckdb::InvalidInputException("Root element of Parquet file must be a struct");
	}
	D_ASSERT(next_schema_idx == file_meta_data->schema.size() - 1);
	D_ASSERT(file_meta_data->row_groups.empty() || next_file_idx == file_meta_data->row_groups[0].columns.size());

	auto &root_struct_reader = ret->Cast<duckdb::StructColumnReader>();
	// add casts if required
	for (auto &entry : reader_data.cast_map) {
		auto column_idx = entry.first;
		auto &expected_type = entry.second;
		auto child_reader = std::move(root_struct_reader.child_readers[column_idx]);
		auto cast_reader = make_uniq<duckdb::CastColumnReader>(std::move(child_reader), expected_type);
		root_struct_reader.child_readers[column_idx] = std::move(cast_reader);
	}
	if (parquet_options.file_row_number) {
		root_struct_reader.child_readers.push_back(
		    make_uniq<duckdb::RowNumberColumnReader>(*this, LogicalType::BIGINT, SchemaElement(), next_file_idx, 0, 0));
	}
	return ret;
}

unique_ptr<ColumnReader> GeoparquetReader::CreateReaderRecursive(idx_t depth, idx_t max_define, idx_t max_repeat,
                                                              idx_t &next_schema_idx, idx_t &next_file_idx) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(file_meta_data);
	D_ASSERT(next_schema_idx < file_meta_data->schema.size());
	auto &s_ele = file_meta_data->schema[next_schema_idx];
	auto this_idx = next_schema_idx;

	auto repetition_type = FieldRepetitionType::REQUIRED;
	if (s_ele.__isset.repetition_type && this_idx > 0) {
		repetition_type = s_ele.repetition_type;
	}
	if (repetition_type != FieldRepetitionType::REQUIRED) {
		max_define++;
	}
	if (repetition_type == FieldRepetitionType::REPEATED) {
		max_repeat++;
	}
	if (s_ele.__isset.num_children && s_ele.num_children > 0) { // inner node
		duckdb::child_list_t<LogicalType> child_types;
		vector<unique_ptr<ColumnReader>> child_readers;

		idx_t c_idx = 0;
		while (c_idx < (idx_t)s_ele.num_children) {
			next_schema_idx++;

			auto &child_ele = file_meta_data->schema[next_schema_idx];

			auto child_reader =
			    CreateReaderRecursive(depth + 1, max_define, max_repeat, next_schema_idx, next_file_idx);
			child_types.push_back(make_pair(child_ele.name, child_reader->Type()));
			child_readers.push_back(std::move(child_reader));

			c_idx++;
		}
		D_ASSERT(!child_types.empty());
		unique_ptr<ColumnReader> result;
		LogicalType result_type;

		bool is_repeated = repetition_type == FieldRepetitionType::REPEATED;
		bool is_list = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::LIST;
		bool is_map = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP;
		bool is_map_kv = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP_KEY_VALUE;
		if (!is_map_kv && this_idx > 0) {
			// check if the parent node of this is a map
			auto &p_ele = file_meta_data->schema[this_idx - 1];
			bool parent_is_map = p_ele.__isset.converted_type && p_ele.converted_type == ConvertedType::MAP;
			bool parent_has_children = p_ele.__isset.num_children && p_ele.num_children == 1;
			is_map_kv = parent_is_map && parent_has_children;
		}

		if (is_map_kv) {
			if (child_types.size() != 2) {
				throw IOException("MAP_KEY_VALUE requires two children");
			}
			if (!is_repeated) {
				throw IOException("MAP_KEY_VALUE needs to be repeated");
			}
			result_type = LogicalType::MAP(std::move(child_types[0].second), std::move(child_types[1].second));

			auto struct_reader =
			    make_uniq<StructColumnReader>(*this, duckdb::ListType::GetChildType(result_type), s_ele, this_idx,
			                                  max_define - 1, max_repeat - 1, std::move(child_readers));
			return make_uniq<ListColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                   std::move(struct_reader));
		}
		if (child_types.size() > 1 || (!is_list && !is_map && !is_repeated)) {
			result_type = LogicalType::STRUCT(child_types);
			result = make_uniq<StructColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                       std::move(child_readers));
		} else {
			// if we have a struct with only a single type, pull up
			result_type = child_types[0].second;
			result = std::move(child_readers[0]);
		}
		if (is_repeated) {
			result_type = LogicalType::LIST(result_type);
			return make_uniq<ListColumnReader>(*this, result_type, s_ele, this_idx, max_define, max_repeat,
			                                   std::move(result));
		}
		return result;
	} else { // leaf node
		if (!s_ele.__isset.type) {
			throw duckdb::InvalidInputException(
			    "Node has neither num_children nor type set - this violates the Parquet spec (corrupted file)");
		}
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			const auto derived_type = DeriveLogicalType(s_ele, parquet_options.binary_as_string);
			auto list_type = LogicalType::LIST(derived_type);

			auto element_reader =
			    GeoparquetReader::CreateColumnReader(*this, derived_type, s_ele, next_file_idx++, max_define, max_repeat);

			return make_uniq<ListColumnReader>(*this, list_type, s_ele, this_idx, max_define, max_repeat,
			                                   std::move(element_reader));
		}
		// TODO check return value of derive type or should we only do this on read()
		return GeoparquetReader::CreateColumnReader(*this, DeriveLogicalType(s_ele, parquet_options.binary_as_string), s_ele, next_file_idx++, max_define,
		                                  max_repeat);
	}
}

inline string_t WKBParquetValueConversion::ConvertToSerializedGeometry(char const* data, uint32_t length, spatial::core::GeometryFactory &factory, VectorStringBuffer& buffer) {
	auto geometry = factory.FromWKB(data, length);
    return factory.Serialize(buffer, geometry);
}

string_t WKBParquetValueConversion::DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
	auto& w_reader = reader.Cast<WKBColumnReader>();
	auto str_len = dict.read<uint32_t>();
	dict.available(str_len);
	auto dict_str = reinterpret_cast<const char *>(dict.ptr);
	dict.inc(str_len);
	return WKBParquetValueConversion::ConvertToSerializedGeometry(dict_str, str_len, w_reader.factory, *w_reader.buffer);
}

string_t WKBParquetValueConversion::PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = reader.Cast<WKBColumnReader>();
	uint32_t str_len = plain_data.read<uint32_t>();
	plain_data.available(str_len);
	auto plain_str = char_ptr_cast(plain_data.ptr);
	plain_data.inc(str_len);
	return WKBParquetValueConversion::ConvertToSerializedGeometry(plain_str, str_len, scr.factory,*scr.buffer);
}

void WKBParquetValueConversion::PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
	StringParquetValueConversion::PlainSkip(plain_data, reader);
}

void WKBColumnReader::PrepareDeltaLengthByteArray(ResizeableBuffer &buffer) {
	throw NotImplementedException("WKBColumnReader::PrepareDeltaLengthByteArray(ResizeableBuffer &buffer)");
}
void WKBColumnReader::PrepareDeltaByteArray(ResizeableBuffer &buffer) {
	throw NotImplementedException("WKBColumnReader::PrepareDeltaByteArray(ResizeableBuffer &buffer)");
}
void WKBColumnReader::DeltaByteArray(uint8_t *defines, idx_t num_values, parquet_filter_t &filter, idx_t result_offset,
                    Vector &result) {
    throw NotImplementedException("WKBColumnReader::DeltaByteArray(uint8_t *defines, idx_t num_values, parquet_filter_t &filter, idx_t result_offset, Vector &result)");
}

WKBColumnReader::WKBColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<string_t, WKBParquetValueConversion>(reader, type_p, schema_p, schema_idx_p,
	                                                                    max_define_p, max_repeat_p),
      factory(reader.allocator)
{
	if(type_p == LogicalTypeId::VARCHAR) {
		throw InvalidInputException("WKBColumnReader can only read WKB as BLOBs");
	}
}

void WKBColumnReader::DictReference(Vector &result) {
	auto buf = make_buffer<VectorStringBuffer>();
	this->buffer = buf;
	StringVector::AddBuffer(result, buf);
}

//void WKBColumnReader::PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) {
////	throw NotImplementedException("WKBColumnReader::PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result)");
//}


unique_ptr<ColumnReader> GeoparquetReader::CreateColumnReader(duckdb::ParquetReader &reader,
                                                              const duckdb::LogicalType &type_p,
                                                              const duckdb_parquet::format::SchemaElement &schema_p,
                                                              idx_t file_idx_p, idx_t max_define, idx_t max_repeat) {
	if (type_p.id() == duckdb::LogicalTypeId::BLOB && HasGeometryColumnName(schema_p.name)){
		return make_uniq<WKBColumnReader>
			(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	}
	return ColumnReader::CreateReader(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
}

void GeoparquetReader::InitializeScan(duckdb::ParquetReaderScanState &state, vector<duckdb::idx_t> groups_to_read) {
	ParquetReader::InitializeScan(state, groups_to_read);
	state.root_reader = CreateReader();
}

} // geoparquet
} // core
} // spatial