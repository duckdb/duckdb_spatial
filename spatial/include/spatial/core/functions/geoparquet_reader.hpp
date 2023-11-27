//
// Created by Aske Wachs on 17/11/2023.
//

#ifndef DUCKDB_GEOPARQUET_READER_HPP
#define DUCKDB_GEOPARQUET_READER_HPP

#include "column_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet_file_metadata_cache.hpp"
#include "parquet_reader.hpp"
#include "parquet_types.h"
#include "string_column_reader.hpp"
#include "templated_column_reader.hpp"

#include <spatial/core/geometry/geometry.hpp>
#include <spatial/core/geometry/geometry_factory.hpp>
#include <string>

namespace spatial {
namespace core {
namespace geoparquet {

using duckdb::ParquetReader;
using duckdb::ParquetFileMetadataCache;
using duckdb::ClientContext;
using duckdb::ParquetOptions;
using duckdb::unique_ptr;
using duckdb::LogicalType;
using duckdb::SchemaElement;
using duckdb::FileSystem;
using duckdb::Allocator;
using duckdb::vector;
using duckdb::MultiFileReaderData;
using duckdb::ColumnReader;
using duckdb::ParquetReaderScanState;
using duckdb::DataChunk;
using duckdb::BaseStatistics;
using duckdb::FileHandle;

using std::string;
using std::shared_ptr;

class GeoparquetReader : public ParquetReader {
public:
	explicit GeoparquetReader(ClientContext &context, string file_name, ParquetOptions parquet_options);
	explicit GeoparquetReader(ClientContext &context, ParquetOptions parquet_options,
				  shared_ptr<ParquetFileMetadataCache> metadata);

	void InitializeSchema();
	void InitializeScan(duckdb::ParquetReaderScanState &state, vector<duckdb::idx_t> groups_to_read);
private:
	unique_ptr<ColumnReader> CreateReader();
	unique_ptr<ColumnReader> CreateReaderRecursive(idx_t depth, idx_t max_define, idx_t max_repeat,
	                                               idx_t &next_schema_idx, idx_t &next_file_idx);

	unique_ptr<ColumnReader> CreateColumnReader(ParquetReader &reader, const LogicalType &type_p,
	                                            const SchemaElement &schema_p, idx_t file_idx_p, idx_t max_define,
	                                            idx_t max_repeat);

};

class WKBParquetValueConversion {
public:
	static string_t DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader);
	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader);
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader);
	static string_t ConvertToSerializedGeometry(char const* data, uint32_t length, spatial::core::GeometryFactory &factory, VectorStringBuffer& buffer);
};

class WKBColumnReader : public TemplatedColumnReader<string_t, WKBParquetValueConversion> {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;
	explicit WKBColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                         idx_t max_define_p, idx_t max_repeat_p);

	GeometryFactory factory;
	shared_ptr<VectorStringBuffer> buffer;

public:

	void PrepareDeltaLengthByteArray(ResizeableBuffer &buffer) override;
	void PrepareDeltaByteArray(ResizeableBuffer &buffer) override;
	void DeltaByteArray(uint8_t *defines, idx_t num_values, parquet_filter_t &filter, idx_t result_offset,
	                    Vector &result) override;
protected:
	void DictReference(Vector &result) override;

};

}
}
}


#endif // DUCKDB_GEOPARQUET_READER_HPP
