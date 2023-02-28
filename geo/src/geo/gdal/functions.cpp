#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/gdal/types.hpp"
#include "geo/gdal/functions.hpp"

#include "gdal_priv.h"
#include "ogrsf_frmts.h"

namespace geo {

namespace gdal {

enum SpatialFilterType { Wkb, Rectangle };

struct SpatialFilter {
	SpatialFilterType type;
	SpatialFilter(SpatialFilterType type_p) : type(type_p) {};
};

struct RectangleSpatialFilter : SpatialFilter {
	double min_x, min_y, max_x, max_y;
	RectangleSpatialFilter(double min_x_p, double min_y_p, double max_x_p, double max_y_p)
	    : SpatialFilter(SpatialFilterType::Rectangle), min_x(min_x_p), min_y(min_y_p), max_x(max_x_p), max_y(max_y_p) {
	}
};

struct WKBSpatialFilter : SpatialFilter {
	OGRGeometryH geom;
	WKBSpatialFilter(string wkb_p) : SpatialFilter(SpatialFilterType::Wkb) {
		auto ok = OGR_G_CreateFromWkb(wkb_p.c_str(), nullptr, &geom, wkb_p.size());
		if (ok != OGRERR_NONE) {
			throw Exception("WKBSpatialFilter: could not create geometry from WKB");
		}
	}
	~WKBSpatialFilter() {
		OGR_G_DestroyGeometry(geom);
	}
};

// TODO: Verify that this actually corresponds to the same sql subset expected by OGR SQL
static string FilterToGdal(const TableFilter &filter, const string &column_name) {

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (const ConstantFilter &)filter;
		return column_name + ExpressionTypeToOperator(constant_filter.comparison_type) +
		       constant_filter.constant.ToSQLString();
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = (const ConjunctionAndFilter &)filter;
		vector<string> filters;
		for (const auto &child_filter : and_filter.child_filters) {
			filters.push_back(FilterToGdal(*child_filter, column_name));
		}
		return StringUtil::Join(filters, " AND ");
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = (const ConjunctionOrFilter &)filter;
		vector<string> filters;
		for (const auto &child_filter : or_filter.child_filters) {
			filters.push_back(FilterToGdal(*child_filter, column_name));
		}
		return StringUtil::Join(filters, " OR ");
	}
	case TableFilterType::IS_NOT_NULL: {
		return column_name + " IS NOT NULL";
	}
	case TableFilterType::IS_NULL: {
		return column_name + " IS NULL";
	}
	default:
		throw NotImplementedException("FilterToGdal: filter type not implemented");
	}
}

static string FilterToGdal(const TableFilterSet &set, const vector<idx_t> &column_ids,
                           const vector<string> &column_names) {

	vector<string> filters;
	for (auto &input_filter : set.filters) {
		auto col_idx = column_ids[input_filter.first];
		auto &col_name = column_names[col_idx];
		filters.push_back(FilterToGdal(*input_filter.second, col_name));
	}
	return StringUtil::Join(filters, " AND ");
}

struct GdalScanFunctionData : public TableFunctionData {
	idx_t layer_idx;
	unique_ptr<SpatialFilter> spatial_filter;
	GDALDatasetUniquePtr dataset;
	unordered_map<idx_t, unique_ptr<ArrowConvertData>> arrow_convert_data;
	idx_t max_threads;
	// before they are renamed
	vector<string> all_names;
	vector<LogicalType> all_types;
	atomic<idx_t> lines_read;
};

struct GdalScanLocalState : ArrowScanLocalState {
	explicit GdalScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk)
	    : ArrowScanLocalState(move(current_chunk)) {
	}
};

struct GdalScanGlobalState : ArrowScanGlobalState {};

unique_ptr<FunctionData> GdalTableFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {

	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning GDAL files is disabled through configuration");
	}

	// First scan for "options" parameter
	auto gdal_open_options = vector<char const *>();
	auto options_param = input.named_parameters.find("open_options");
	if (options_param != input.named_parameters.end()) {
		for (auto &param : ListValue::GetChildren(options_param->second)) {
			gdal_open_options.push_back(StringValue::Get(param).c_str());
		}
	}

	auto gdal_allowed_drivers = vector<char const *>();
	auto drivers_param = input.named_parameters.find("allowed_drivers");
	if (drivers_param != input.named_parameters.end()) {
		for (auto &param : ListValue::GetChildren(drivers_param->second)) {
			gdal_allowed_drivers.push_back(StringValue::Get(param).c_str());
		}
	}

	auto gdal_sibling_files = vector<char const *>();
	auto siblings_params = input.named_parameters.find("sibling_files");
	if (siblings_params != input.named_parameters.end()) {
		for (auto &param : ListValue::GetChildren(drivers_param->second)) {
			gdal_sibling_files.push_back(StringValue::Get(param).c_str());
		}
	}

	// Now we can open the dataset
	auto file_name = input.inputs[0].GetValue<string>();
	auto dataset =
	    GDALDatasetUniquePtr(GDALDataset::Open(file_name.c_str(), GDAL_OF_VECTOR | GDAL_OF_VERBOSE_ERROR,
	                                           gdal_allowed_drivers.empty() ? nullptr : gdal_allowed_drivers.data(),
	                                           gdal_open_options.empty() ? nullptr : gdal_open_options.data(),
	                                           gdal_sibling_files.empty() ? nullptr : gdal_sibling_files.data()));

	if (dataset == nullptr) {
		auto error = string(CPLGetLastErrorMsg());
		throw IOException("Could not open file: " + file_name + " (" + error + ")");
	}

	// Double check that the dataset have any layers
	if (dataset->GetLayerCount() <= 0) {
		throw IOException("Dataset does not contain any layers");
	}

	// Now we can bind the additonal options
	auto result = make_unique<GdalScanFunctionData>();

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "layer") {

			// Find layer by index
			if (kv.second.type() == LogicalType::INTEGER) {
				auto layer_idx = IntegerValue::Get(kv.second);
				if (layer_idx < 0) {
					throw BinderException("Layer index must be positive");
				}
				if (layer_idx > dataset->GetLayerCount()) {
					throw BinderException(
					    StringUtil::Format("Layer index too large (%s > %s)", layer_idx, dataset->GetLayerCount()));
				}
				result->layer_idx = (idx_t)layer_idx;
			}

			// Find layer by name
			if (kv.second.type() == LogicalTypeId::VARCHAR) {
				auto name = StringValue::Get(kv.second).c_str();
				bool found = false;
				for (auto layer_idx = 0; layer_idx < dataset->GetLayerCount(); layer_idx++) {
					if (strcmp(dataset->GetLayer(layer_idx)->GetName(), name) == 0) {
						result->layer_idx = (idx_t)layer_idx;
						found = true;
						break;
					}
				}
				if (!found) {
					throw BinderException(StringUtil::Format("Layer '%s' could not be found in dataset"), name);
				}
			}
		}

		if (loption == "spatial_filter_box" && kv.second.type() == core::GeoTypes::BOX_2D) {
			if (result->spatial_filter) {
				throw BinderException("Only one spatial filter can be specified");
			}
			auto &children = StructValue::GetChildren(kv.second);
			auto minx = DoubleValue::Get(children[0]);
			auto miny = DoubleValue::Get(children[1]);
			auto maxx = DoubleValue::Get(children[2]);
			auto maxy = DoubleValue::Get(children[3]);
			result->spatial_filter = make_unique<RectangleSpatialFilter>(minx, miny, maxx, maxy);
		}

		if (loption == "spatial_filter" && kv.second.type() == gdal::GeoTypes::WKB_BLOB) {
			if (result->spatial_filter) {
				throw BinderException("Only one spatial filter can be specified");
			}
			auto wkb = StringValue::Get(kv.second);
			result->spatial_filter = make_unique<WKBSpatialFilter>(wkb);
		}

		if (loption == "max_threads") {
			auto max_threads = IntegerValue::Get(kv.second);
			if (max_threads <= 0) {
				throw BinderException("'max_threads' parameter must be positive");
			}
			result->max_threads = (idx_t)max_threads;
		}
	}

	// set default max_threads
	if (result->max_threads == 0) {
		result->max_threads = context.db->NumberOfThreads();
	}

	// Get the schema for the selected layer
	auto layer = dataset->GetLayer(result->layer_idx);

	struct ArrowArrayStream stream;
	if (!layer->GetArrowStream(&stream)) {
		// layer is owned by GDAL, we do not need to destory it
		throw IOException("Could not get arrow stream from layer");
	}

	struct ArrowSchema schema;
	if (stream.get_schema(&stream, &schema) != 0) {
		if (stream.release) {
			stream.release(&stream);
		}
		throw IOException("Could not get arrow schema from layer");
	}

	// The Arrow API will return attributes in this order
	// 1. FID column
	// 2. all ogr field attributes
	// 3. all geometry columns

	auto attribute_count = schema.n_children;
	auto attributes = schema.children;

	result->all_names.reserve(attribute_count);
	names.reserve(attribute_count);

	for (idx_t col_idx = 0; col_idx < (idx_t)attribute_count; col_idx++) {
		auto &attribute = *attributes[col_idx];

		const char ogc_flag[] = {'\x01', '\0', '\0', '\0', '\x14', '\0', '\0', '\0', 'A', 'R', 'R', 'O', 'W',
		                         ':',    'e',  'x',  't',  'e',    'n',  's',  'i',  'o', 'n', ':', 'n', 'a',
		                         'm',    'e',  '\a', '\0', '\0',   '\0', 'o',  'g',  'c', '.', 'w', 'k', 'b'};

		if (attribute.metadata != nullptr && strncmp(attribute.metadata, ogc_flag, sizeof(ogc_flag)) == 0) {
			// This is a WKB geometry blob
			GetArrowLogicalType(attribute, result->arrow_convert_data, col_idx);
			return_types.emplace_back(gdal::GeoTypes::WKB_BLOB);
		} else if (attribute.dictionary) {
			result->arrow_convert_data[col_idx] =
			    make_unique<ArrowConvertData>(GetArrowLogicalType(attribute, result->arrow_convert_data, col_idx));
			return_types.emplace_back(GetArrowLogicalType(*attribute.dictionary, result->arrow_convert_data, col_idx));
		} else {
			return_types.emplace_back(GetArrowLogicalType(attribute, result->arrow_convert_data, col_idx));
		}

		// keep these around for projection/filter pushdown later
		// does GDAL even allow duplicate/missing names?
		auto s = string(attributes[col_idx]->name);
		result->all_names.push_back(s);

		if (strcmp(attribute.name, "") == 0) {
			names.push_back("v" + to_string(col_idx));
		} else {
			auto s2 = string(attributes[col_idx]->name);
			names.push_back(s2);
		}
	}

	schema.release(&schema);
	stream.release(&stream);

	RenameArrowColumns(names);

	result->dataset = std::move(dataset);
	result->all_types = return_types;

	return result;
};

idx_t GdalTableFunction::MaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	auto data = (const GdalScanFunctionData *)bind_data_p;
	return data->max_threads;
}

// init global
unique_ptr<GlobalTableFunctionState> GdalTableFunction::InitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &data = (GdalScanFunctionData &)*input.bind_data;

	auto global_state = make_unique<GdalScanGlobalState>();

	// Get selected layer
	auto layer = data.dataset->GetLayer(data.layer_idx);

	// Apply spatial filter (if we got one)
	if (data.spatial_filter != nullptr) {
		if (data.spatial_filter->type == SpatialFilterType::Rectangle) {
			auto &rect = (RectangleSpatialFilter &)*data.spatial_filter;
			layer->SetSpatialFilterRect(rect.min_x, rect.min_y, rect.max_x, rect.max_y);
		} else if (data.spatial_filter->type == SpatialFilterType::Wkb) {
			auto &filter = (WKBSpatialFilter &)*data.spatial_filter;
			layer->SetSpatialFilter(OGRGeometry::FromHandle(filter.geom));
		}
	}

	// TODO: Apply projection pushdown

	// Apply predicate pushdown
	// We simply create a string out of the predicates and pass it to GDAL.
	if (input.filters) {
		auto filter_clause = FilterToGdal(*input.filters, input.column_ids, data.all_names);
		layer->SetAttributeFilter(filter_clause.c_str());
	}

	// Create arrow stream from layer

	global_state->stream = make_unique<ArrowArrayStreamWrapper>();

	if (!layer->GetArrowStream(&global_state->stream->arrow_array_stream)) {
		throw IOException("Could not get arrow stream");
	}

	global_state->max_threads = GdalTableFunction::MaxThreads(context, input.bind_data);

	if (input.CanRemoveFilterColumns()) {
		global_state->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				global_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				global_state->scanned_types.push_back(data.all_types[col_idx]);
			}
		}
	}

	return move(global_state);
}

// Scan
void GdalTableFunction::Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (!input.local_state) {
		return;
	}
	auto &data = (GdalScanFunctionData &)*input.bind_data;
	auto &state = (ArrowScanLocalState &)*input.local_state;
	auto &global_state = (GdalScanGlobalState &)*input.global_state;

	//! Out of tuples in this chunk
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		if (!ArrowScanParallelStateNext(context, input.bind_data, state, global_state)) {
			return;
		}
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;

	if (global_state.CanRemoveFilterColumns()) {
		state.all_columns.Reset();
		state.all_columns.SetCardinality(output_size);
		ArrowToDuckDB(state, data.arrow_convert_data, state.all_columns, data.lines_read - output_size, false);
		output.ReferenceColumns(state.all_columns, global_state.projection_ids);
	} else {
		output.SetCardinality(output_size);

		ArrowToDuckDB(state, data.arrow_convert_data, output, data.lines_read - output_size, false);
	}

	output.Verify();
	state.chunk_offset += output.size();
}

void GdalTableFunction::Register(ClientContext &context) {

	TableFunctionSet set("st_read");
	TableFunction scan({LogicalType::VARCHAR}, GdalTableFunction::Scan, GdalTableFunction::Bind,
	                   GdalTableFunction::InitGlobal, ArrowTableFunction::ArrowScanInitLocal);

	scan.cardinality = ArrowTableFunction::ArrowScanCardinality;
	scan.get_batch_index = ArrowTableFunction::ArrowGetBatchIndex;

	scan.projection_pushdown = true;
	scan.filter_pushdown = true;

	scan.named_parameters["open_options"] = LogicalType::LIST(LogicalType::VARCHAR);
	scan.named_parameters["allowed_drivers"] = LogicalType::LIST(LogicalType::VARCHAR);
	scan.named_parameters["sibling_files"] = LogicalType::LIST(LogicalType::VARCHAR);
	scan.named_parameters["spatial_filter_box"] = core::GeoTypes::BOX_2D;
	scan.named_parameters["spatial_filter"] = gdal::GeoTypes::WKB_BLOB;
	set.AddFunction(scan);

	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo info(set);
	catalog.CreateTableFunction(context, &info);
}

} // namespace gdal

} // namespace geo
