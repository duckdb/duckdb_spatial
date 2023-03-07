#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/gdal/functions.hpp"

#include "ogrsf_frmts.h"

namespace geo {

namespace gdal {

struct BindData : public TableFunctionData {

	string filename;
	vector<LogicalType> field_sql_types;
	vector<string> field_names;

	BindData(string filename, vector<LogicalType> field_sql_types, vector<string> field_names) :
		filename(std::move(filename)), field_sql_types(std::move(field_sql_types)), field_names(std::move(field_names)) {}
};

struct LocalState : public LocalFunctionData {

};

struct GlobalState : public GlobalFunctionData {
	mutex lock;
	GDALDatasetUniquePtr dataset;
	OGRLayer *layer;
	vector<unique_ptr<OGRFieldDefn>> field_defs;

	GlobalState(GDALDatasetUniquePtr dataset, OGRLayer *layer, vector<unique_ptr<OGRFieldDefn>> field_defs)
	    : dataset(std::move(dataset)), layer(layer), field_defs(std::move(field_defs)) {}
};

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> Bind(ClientContext &context, CopyInfo &info, vector<string> &names,
                                     vector<LogicalType> &sql_types) {

	auto bind_data = make_unique<BindData>(info.file_path, sql_types, names);

	// check all the options in the copy info
	// and set
	for (auto &option : info.options) {
		// save dataset open options.. i guess?
	}

	return bind_data;
}

//===--------------------------------------------------------------------===//
// Init Local
//===--------------------------------------------------------------------===//
static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {
	auto &gdal_data = (BindData &)bind_data;
	auto local_data = make_unique<LocalState>();
	return std::move(local_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//
static bool IsGeometryType(const LogicalType &type) {
	return type == core::GeoTypes::WKB_BLOB || type == core::GeoTypes::POINT_2D;
}
static unique_ptr<OGRGeomFieldDefn> OGRGeometryFieldTypeFromLogicalType(const string &name, const LogicalType &type) {
	// TODO: Support more geometry types
	if (type == core::GeoTypes::WKB_BLOB) {
		return make_unique<OGRGeomFieldDefn>(name.c_str(), wkbUnknown);
	} else if (type == core::GeoTypes::POINT_2D) {
		return make_unique<OGRGeomFieldDefn>(name.c_str(), wkbPoint);
	} else {
		throw NotImplementedException("Unsupported geometry type");
	}
}
static unique_ptr<OGRFieldDefn> OGRFieldTypeFromLogicalType(const string &name, const LogicalType &type) {
	// TODO: Set OGRFieldSubType for integers and integer lists
	// TODO: Set string width?

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTInteger);
	case LogicalTypeId::BIGINT:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTInteger64);
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTReal);
	case LogicalTypeId::VARCHAR:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTString);
	case LogicalTypeId::BLOB:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTBinary);
	case LogicalTypeId::DATE:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTDate);
	case LogicalTypeId::TIME:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTTime);
	case LogicalTypeId::TIMESTAMP:
		return make_unique<OGRFieldDefn>(name.c_str(), OFTDateTime);
	case LogicalTypeId::LIST: {
		auto child_type = ListType::GetChildType(type);
		switch (child_type.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
			return make_unique<OGRFieldDefn>(name.c_str(), OFTIntegerList);
		case LogicalTypeId::BIGINT:
			return make_unique<OGRFieldDefn>(name.c_str(), OFTInteger64List);
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return make_unique<OGRFieldDefn>(name.c_str(), OFTRealList);
		case LogicalTypeId::VARCHAR:
			return make_unique<OGRFieldDefn>(name.c_str(), OFTStringList);
		default:
			throw NotImplementedException("Unsupported type for OGR: %s", type.ToString());
		}
	}
	default:
		throw NotImplementedException("Unsupported type for OGR: %s", type.ToString());
	}
}
static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data, const string &file_path) {
	//auto gdal_data = (BindData&)bind_data;
	//auto global_data = make_unique<GlobalState>(file_path, "FlatGeobuf");
	//return std::move(global_data);

	auto &gdal_data = (BindData &)bind_data;

	auto driver_name = "ESRI Shapefile";
	GDALDriver *driver = GetGDALDriverManager()->GetDriverByName(driver_name);
	if (!driver) {
		throw IOException("Could not open driver");
	}

	auto dataset = GDALDatasetUniquePtr(driver->Create(file_path.c_str(), 0, 0, 0, GDT_Unknown, nullptr));
	if (!dataset) {
		throw IOException("Could not open dataset");
	}

	auto layer = dataset->CreateLayer("layer", nullptr, wkbUnknown, nullptr);
	if (!layer) {
		throw IOException("Could not create layer");
	}

	// Create the layer field definitions
	vector<unique_ptr<OGRFieldDefn>> field_defs;
	for (idx_t i = 0; i < gdal_data.field_names.size(); i++) {
		auto &name = gdal_data.field_names[i];
		auto &type = gdal_data.field_sql_types[i];

		if(IsGeometryType(type)) { /*
			auto field = OGRGeometryFieldTypeFromLogicalType(name, type);
			if (layer->CreateGeomField(field.get()) != OGRERR_NONE) {
				throw IOException("Could not create geometry field");
			}
			// TODO: Do we need to keep the field around?
			*/
		   // Not all drivers support "creating" geometry fields
			// so skip it and use the built in geometry field instead for now.
		} else {
			auto field = OGRFieldTypeFromLogicalType(name, type);
			if (layer->CreateField(field.get()) != OGRERR_NONE) {
				throw IOException("Could not create attribute field");
			}
			// TODO: ^ Like we do here vvv
			field_defs.push_back(std::move(field));
		}
	}
	auto global_data = make_unique<GlobalState>(std::move(dataset), layer, std::move(field_defs));

	return std::move(global_data);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

static OGRGeometryUniquePtr OGRGeometryFromLogicalType(const LogicalType &type, const Value &value) {
	if(type == core::GeoTypes::WKB_BLOB) {
		auto str = value.GetValueUnsafe<string_t>();

		OGRGeometry *ptr;
		size_t consumed;
		auto ok = OGRGeometryFactory::createFromWkb(
		    str.GetDataUnsafe(), nullptr, &ptr, str.GetSize(), wkbVariantIso, consumed);

		if(ok != OGRERR_NONE) {
			throw IOException("Could not parse WKB");
		}
		return OGRGeometryUniquePtr(ptr);
	}
	if(type == core::GeoTypes::POINT_2D) {
		auto children = StructValue::GetChildren(value);
		auto x = children[0].GetValue<double>();
		auto y = children[1].GetValue<double>();
		auto ogr_point = new OGRPoint(x, y);
		return OGRGeometryUniquePtr(ogr_point);
	} else {
		throw NotImplementedException("Unsupported geometry type");
	}
}

static void SetOgrFieldFromValue(OGRFeature *feature, const char* name, const LogicalType &type, const Value &value) {
	// TODO: Set field by index always instead of by name for performance.
	if (value.IsNull()) {
		auto idx = feature->GetFieldIndex(name);
		feature->SetFieldNull(idx);
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		feature->SetField(name, value.GetValue<bool>());
		break;
	case LogicalTypeId::TINYINT:
		feature->SetField(name, value.GetValue<int8_t>());
		break;
	case LogicalTypeId::SMALLINT:
		feature->SetField(name, value.GetValue<int16_t>());
		break;
	case LogicalTypeId::INTEGER:
		feature->SetField(name, value.GetValue<int32_t>());
		break;
	case LogicalTypeId::BIGINT:
		feature->SetField(name, value.GetValue<int64_t>());
		break;
	case LogicalTypeId::FLOAT:
		feature->SetField(name, value.GetValue<float>());
		break;
	case LogicalTypeId::DOUBLE:
		feature->SetField(name, value.GetValue<double>());
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB: {
		auto str = value.GetValue<string>();
		feature->SetField(name, str.c_str());
	} break;
	default:
		// TODO: Add time types
		throw NotImplementedException("Unsupported type");
	}
}

static void Sink(ExecutionContext &context, FunctionData &bdata, GlobalFunctionData &gstate, LocalFunctionData &lstate, DataChunk &input) {

	auto &bind_data = (BindData &)bdata;
	auto &global_state = (GlobalState &)gstate;
	auto &local_state = (LocalState &)lstate;

	lock_guard<mutex> d_lock(global_state.lock);
	auto layer = global_state.layer;

	// Create the feature
	input.Flatten();
	for(idx_t row_idx = 0; row_idx < input.size(); row_idx++) {

		auto feature = OGRFeatureUniquePtr(OGRFeature::CreateFeature(layer->GetLayerDefn()));

		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto &name = bind_data.field_names[col_idx];
			auto &type = bind_data.field_sql_types[col_idx];
			auto value = input.GetValue(col_idx, row_idx);

			if(IsGeometryType(type)) {
				// TODO: check how many geometry fields there are and use the correct one.
				auto geom = OGRGeometryFromLogicalType(type, value);
				if (feature->SetGeometry(geom.get()) != OGRERR_NONE) {
					throw IOException("Could not set geometry");
				}
			} else {
				SetOgrFieldFromValue(feature.get(), name.c_str(), type, value);
			}
		}
		if (layer->CreateFeature(feature.get()) != OGRERR_NONE) {
			throw IOException("Could not create feature");
		}
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {

}

//===--------------------------------------------------------------------===//
// Parallel
//===--------------------------------------------------------------------===//
bool IsParallel(ClientContext &context, FunctionData &bind_data) {
	return false; // always in order?
}

void GdalCopyFunction::Register(ClientContext &context) {
	// register the copy function
	CopyFunction info("GDAL");
	info.copy_to_bind = Bind;
	info.copy_to_initialize_local = InitLocal;
	info.copy_to_initialize_global = InitGlobal;
	info.copy_to_sink = Sink;
	//info.copy_to_combine = WriteCSVCombine;
	info.copy_to_finalize = Finalize;
	info.parallel = IsParallel;

	//info.copy_from_bind = ReadCSVBind;
	//info.copy_from_function = ReadCSVTableFunction::GetFunction();

	//info.extension = "csv";

	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateCopyFunctionInfo create(std::move(info));
	create.internal = true;
	catalog.CreateCopyFunction(context, &create);
}


} // namespace gdal

} // namespace geo