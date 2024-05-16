#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"
#include "spatial/core/geometry/wkt_reader.hpp"
#include "spatial/core/util/math.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/error_data.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Point2DToVarchar(Vector &source, Vector &result, idx_t count) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;

	GenericExecutor::ExecuteUnary<POINT_TYPE, VARCHAR_TYPE>(source, result, count, [&](POINT_TYPE &point) {
		auto x = point.a_val;
		auto y = point.b_val;

		if (std::isnan(x) || std::isnan(y)) {
			return StringVector::AddString(result, "POINT EMPTY");
		}

		return StringVector::AddString(result, StringUtil::Format("POINT (%s)", MathUtil::format_coord(x, y)));
	});
}

//------------------------------------------------------------------------------
// LINESTRING_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::LineString2DToVarchar(Vector &source, Vector &result, idx_t count) {
	auto &inner = ListVector::GetEntry(source);
	auto &children = StructVector::GetEntries(inner);
	auto x_data = FlatVector::GetData<double>(*children[0]);
	auto y_data = FlatVector::GetData<double>(*children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &line) {
		auto offset = line.offset;
		auto length = line.length;

		if (length == 0) {
			return StringVector::AddString(result, "LINESTRING EMPTY");
		}

		string result_str = "LINESTRING (";
		for (idx_t i = offset; i < offset + length; i++) {
			result_str += MathUtil::format_coord(x_data[i], y_data[i]);
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// POLYGON_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Polygon2DToVarchar(Vector &source, Vector &result, idx_t count) {
	auto &poly_vector = source;
	auto &ring_vector = ListVector::GetEntry(poly_vector);
	auto ring_entries = ListVector::GetData(ring_vector);
	auto &point_vector = ListVector::GetEntry(ring_vector);
	auto &point_children = StructVector::GetEntries(point_vector);
	auto x_data = FlatVector::GetData<double>(*point_children[0]);
	auto y_data = FlatVector::GetData<double>(*point_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(poly_vector, result, count, [&](list_entry_t polygon_entry) {
		auto offset = polygon_entry.offset;
		auto length = polygon_entry.length;

		if (length == 0) {
			return StringVector::AddString(result, "POLYGON EMPTY");
		}

		string result_str = "POLYGON (";
		for (idx_t i = offset; i < offset + length; i++) {
			auto ring_entry = ring_entries[i];
			auto ring_offset = ring_entry.offset;
			auto ring_length = ring_entry.length;
			result_str += "(";
			for (idx_t j = ring_offset; j < ring_offset + ring_length; j++) {
				result_str += MathUtil::format_coord(x_data[j], y_data[j]);
				if (j < ring_offset + ring_length - 1) {
					result_str += ", ";
				}
			}
			result_str += ")";
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// BOX_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Box2DToVarchar(Vector &source, Vector &result, idx_t count) {
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;
	GenericExecutor::ExecuteUnary<BOX_TYPE, VARCHAR_TYPE>(source, result, count, [&](BOX_TYPE &box) {
		return StringVector::AddString(result,
		                               StringUtil::Format("BOX(%s, %s)", MathUtil::format_coord(box.a_val, box.b_val),
		                                                  MathUtil::format_coord(box.c_val, box.d_val)));
	});
}

//------------------------------------------------------------------------------
// GEOMETRY -> VARCHAR
//------------------------------------------------------------------------------
class GeometryTextProcessor final : GeometryProcessor<void, bool> {
private:
	string text;

public:
	void OnVertexData(const VertexData &data) {
		auto &dims = data.data;
		auto &strides = data.stride;
		auto count = data.count;

		if (HasZ() && HasM()) {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				auto z = Load<double>(dims[2] + i * strides[2]);
				auto m = Load<double>(dims[3] + i * strides[3]);
				text += MathUtil::format_coord(x, y, z, m);
				if (i < count - 1) {
					text += ", ";
				}
			}
		} else if (HasZ()) {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				auto zm = Load<double>(dims[2] + i * strides[2]);
				text += MathUtil::format_coord(x, y, zm);
				if (i < count - 1) {
					text += ", ";
				}
			}
		} else if (HasM()) {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				auto m = Load<double>(dims[3] + i * strides[3]);
				text += MathUtil::format_coord(x, y, m);
				if (i < count - 1) {
					text += ", ";
				}
			}
		} else {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				text += MathUtil::format_coord(x, y);

				if (i < count - 1) {
					text += ", ";
				}
			}
		}
	}

	void ProcessPoint(const VertexData &data, bool in_typed_collection) override {
		if (!in_typed_collection) {
			text += "POINT";
			if (HasZ() && HasM()) {
				text += " ZM";
			} else if (HasZ()) {
				text += " Z";
			} else if (HasM()) {
				text += " M";
			}
			text += " ";
		}

		if (data.count == 0) {
			text += "EMPTY";
		} else if (in_typed_collection) {
			OnVertexData(data);
		} else {
			text += "(";
			OnVertexData(data);
			text += ")";
		}
	}

	void ProcessLineString(const VertexData &data, bool in_typed_collection) override {
		if (!in_typed_collection) {
			text += "LINESTRING";
			if (HasZ() && HasM()) {
				text += " ZM";
			} else if (HasZ()) {
				text += " Z";
			} else if (HasM()) {
				text += " M";
			}
			text += " ";
		}

		if (data.count == 0) {
			text += "EMPTY";
		} else {
			text += "(";
			OnVertexData(data);
			text += ")";
		}
	}

	void ProcessPolygon(PolygonState &state, bool in_typed_collection) override {
		if (!in_typed_collection) {
			text += "POLYGON";
			if (HasZ() && HasM()) {
				text += " ZM";
			} else if (HasZ()) {
				text += " Z";
			} else if (HasM()) {
				text += " M";
			}
			text += " ";
		}

		if (state.RingCount() == 0) {
			text += "EMPTY";
		} else {
			text += "(";
			bool first = true;
			while (!state.IsDone()) {
				if (!first) {
					text += ", ";
				}
				first = false;
				text += "(";
				auto vertices = state.Next();
				OnVertexData(vertices);
				text += ")";
			}
			text += ")";
		}
	}

	void ProcessCollection(CollectionState &state, bool) override {
		bool collection_is_typed = false;
		switch (CurrentType()) {
		case GeometryType::MULTIPOINT:
			text += "MULTIPOINT";
			collection_is_typed = true;
			break;
		case GeometryType::MULTILINESTRING:
			text += "MULTILINESTRING";
			collection_is_typed = true;
			break;
		case GeometryType::MULTIPOLYGON:
			text += "MULTIPOLYGON";
			collection_is_typed = true;
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			text += "GEOMETRYCOLLECTION";
			collection_is_typed = false;
			break;
		default:
			throw InvalidInputException("Invalid geometry type");
		}

		if (HasZ() && HasM()) {
			text += " ZM";
		} else if (HasZ()) {
			text += " Z";
		} else if (HasM()) {
			text += " M";
		}

		if (state.ItemCount() == 0) {
			text += " EMPTY";
		} else {
			text += " (";
			bool first = true;
			while (!state.IsDone()) {
				if (!first) {
					text += ", ";
				}
				first = false;
				state.Next(collection_is_typed);
			}
			text += ")";
		}
	}

	const string &Execute(const geometry_t &geom) {
		text.clear();
		Process(geom, false);
		return text;
	}
};

void CoreVectorOperations::GeometryToVarchar(Vector &source, Vector &result, idx_t count) {
	GeometryTextProcessor processor;
	UnaryExecutor::Execute<geometry_t, string_t>(source, result, count, [&](geometry_t &input) {
		auto text = processor.Execute(input);
		return StringVector::AddString(result, text);
	});
}

static bool TextToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	WKTReader reader(lstate.arena);

	bool success = true;
	UnaryExecutor::ExecuteWithNulls<string_t, geometry_t>(
	    source, result, count, [&](string_t &wkt, ValidityMask &mask, idx_t idx) {
		    try {
			    auto geom = reader.Parse(wkt);
			    return Geometry::Serialize(geom, result);
		    } catch (InvalidInputException &e) {
			    if (success) {
				    success = false;
				    ErrorData error(e);
				    HandleCastError::AssignError(error.RawMessage(), parameters.error_message);
			    }
			    mask.SetInvalid(idx);
			    return geometry_t {};
		    }
	    });
	return success;
}

//------------------------------------------------------------------------------
// CASTS
//------------------------------------------------------------------------------
static bool Point2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::Point2DToVarchar(source, result, count);
	return true;
}

static bool LineString2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::LineString2DToVarchar(source, result, count);
	return true;
}

static bool Polygon2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::Polygon2DToVarchar(source, result, count);
	return true;
}

static bool Box2DToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::Box2DToVarchar(source, result, count);
	return true;
}

static bool GeometryToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	CoreVectorOperations::GeometryToVarchar(source, result, count);
	return true;
}

void CoreCastFunctions::RegisterVarcharCasts(DatabaseInstance &db) {

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::POINT_2D(), LogicalType::VARCHAR,
	                                    BoundCastInfo(Point2DToVarcharCast), 1);

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::LINESTRING_2D(), LogicalType::VARCHAR,
	                                    BoundCastInfo(LineString2DToVarcharCast), 1);

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::POLYGON_2D(), LogicalType::VARCHAR,
	                                    BoundCastInfo(Polygon2DToVarcharCast), 1);

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::BOX_2D(), LogicalType::VARCHAR, BoundCastInfo(Box2DToVarcharCast),
	                                    1);

	ExtensionUtil::RegisterCastFunction(db, GeoTypes::GEOMETRY(), LogicalType::VARCHAR,
	                                    BoundCastInfo(GeometryToVarcharCast), 1);

	ExtensionUtil::RegisterCastFunction(
	    db, LogicalType::VARCHAR, core::GeoTypes::GEOMETRY(),
	    BoundCastInfo(TextToGeometryCast, nullptr, GeometryFunctionLocalState::InitCast));
}

} // namespace core

} // namespace spatial
