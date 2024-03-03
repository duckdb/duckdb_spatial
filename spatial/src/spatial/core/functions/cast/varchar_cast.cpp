#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/cast.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"


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

		return StringVector::AddString(result, StringUtil::Format("POINT (%s)", Utils::format_coord(x, y)));
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
			result_str += Utils::format_coord(x_data[i], y_data[i]);
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
				result_str += Utils::format_coord(x_data[j], y_data[j]);
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
		                               StringUtil::Format("BOX(%s, %s)", Utils::format_coord(box.a_val, box.b_val),
		                                                  Utils::format_coord(box.c_val, box.d_val)));
	});
}

//------------------------------------------------------------------------------
// GEOMETRY -> VARCHAR
//------------------------------------------------------------------------------
class GeometryTextProcessor final : public GeometryProcessor {
    string text;
public:
    const string& GetText() {
        return text;
    }

    void OnBegin() override {
        text.clear();
    }

    void OnVertexData(const_data_ptr_t dims[4], ptrdiff_t strides[4], uint32_t count) override {
        if(HasZ() && HasM()) {
            for(uint32_t i = 0; i < count; i++) {
                auto x = Load<double>(dims[0] + i * strides[0]);
                auto y = Load<double>(dims[1] + i * strides[1]);
                auto z = Load<double>(dims[2] + i * strides[2]);
                auto m = Load<double>(dims[3] + i * strides[3]);
                text += Utils::format_coord(x, y, z, m);
                if(i < count - 1) {
                    text += ", ";
                }
            }
        } else if(HasZ() || HasM()) {
            for(uint32_t i = 0; i < count; i++) {
                auto x = Load<double>(dims[0] + i * strides[0]);
                auto y = Load<double>(dims[1] + i * strides[1]);
                auto zm = Load<double>(dims[2] + i * strides[2]);
                text += Utils::format_coord(x, y, zm);
                if(i < count - 1) {
                    text += ", ";
                }
            }
        } else {
            for(uint32_t i = 0; i < count; i++) {
                auto x = Load<double>(dims[0] + i * strides[0]);
                auto y = Load<double>(dims[1] + i * strides[1]);
                text += Utils::format_coord(x, y);

                if(i < count - 1) {
                    text += ", ";
                }
            }
        }
    }

    void OnPointBegin(bool is_empty) override {
        if(ParentType() != GeometryType::MULTIPOINT) {
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

        if(is_empty) {
            text += "EMPTY";
        } else if(ParentType() != GeometryType::MULTIPOINT) {
            text += "(";
        }
    }
    void OnPointEnd(bool is_empty) override {
        if (!is_empty && ParentType() != GeometryType::MULTIPOINT) {
            text += ")";
        }
    }

    void OnLineStringBegin(uint32_t count) override {
        if(ParentType() != GeometryType::MULTILINESTRING) {
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
        if (count == 0) {
            text += "EMPTY";
        } else {
            text += "(";
        }
    }

    void OnLineStringEnd(uint32_t count) override {
        if (count != 0) {
            text += ")";
        }
    }

    void OnPolygonBegin(uint32_t count) override {
        if(ParentType() != GeometryType::MULTIPOLYGON) {
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
        if (count == 0) {
            text += "EMPTY";
        } else {
            text += "(";
        }
    }

    void OnPolygonEnd(uint32_t count) override {
        if (count != 0) {
            text += ")";
        }
    }

    void OnPolygonRingBegin(uint32_t ring_idx, bool is_last) override {
        text += "(";
    }

    void OnPolygonRingEnd(uint32_t ring_idx, bool is_last) override {
        text += ")";
        if(!is_last) {
            text += ", ";
        }
    }

    void OnCollectionBegin(uint32_t count) override {
        switch(CurrentType()) {
            case GeometryType::MULTIPOINT:
                text += "MULTIPOINT";
                break;
            case GeometryType::MULTILINESTRING:
                text += "MULTILINESTRING";
                break;
            case GeometryType::MULTIPOLYGON:
                text += "MULTIPOLYGON";
                break;
            case GeometryType::GEOMETRYCOLLECTION:
                text += "GEOMETRYCOLLECTION";
                break;
            default:
                throw InvalidInputException("Invalid geometry type");
        }

        if(HasZ() && HasM()) {
            text += " ZM";
        } else if(HasZ()) {
            text += " Z";
        } else if(HasM()) {
            text += " M";
        }

        if (count == 0) {
            text += " EMPTY";
        } else {
            text += " (";
        }
    }

    void OnCollectionItemEnd(uint32_t item_idx, bool is_last) override {
        if(!is_last) {
            text += ", ";
        }
    }

    void OnCollectionEnd(uint32_t count) override {
        if (count != 0) {
            text += ")";
        }
    }
};

void CoreVectorOperations::GeometryToVarchar(Vector &source, Vector &result, idx_t count, GeometryFactory &factory) {
    GeometryTextProcessor processor;
	UnaryExecutor::Execute<geometry_t, string_t>(source, result, count, [&](geometry_t &input) {
        processor.Execute(input);
		return StringVector::AddString(result, processor.GetText());
	});
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
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(parameters);
	CoreVectorOperations::GeometryToVarchar(source, result, count, lstate.factory);
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

	ExtensionUtil::RegisterCastFunction(
	    db, GeoTypes::GEOMETRY(), LogicalType::VARCHAR,
	    BoundCastInfo(GeometryToVarcharCast, nullptr, GeometryFunctionLocalState::InitCast), 1);
}

} // namespace core

} // namespace spatial
