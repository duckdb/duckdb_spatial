#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Single Part Geometry
//------------------------------------------------------------------------------
void SinglePartGeometry::Reference(const SinglePartGeometry &other, uint32_t offset, uint32_t count) {
    properties = other.properties;
    data.vertex_data = other.data.vertex_data + offset * properties.VertexSize();
    data_count = count;
    is_readonly = true;
}

void SinglePartGeometry::ReferenceData(const_data_ptr_t data_ptr, uint32_t count, bool has_z, bool has_m) {
    properties.SetZ(has_z);
    properties.SetM(has_m);
    data.vertex_data = const_cast<data_ptr_t>(data_ptr);
    data_count = count;
    is_readonly = true;
}

void SinglePartGeometry::Copy(ArenaAllocator& alloc, const SinglePartGeometry &other, uint32_t offset, uint32_t count) {
    properties = other.properties;
    data_count = count;
    data.vertex_data = alloc.AllocateAligned(properties.VertexSize() * count);
    memcpy(data.vertex_data, other.data.vertex_data + offset * properties.VertexSize(), properties.VertexSize() * count);
    is_readonly = false;
}

void SinglePartGeometry::CopyData(ArenaAllocator& alloc, const_data_ptr_t data_ptr, uint32_t count, bool has_z, bool has_m) {
    properties.SetZ(has_z);
    properties.SetM(has_m);
    data_count = count;
    data.vertex_data = alloc.AllocateAligned(properties.VertexSize() * count);
    memcpy(data.vertex_data, data_ptr, properties.VertexSize() * count);
    is_readonly = false;
}

void SinglePartGeometry::Resize(ArenaAllocator &alloc, uint32_t new_count) {
    auto vertex_size = properties.VertexSize();
    if (new_count == data_count) {
        return;
    }
    if (data.vertex_data == nullptr) {
        data.vertex_data = alloc.AllocateAligned(vertex_size * new_count);
        data_count = new_count;
        is_readonly = false;
        memset(data.vertex_data, 0, vertex_size * new_count);
        return;
    }

    if (!IsReadOnly()) {
        data.vertex_data = alloc.Reallocate(data.vertex_data, data_count * vertex_size, vertex_size * new_count);
        data_count = new_count;
    } else {
        auto new_data = alloc.AllocateAligned(vertex_size * new_count);
        memset(new_data, 0, vertex_size * new_count);
        auto copy_count = std::min(data_count, new_count);
        memcpy(new_data, data.vertex_data, vertex_size * copy_count);
        data.vertex_data = new_data;
        data_count = new_count;
        is_readonly = false;
    }
}

void SinglePartGeometry::Append(ArenaAllocator &alloc, const SinglePartGeometry& other) {
    Append(alloc, &other, 1);
}

void SinglePartGeometry::Append(ArenaAllocator &alloc, const SinglePartGeometry* others, uint32_t others_count) {
    if (IsReadOnly()) {
        MakeMutable(alloc);
    }

    auto old_count = data_count;
    auto new_count = old_count;
    for (uint32_t i = 0; i < others_count; i++) {
        new_count += others[i].Count();
        D_ASSERT(properties.HasZ() == others[i].properties.HasZ());
        D_ASSERT(properties.HasM() == others[i].properties.HasM());
    }
    Resize(alloc, new_count);

    auto vertex_size = properties.VertexSize();
    for(uint32_t i = 0; i < others_count; i++) {
        auto other = others[i];
        memcpy(data.vertex_data + old_count * vertex_size, other.data.vertex_data, vertex_size * other.data_count);
        old_count += other.data_count;
    }
    data_count = new_count;
}

void SinglePartGeometry::SetVertexType(ArenaAllocator &alloc, bool has_z, bool has_m, double default_z, double default_m) {
    if (properties.HasZ() == has_z && properties.HasM() == has_m) {
        return;
    }
    if (IsReadOnly()) {
        MakeMutable(alloc);
    }

    auto used_to_have_z = properties.HasZ();
    auto used_to_have_m = properties.HasM();
    auto old_vertex_size = properties.VertexSize();

    properties.SetZ(has_z);
    properties.SetM(has_m);

    auto new_vertex_size = properties.VertexSize();
    // Case 1: The new vertex size is larger than the old vertex size
    if (new_vertex_size > old_vertex_size) {
        data.vertex_data =
                alloc.ReallocateAligned(data.vertex_data, data_count * old_vertex_size, data_count * new_vertex_size);

        // There are 5 cases here:
        if (used_to_have_m && has_m && !used_to_have_z && has_z) {
            // 1. We go from XYM to XYZM
            // This is special, because we need to slide the M value to the end of each vertex
            for (int64_t i = data_count - 1; i >= 0; i--) {
                auto old_offset = i * old_vertex_size;
                auto new_offset = i * new_vertex_size;
                auto old_m_offset = old_offset + sizeof(double) * 2;
                auto new_z_offset = new_offset + sizeof(double) * 2;
                auto new_m_offset = new_offset + sizeof(double) * 3;
                // Move the M value
                memcpy(data.vertex_data + new_m_offset, data.vertex_data + old_m_offset, sizeof(double));
                // Set the new Z value
                memcpy(data.vertex_data + new_z_offset, &default_z, sizeof(double));
                // Move the X and Y values
                memcpy(data.vertex_data + new_offset, data.vertex_data + old_offset, sizeof(double) * 2);
            }
        } else if (!used_to_have_z && has_z && !used_to_have_m && has_m) {
            // 2. We go from XY to XYZM
            // This is special, because we need to add both the default Z and M values to the end of each vertex
            for (int64_t i = data_count - 1; i >= 0; i--) {
                auto old_offset = i * old_vertex_size;
                auto new_offset = i * new_vertex_size;
                memcpy(data.vertex_data + new_offset, data.vertex_data + old_offset, sizeof(double) * 2);
                memcpy(data.vertex_data + new_offset + sizeof(double) * 2, &default_z, sizeof(double));
                memcpy(data.vertex_data + new_offset + sizeof(double) * 3, &default_m, sizeof(double));
            }
        } else {
            // Otherwise:
            // 3. We go from XY to XYZ
            // 4. We go from XY to XYM
            // 5. We go from XYZ to XYZM
            // These are all really the same, we just add the default to the end
            auto default_value = has_m ? default_m : default_z;
            for (int64_t i = data_count - 1; i >= 0; i--) {
                auto old_offset = i * old_vertex_size;
                auto new_offset = i * new_vertex_size;
                memmove(data.vertex_data + new_offset, data.vertex_data + old_offset, old_vertex_size);
                memcpy(data.vertex_data + new_offset + old_vertex_size, &default_value, sizeof(double));
            }
        }
    }
        // Case 2: The new vertex size is equal to the old vertex size
    else if (new_vertex_size == old_vertex_size) {
        // This only happens when we go from XYZ -> XYM or XYM -> XYZ
        // In this case we just need to set the default on the third dimension
        auto default_value = has_m ? default_m : default_z;
        for (uint32_t i = 0; i < data_count; i++) {
            auto offset = i * new_vertex_size + sizeof(double) * 2;
            memcpy(data.vertex_data + offset, &default_value, sizeof(double));
        }
    }
        // Case 3: The new vertex size is smaller than the old vertex size.
        // In this case we need to allocate new memory and copy the data over to not lose any data
    else {
        auto new_data = alloc.AllocateAligned(data_count * new_vertex_size);
        memset(new_data, 0, data_count * new_vertex_size);

        // Special case: If we go from XYZM to XYM, we need to slide the M value to the end of each vertex
        if (used_to_have_z && used_to_have_m && !has_z && has_m) {
            for (uint32_t i = 0; i < data_count; i++) {
                auto old_offset = i * old_vertex_size;
                auto new_offset = i * new_vertex_size;
                memcpy(new_data + new_offset, data.vertex_data + old_offset, sizeof(double) * 2);
                auto m_offset = old_offset + sizeof(double) * 3;
                memcpy(new_data + new_offset + sizeof(double) * 2, data.vertex_data + m_offset, sizeof(double));
            }
        } else {
            // Otherwise, we just copy the data over
            for (uint32_t i = 0; i < data_count; i++) {
                auto old_offset = i * old_vertex_size;
                auto new_offset = i * new_vertex_size;
                memcpy(new_data + new_offset, data.vertex_data + old_offset, new_vertex_size);
            }
        }
        data.vertex_data = new_data;
    }
}

void SinglePartGeometry::MakeMutable(ArenaAllocator &alloc) {
    if (!IsReadOnly()) {
        return;
    }
    if (data_count == 0) {
        data.vertex_data = nullptr;
        is_readonly = false;
        return;
    }

    auto new_data = alloc.AllocateAligned(ByteSize());
    memcpy(new_data, data.vertex_data, ByteSize());
    data.vertex_data = new_data;
    is_readonly = false;
}


bool SinglePartGeometry::IsClosed() const {
    switch(Count()) {
        case 0: return false;
        case 1: return true;
        default:
            auto first = Get(0);
            auto last = Get(Count() - 1);
            // TODO: Approximate comparison?
            return first.x == last.x && first.y == last.y;
    }
}

double SinglePartGeometry::Length() const {
    double length = 0;
    for (uint32_t i = 1; i < Count(); i++) {
        auto p1 = Get(i - 1);
        auto p2 = Get(i);
        length += sqrt((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y));
    }
    return length;
}


string SinglePartGeometry::ToString(uint32_t start, uint32_t count) const {
    auto has_z = properties.HasZ();
    auto has_m = properties.HasM();

    D_ASSERT(type == GeometryType::POINT || type == GeometryType::LINESTRING);
    auto type_name = type == GeometryType::POINT ? "POINT" : "LINESTRING";

    if (has_z && has_m) {
        string result = StringUtil::Format("%s XYZM ([%d-%d]/%d) [", type_name, start, start + count, data_count);
        for (uint32_t i = start; i < count; i++) {
            auto vertex = GetExact<VertexXYZM>(i);
            result += StringUtil::Format("(%f, %f, %f, %f)", vertex.x, vertex.y, vertex.z, vertex.m);
            if (i < count - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    } else if (has_z) {
        string result = StringUtil::Format("%s XYZ ([%d-%d]/%d) [", type_name, start, start + count, data_count);
        for (uint32_t i = start; i < count; i++) {
            auto vertex = GetExact<VertexXYZ>(i);
            result += StringUtil::Format("(%f, %f, %f)", vertex.x, vertex.y, vertex.z);
            if (i < count - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    } else if (has_m) {
        string result = StringUtil::Format("%s XYM ([%d-%d]/%d) [", type_name, start, start + count, data_count);
        for (uint32_t i = start; i < count; i++) {
            auto vertex = GetExact<VertexXYM>(i);
            result += StringUtil::Format("(%f, %f, %f)", vertex.x, vertex.y, vertex.m);
            if (i < count - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    } else {
        string result = StringUtil::Format("%s XY ([%d-%d]/%d) [", type_name, start, start + count, data_count);
        for (uint32_t i = start; i < count; i++) {
            auto vertex = GetExact<VertexXY>(i);
            result += StringUtil::Format("(%f, %f)", vertex.x, vertex.y);
            if (i < count - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }
}

//------------------------------------------------------------------------------
// Multi Part Geometry
//------------------------------------------------------------------------------

void MultiPartGeometry::Resize(ArenaAllocator &alloc, uint32_t new_count) {
    if (new_count == data_count) {
        return;
    }
    if (data.part_data == nullptr) {
        data.part_data = reinterpret_cast<Geometry*>(alloc.AllocateAligned(sizeof(Geometry) * new_count));
    } else {
        data.part_data = reinterpret_cast<Geometry *>(alloc.ReallocateAligned(data_ptr_cast(data.part_data),
                                                                              data_count * sizeof(Geometry),
                                                                              new_count * sizeof(Geometry)));
    }
    data_count = new_count;
}


/*
string Point::ToString() const {
	if (IsEmpty()) {
		return "POINT EMPTY";
	}
	auto vert = vertices.Get(0);
	if (std::isnan(vert.x) && std::isnan(vert.y)) {
		// This is a special case for WKB. WKB does not support empty points,
		// and instead writes a point with NaN coordinates. We therefore need to
		// check for this case and return POINT EMPTY instead to round-trip safely
		return "POINT EMPTY";
	}
	return StringUtil::Format("POINT (%s)", Utils::format_coord(vert.x, vert.y));
}

string LineString::ToString() const {
	auto count = vertices.Count();
	if (count == 0) {
		return "LINESTRING EMPTY";
	}

	string result = "LINESTRING (";
	for (uint32_t i = 0; i < vertices.Count(); i++) {
		auto x = vertices.Get(i).x;
		auto y = vertices.Get(i).y;
		result += Utils::format_coord(x, y);
		if (i < vertices.Count() - 1) {
			result += ", ";
		}
	}
	result += ")";
	return result;
}

string Polygon::ToString() const {

	// check if the polygon is empty
	uint32_t total_verts = 0;
	auto num_rings = ring_count;
	for (uint32_t i = 0; i < num_rings; i++) {
		total_verts += rings[i].Count();
	}
	if (total_verts == 0) {
		return "POLYGON EMPTY";
	}

	string result = "POLYGON (";
	for (uint32_t i = 0; i < num_rings; i++) {
		result += "(";
		for (uint32_t j = 0; j < rings[i].Count(); j++) {
			auto x = rings[i].Get(j).x;
			auto y = rings[i].Get(j).y;
			result += Utils::format_coord(x, y);
			if (j < rings[i].Count() - 1) {
				result += ", ";
			}
		}
		result += ")";
		if (i < num_rings - 1) {
			result += ", ";
		}
	}
	result += ")";
	return result;
}

string MultiPoint::ToString() const {
	auto num_points = ItemCount();
	if (num_points == 0) {
		return "MULTIPOINT EMPTY";
	}
	string str = "MULTIPOINT (";
	auto &points = *this;
	for (uint32_t i = 0; i < num_points; i++) {
		auto &point = points[i];
		if (point.IsEmpty()) {
			str += "EMPTY";
		} else {
			auto vert = point.Vertices().Get(0);
			str += Utils::format_coord(vert.x, vert.y);
		}
		if (i < num_points - 1) {
			str += ", ";
		}
	}
	return str + ")";
}


string MultiLineString::ToString() const {
	auto count = ItemCount();
	if (count == 0) {
		return "MULTILINESTRING EMPTY";
	}
	string str = "MULTILINESTRING (";

	bool first_line = true;
	for (auto &line : *this) {
		if (first_line) {
			first_line = false;
		} else {
			str += ", ";
		}
		str += "(";
		bool first_vert = true;
		for (uint32_t i = 0; i < line.Vertices().Count(); i++) {
			auto vert = line.Vertices().Get(i);
			if (first_vert) {
				first_vert = false;
			} else {
				str += ", ";
			}
			str += Utils::format_coord(vert.x, vert.y);
		}
		str += ")";
	}
	return str + ")";
}

string MultiPolygon::ToString() const {
	auto count = ItemCount();
	if (count == 0) {
		return "MULTIPOLYGON EMPTY";
	}
	string str = "MULTIPOLYGON (";

	bool first_poly = true;
	for (auto &poly : *this) {
		if (first_poly) {
			first_poly = false;
		} else {
			str += ", ";
		}
		str += "(";
		bool first_ring = true;
		for (auto &ring : poly) {
			if (first_ring) {
				first_ring = false;
			} else {
				str += ", ";
			}
			str += "(";
			bool first_vert = true;
			for (uint32_t v = 0; v < ring.Count(); v++) {
				auto vert = ring.Get(v);
				if (first_vert) {
					first_vert = false;
				} else {
					str += ", ";
				}
				str += Utils::format_coord(vert.x, vert.y);
			}
			str += ")";
		}
		str += ")";
	}

	return str + ")";
}

string GeometryCollection::ToString() const {
	auto count = ItemCount();
	if (count == 0) {
		return "GEOMETRYCOLLECTION EMPTY";
	}
	string str = "GEOMETRYCOLLECTION (";
	for (uint32_t i = 0; i < count; i++) {
		str += (*this)[i].ToString();
		if (i < count - 1) {
			str += ", ";
		}
	}
	return str + ")";
}
*/

//------------------------------------------------------------------------------
// Util
//------------------------------------------------------------------------------
// We've got this exposed upstream, we just need to wait for the next release
extern "C" int geos_d2sfixed_buffered_n(double f, uint32_t precision, char *result);

string Utils::format_coord(double d) {
    char buf[25];
    auto len = geos_d2sfixed_buffered_n(d, 15, buf);
    buf[len] = '\0';
    return string {buf};
}

string Utils::format_coord(double x, double y) {
    char buf[51];
    auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
    buf[res_x++] = ' ';
    auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
    buf[res_x + res_y] = '\0';
    return string {buf};
}

string Utils::format_coord(double x, double y, double zm) {
    char buf[76];
    auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
    buf[res_x++] = ' ';
    auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
    buf[res_x + res_y++] = ' ';
    auto res_zm = geos_d2sfixed_buffered_n(zm, 15, buf + res_x + res_y);
    buf[res_x + res_y + res_zm] = '\0';
    return string {buf};
}

string Utils::format_coord(double x, double y, double z, double m) {
    char buf[101];
    auto res_x = geos_d2sfixed_buffered_n(x, 15, buf);
    buf[res_x++] = ' ';
    auto res_y = geos_d2sfixed_buffered_n(y, 15, buf + res_x);
    buf[res_x + res_y++] = ' ';
    auto res_z = geos_d2sfixed_buffered_n(z, 15, buf + res_x + res_y);
    buf[res_x + res_y + res_z++] = ' ';
    auto res_m = geos_d2sfixed_buffered_n(m, 15, buf + res_x + res_y + res_z);
    buf[res_x + res_y + res_z + res_m] = '\0';
    return string {buf};
}

} // namespace core

} // namespace spatial
