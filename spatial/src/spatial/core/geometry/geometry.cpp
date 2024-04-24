#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/util/math.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Single Part Geometry
//------------------------------------------------------------------------------
void SinglePartGeometry::Resize(Geometry &geom, ArenaAllocator &alloc, uint32_t new_count) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.type));

	auto vertex_size = geom.properties.VertexSize();
	if (new_count == geom.data_count) {
		return;
	}
	if (geom.data_ptr == nullptr) {
		geom.data_ptr = alloc.AllocateAligned(vertex_size * new_count);
		geom.data_count = new_count;
		geom.is_readonly = false;
		memset(geom.data_ptr, 0, vertex_size * new_count);
		return;
	}

	if (!geom.is_readonly) {
		geom.data_ptr = alloc.ReallocateAligned(geom.data_ptr, geom.data_count * vertex_size, vertex_size * new_count);
		geom.data_count = new_count;
	} else {
		auto new_data = alloc.AllocateAligned(vertex_size * new_count);
		memset(new_data, 0, vertex_size * new_count);
		auto copy_count = std::min(geom.data_count, new_count);
		memcpy(new_data, geom.data_ptr, vertex_size * copy_count);
		geom.data_ptr = new_data;
		geom.data_count = new_count;
		geom.is_readonly = false;
	}
}

void SinglePartGeometry::Append(Geometry &geom, ArenaAllocator &alloc, const Geometry &other) {
	Append(geom, alloc, &other, 1);
}

void SinglePartGeometry::Append(Geometry &geom, ArenaAllocator &alloc, const Geometry *others, uint32_t others_count) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.type));
	if (geom.IsReadOnly()) {
		MakeMutable(geom, alloc);
	}

	auto old_count = geom.data_count;
	auto new_count = old_count;
	for (uint32_t i = 0; i < others_count; i++) {
		new_count += others[i].Count();
		// The other geometries has to be single part
		D_ASSERT(GeometryTypes::IsSinglePart(others[i].type));
		// And have the same z and m properties
		D_ASSERT(geom.properties.HasZ() == others[i].properties.HasZ());
		D_ASSERT(geom.properties.HasM() == others[i].properties.HasM());
	}

	Resize(geom, alloc, new_count);

	auto vertex_size = geom.properties.VertexSize();
	for (uint32_t i = 0; i < others_count; i++) {
		auto other = others[i];
		memcpy(geom.data_ptr + old_count * vertex_size, other.data_ptr, vertex_size * other.data_count);
		old_count += other.data_count;
	}
	geom.data_count = new_count;
}

void SinglePartGeometry::SetVertexType(Geometry &geom, ArenaAllocator &alloc, bool has_z, bool has_m, double default_z,
                                       double default_m) {
	if (geom.properties.HasZ() == has_z && geom.properties.HasM() == has_m) {
		return;
	}
	if (geom.is_readonly) {
		MakeMutable(geom, alloc);
	}

	auto used_to_have_z = geom.properties.HasZ();
	auto used_to_have_m = geom.properties.HasM();
	auto old_vertex_size = geom.properties.VertexSize();

	geom.properties.SetZ(has_z);
	geom.properties.SetM(has_m);

	auto new_vertex_size = geom.properties.VertexSize();
	// Case 1: The new vertex size is larger than the old vertex size
	if (new_vertex_size > old_vertex_size) {
		geom.data_ptr = alloc.ReallocateAligned(geom.data_ptr, geom.data_count * old_vertex_size,
		                                        geom.data_count * new_vertex_size);

		// There are 5 cases here:
		if (used_to_have_m && has_m && !used_to_have_z && has_z) {
			// 1. We go from XYM to XYZM
			// This is special, because we need to slide the M value to the end of each vertex
			for (int64_t i = geom.data_count - 1; i >= 0; i--) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				auto old_m_offset = old_offset + sizeof(double) * 2;
				auto new_z_offset = new_offset + sizeof(double) * 2;
				auto new_m_offset = new_offset + sizeof(double) * 3;
				// Move the M value
				memcpy(geom.data_ptr + new_m_offset, geom.data_ptr + old_m_offset, sizeof(double));
				// Set the new Z value
				memcpy(geom.data_ptr + new_z_offset, &default_z, sizeof(double));
				// Move the X and Y values
				memcpy(geom.data_ptr + new_offset, geom.data_ptr + old_offset, sizeof(double) * 2);
			}
		} else if (!used_to_have_z && has_z && !used_to_have_m && has_m) {
			// 2. We go from XY to XYZM
			// This is special, because we need to add both the default Z and M values to the end of each vertex
			for (int64_t i = geom.data_count - 1; i >= 0; i--) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				memcpy(geom.data_ptr + new_offset, geom.data_ptr + old_offset, sizeof(double) * 2);
				memcpy(geom.data_ptr + new_offset + sizeof(double) * 2, &default_z, sizeof(double));
				memcpy(geom.data_ptr + new_offset + sizeof(double) * 3, &default_m, sizeof(double));
			}
		} else {
			// Otherwise:
			// 3. We go from XY to XYZ
			// 4. We go from XY to XYM
			// 5. We go from XYZ to XYZM
			// These are all really the same, we just add the default to the end
			auto default_value = has_m ? default_m : default_z;
			for (int64_t i = geom.data_count - 1; i >= 0; i--) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				memmove(geom.data_ptr + new_offset, geom.data_ptr + old_offset, old_vertex_size);
				memcpy(geom.data_ptr + new_offset + old_vertex_size, &default_value, sizeof(double));
			}
		}
	}
	// Case 2: The new vertex size is equal to the old vertex size
	else if (new_vertex_size == old_vertex_size) {
		// This only happens when we go from XYZ -> XYM or XYM -> XYZ
		// In this case we just need to set the default on the third dimension
		auto default_value = has_m ? default_m : default_z;
		for (uint32_t i = 0; i < geom.data_count; i++) {
			auto offset = i * new_vertex_size + sizeof(double) * 2;
			memcpy(geom.data_ptr + offset, &default_value, sizeof(double));
		}
	}
	// Case 3: The new vertex size is smaller than the old vertex size.
	// In this case we need to allocate new memory and copy the data over to not lose any data
	else {
		auto new_data = alloc.AllocateAligned(geom.data_count * new_vertex_size);
		memset(new_data, 0, geom.data_count * new_vertex_size);

		// Special case: If we go from XYZM to XYM, we need to slide the M value to the end of each vertex
		if (used_to_have_z && used_to_have_m && !has_z && has_m) {
			for (uint32_t i = 0; i < geom.data_count; i++) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				memcpy(new_data + new_offset, geom.data_ptr + old_offset, sizeof(double) * 2);
				auto m_offset = old_offset + sizeof(double) * 3;
				memcpy(new_data + new_offset + sizeof(double) * 2, geom.data_ptr + m_offset, sizeof(double));
			}
		} else {
			// Otherwise, we just copy the data over
			for (uint32_t i = 0; i < geom.data_count; i++) {
				auto old_offset = i * old_vertex_size;
				auto new_offset = i * new_vertex_size;
				memcpy(new_data + new_offset, geom.data_ptr + old_offset, new_vertex_size);
			}
		}
		geom.data_ptr = new_data;
	}
}

void SinglePartGeometry::MakeMutable(Geometry &geom, ArenaAllocator &alloc) {
	if (!geom.is_readonly) {
		return;
	}

	if (geom.data_count == 0) {
		geom.data_ptr = nullptr;
		geom.is_readonly = false;
		return;
	}

	auto data_size = ByteSize(geom);
	auto new_data = alloc.AllocateAligned(data_size);
	memcpy(new_data, geom.data_ptr, data_size);
	geom.data_ptr = new_data;
	geom.is_readonly = false;
}

bool SinglePartGeometry::IsClosed(const Geometry &geom) {
	switch (geom.Count()) {
	case 0:
		return false;
	case 1:
		return true;
	default:
		VertexXY first = GetVertex(geom, 0);
		VertexXY last = GetVertex(geom, geom.Count() - 1);
		// TODO: Approximate comparison?
		return first.x == last.x && first.y == last.y;
	}
}

double SinglePartGeometry::Length(const Geometry &geom) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.type));
	double length = 0;
	for (uint32_t i = 1; i < geom.data_count; i++) {
		auto p1 = GetVertex(geom, i - 1);
		auto p2 = GetVertex(geom, i);
		length += sqrt((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y));
	}
	return length;
}

string SinglePartGeometry::ToString(const Geometry &geom, uint32_t start, uint32_t count) {
	D_ASSERT(GeometryTypes::IsSinglePart(geom.type));
	auto has_z = geom.properties.HasZ();
	auto has_m = geom.properties.HasM();

	D_ASSERT(geom.type == GeometryType::POINT || geom.type == GeometryType::LINESTRING);
	auto type_name = geom.type == GeometryType::POINT ? "POINT" : "LINESTRING";

	if (has_z && has_m) {
		string result = StringUtil::Format("%s XYZM ([%d-%d]/%d) [", type_name, start, start + count, geom.data_count);
		for (uint32_t i = start; i < count; i++) {
			auto vertex = GetVertex<VertexXYZM>(geom, i);
			result += "(" + MathUtil::format_coord(vertex.x, vertex.y, vertex.z, vertex.m) + ")";
			if (i < count - 1) {
				result += ", ";
			}
		}
		result += "]";
		return result;
	} else if (has_z) {
		string result = StringUtil::Format("%s XYZ ([%d-%d]/%d) [", type_name, start, start + count, geom.data_count);
		for (uint32_t i = start; i < count; i++) {
			auto vertex = GetVertex<VertexXYZ>(geom, i);
			result += "(" + MathUtil::format_coord(vertex.x, vertex.y, vertex.z) + ")";
			if (i < count - 1) {
				result += ", ";
			}
		}
		result += "]";
		return result;
	} else if (has_m) {
		string result = StringUtil::Format("%s XYM ([%d-%d]/%d) [", type_name, start, start + count, geom.data_count);
		for (uint32_t i = start; i < count; i++) {
			auto vertex = GetVertex<VertexXYM>(geom, i);
			result += "(" + MathUtil::format_coord(vertex.x, vertex.y, vertex.m) + ")";
			if (i < count - 1) {
				result += ", ";
			}
		}
		result += "]";
		return result;
	} else {
		string result = StringUtil::Format("%s XY ([%d-%d]/%d) [", type_name, start, start + count, geom.data_count);
		for (uint32_t i = start; i < count; i++) {
			auto vertex = GetVertex<VertexXY>(geom, i);
			result += "(" + MathUtil::format_coord(vertex.x, vertex.y) + ")";
			if (i < count - 1) {
				result += ", ";
			}
		}
		result += "]";
		return result;
	}
}

//------------------------------------------------------------------------------
// Geometry
//------------------------------------------------------------------------------
void Geometry::SetVertexType(ArenaAllocator &alloc, bool has_z, bool has_m, double default_z, double default_m) {
	struct op {
		static void Case(Geometry::Tags::SinglePartGeometry, Geometry &geom, ArenaAllocator &alloc, bool has_z,
		                 bool has_m, double default_z, double default_m) {
			SinglePartGeometry::SetVertexType(geom, alloc, has_z, has_m, default_z, default_m);
		}
		static void Case(Geometry::Tags::MultiPartGeometry, Geometry &geom, ArenaAllocator &alloc, bool has_z,
		                 bool has_m, double default_z, double default_m) {
			geom.properties.SetZ(has_z);
			geom.properties.SetM(has_m);
			for (auto &p : MultiPartGeometry::Parts(geom)) {
				p.SetVertexType(alloc, has_z, has_m, default_z, default_m);
			}
		}
	};
	Geometry::Match<op>(*this, alloc, has_z, has_m, default_z, default_m);
}

//------------------------------------------------------------------------------
// Multi Part Geometry
//------------------------------------------------------------------------------
/*
void MultiPartGeometry::Resize(Geometry& geom, ArenaAllocator &alloc, uint32_t new_count) {
    D_ASSERT(GeometryTypes::IsMultiPart(geom.type));
    if (new_count == geom.data_count) {
        return;
    }
    if (geom.data_ptr == nullptr) {
        geom.data_ptr = alloc.AllocateAligned(sizeof(Geometry) * new_count);
        // Need to create a new Geometry for each entry
        for (uint32_t i = 0; i < new_count; i++) {
            new (geom.data_ptr + i * sizeof(Geometry)) Geometry();
        }
    }
    else if(geom.IsReadOnly()) {
        auto new_data = alloc.AllocateAligned(sizeof(Geometry) * new_count);
        for(uint32_t i = 0; i < geom.data_count; i++) {
            new (new_data + i * sizeof(Geometry)) Geometry();
            new_data[i] = geom.data_ptr[i];
        }


        geom.data_ptr = new_data;
    }
    else {
        geom.data_ptr = alloc.ReallocateAligned(
            geom.data_ptr, geom.data_count * sizeof(Geometry), new_count * sizeof(Geometry));
        // If we added new entries, we need to create a new Geometry for each entry
        for (uint32_t i = geom.data_count; i < new_count; i++) {
            new (geom.data_ptr + i * sizeof(Geometry)) Geometry();
        }
    }
    geom.data_count = new_count;
}
 */

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

} // namespace core

} // namespace spatial
