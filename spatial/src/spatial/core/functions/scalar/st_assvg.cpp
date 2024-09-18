#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/util/math.hpp"

namespace spatial {

namespace core {

static void GeometrySVGFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &arena = lstate.arena;

	struct op {

		static void PrintVertices(const Geometry &geom, const bool rel, const int32_t max_digits, bool close,
		                          vector<char> &buffer) {
			const auto vertex_count = LineString::VertexCount(geom);
			if (vertex_count == 0) {
				return;
			}

			VertexXY last_vert = LineString::GetVertex(geom, 0);
			buffer.push_back('M');
			buffer.push_back(' ');
			MathUtil::format_coord(last_vert.x, -last_vert.y, buffer, max_digits);

			if (vertex_count == 1) {
				return;
			}

			buffer.push_back(' ');
			buffer.push_back(rel ? 'l' : 'L');

			if (rel) {
				for (uint32_t i = 1; i < vertex_count; i++) {
					if (i == vertex_count - 1 && close) {
						buffer.push_back(' ');
						buffer.push_back('z');
					} else {
						const auto vert = LineString::GetVertex(geom, i);
						const auto relative_vert = vert - last_vert;
						last_vert = vert;
						buffer.push_back(' ');
						MathUtil::format_coord(relative_vert.x, -relative_vert.y, buffer, max_digits);
					}
				}
			} else {
				for (uint32_t i = 1; i < vertex_count; i++) {
					if (i == vertex_count - 1 && close) {
						buffer.push_back(' ');
						buffer.push_back('Z');
					} else {
						const auto vert = LineString::GetVertex(geom, i);
						buffer.push_back(' ');
						MathUtil::format_coord(vert.x, -vert.y, buffer, max_digits);
					}
				}
			}
		}

		static void Case(Geometry::Tags::Point, const Geometry &geom, const bool rel, const int32_t max_digits,
		                 vector<char> &buffer) {
			if (!Point::IsEmpty(geom)) {
				const auto vert = Point::GetVertex(geom);
				if (!rel) {
					constexpr auto cx = "cx=\"";
					constexpr auto cy = "cy=\"";
					buffer.insert(buffer.end(), cx, cx + 4);
					MathUtil::format_coord(vert.x, buffer, max_digits);
					buffer.push_back('"');
					buffer.push_back(' ');
					buffer.insert(buffer.end(), cy, cy + 4);
					MathUtil::format_coord(-vert.y, buffer, max_digits);
					buffer.push_back('"');
				} else {
					constexpr auto x = "x=\"";
					constexpr auto y = "y=\"";
					buffer.insert(buffer.end(), x, x + 3);
					MathUtil::format_coord(vert.x, buffer, max_digits);
					buffer.push_back('"');
					buffer.push_back(' ');
					buffer.insert(buffer.end(), y, y + 3);
					MathUtil::format_coord(-vert.y, buffer, max_digits);
					buffer.push_back('"');
				}
			}
		}
		static void Case(Geometry::Tags::LineString, const Geometry &geom, const bool rel, const int32_t max_digits,
		                 vector<char> &buffer) {
			PrintVertices(geom, rel, max_digits, false, buffer);
		}
		static void Case(Geometry::Tags::Polygon, const Geometry &geom, const bool rel, const int32_t max_digits,
		                 vector<char> &buffer) {
			const auto ring_count = Polygon::PartCount(geom);
			for (uint32_t i = 0; i < ring_count; i++) {
				const auto &ring = Polygon::Part(geom, i);
				PrintVertices(ring, rel, max_digits, true, buffer);
			}
		}
		static void Case(Geometry::Tags::MultiPartGeometry, const Geometry &geom, const bool rel,
		                 const int32_t max_digits, vector<char> &buffer) {
			// Special delimiter for multipoint and geometry collections
			char delimiter = ' ';
			if (geom.GetType() == GeometryType::MULTIPOINT) {
				delimiter = ',';
			} else if (geom.GetType() == GeometryType::GEOMETRYCOLLECTION) {
				delimiter = ';';
			}

			const auto part_count = MultiPartGeometry::PartCount(geom);
			for (uint32_t i = 0; i < part_count; i++) {
				if (i > 0) {
					buffer.push_back(delimiter);
				}
				auto &part = MultiPartGeometry::Part(geom, i);
				Geometry::Match<op>(part, rel, max_digits, buffer);
			}
		}
	};

	// Buffer holding the SVG fragment
	vector<char> buffer;

	TernaryExecutor::Execute<geometry_t, bool, int32_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](const geometry_t &blob, bool rel, int32_t max_digits) {
		    if (max_digits < 0 || max_digits > 15) {
			    throw InvalidInputException("max_digits must be between 0 and 15");
		    }

		    buffer.clear();

		    auto geom = Geometry::Deserialize(arena, blob);

		    Geometry::Match<op>(geom, rel, max_digits, buffer);

		    return StringVector::AddString(result, buffer.data(), buffer.size());
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Convert the geometry into a SVG fragment or path

    Convert the geometry into a SVG fragment or path
	The SVG fragment is returned as a string. The fragment is a path element that can be used in an SVG document.
	The second boolean argument specifies whether the path should be relative or absolute.
	The third argument specifies the maximum number of digits to use for the coordinates.

	Points are formatted as cx/cy using absolute coordinates or x/y using relative coordinates.
)";

static constexpr const char *DOC_EXAMPLE = R"(
SELECT ST_AsSVG('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::GEOMETRY, false, 15);
----
M 0 0 L 0 -1 1 -1 1 0 Z
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsSVG(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_AsSVG");
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::BOOLEAN, LogicalType::INTEGER},
	                               LogicalType::VARCHAR, GeometrySVGFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_AsSVG", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
