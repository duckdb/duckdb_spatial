#include "spatial/common.hpp"
#include "spatial/core/functions/macros.hpp"

#include "duckdb/catalog/default/default_functions.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarMacros::Register(DatabaseInstance &db) {

	// Sadly this doesnt work since we cant overload macros :(
	/*
	// Macros
	DefaultMacro macros[] = {
	        // Accessors
	        {DEFAULT_SCHEMA, "x", {"geom", nullptr}, "ST_X(geom)"},
	        {DEFAULT_SCHEMA, "y", {"geom", nullptr}, "ST_Y(geom)"},
	        {DEFAULT_SCHEMA, "z", {"geom", nullptr}, "ST_Z(geom)"},
	        {DEFAULT_SCHEMA, "m", {"geom", nullptr}, "ST_M(geom)"},
	        {DEFAULT_SCHEMA, "xmin", {"geom", nullptr}, "ST_XMin(geom)"},
	        {DEFAULT_SCHEMA, "ymin", {"geom", nullptr}, "ST_YMin(geom)"},
	        {DEFAULT_SCHEMA, "zmin", {"geom", nullptr}, "ST_ZMin(geom)"},
	        {DEFAULT_SCHEMA, "mmin", {"geom", nullptr}, "ST_MMin(geom)"},
	        {DEFAULT_SCHEMA, "xmax", {"geom", nullptr}, "ST_XMax(geom)"},
	        {DEFAULT_SCHEMA, "ymax", {"geom", nullptr}, "ST_YMax(geom)"},
	        {DEFAULT_SCHEMA, "zmax", {"geom", nullptr}, "ST_ZMax(geom)"},
	        {DEFAULT_SCHEMA, "mmax", {"geom", nullptr}, "ST_MMax(geom)"},
	        // Predicates
	        {DEFAULT_SCHEMA, "overlaps", {"a", "b", nullptr}, "ST_Overlaps(a, b)"},
	        {DEFAULT_SCHEMA, "contains", {"a", "b", nullptr}, "ST_Contains(a, b)"},
	        {DEFAULT_SCHEMA, "intersects", {"a", "b", nullptr}, "ST_Intersects(a, b)"},
	        {DEFAULT_SCHEMA, "within", {"a", "b", nullptr}, "ST_Within(a, b)"},
	        {DEFAULT_SCHEMA, "covers", {"a", "b", nullptr}, "ST_Covers(a, b)"},
	        {DEFAULT_SCHEMA, "crosses", {"a", "b", nullptr}, "ST_Crosses(a, b)"},
	        {DEFAULT_SCHEMA, "touches", {"a", "b", nullptr}, "ST_Touches(a, b)"},

	        // Properties
	        {DEFAULT_SCHEMA, "area", {"geom", nullptr}, "ST_Area(geom)"},
	        {DEFAULT_SCHEMA, "length", {"geom", nullptr}, "ST_Length(geom)"},
	        {DEFAULT_SCHEMA, "centroid", {"geom", nullptr}, "ST_Centroid(geom)"},
	        {DEFAULT_SCHEMA, "is_empty", {"geom", nullptr}, "ST_IsEmpty(geom)"},
	        {DEFAULT_SCHEMA, "is_simple", {"geom", nullptr}, "ST_IsSimple(geom)"},
	        {DEFAULT_SCHEMA, "is_valid", {"geom", nullptr}, "ST_IsValid(geom)"},
	        {DEFAULT_SCHEMA, "is_closed", {"geom", nullptr}, "ST_IsClosed(geom)"},
	        // Conversion
	        {DEFAULT_SCHEMA, "as_wkb", {"geom", nullptr}, "ST_AsWKB(geom)"},
	        {DEFAULT_SCHEMA, "as_text", {"geom", nullptr}, "ST_AsText(geom)"},
	        {DEFAULT_SCHEMA, "as_json", {"geom", nullptr}, "ST_AsGeoJSON(geom)"},
	        // Misc
	        {DEFAULT_SCHEMA, "flip", {"geom", nullptr}, "ST_FlipCoordinates(geom)"},
	        {DEFAULT_SCHEMA, "reverse", {"geom", nullptr}, "ST_Reverse(geom)"},
	        {DEFAULT_SCHEMA, "extent", {"geom", nullptr}, "ST_Extent(geom)"},
	        {DEFAULT_SCHEMA, "buffer", {"geom", "radius", nullptr}, "ST_Buffer(geom, radius)"},
	        {DEFAULT_SCHEMA, "simplify", {"geom", "tolerance", nullptr}, "ST_Simplify(geom, tolerance)"}
	};

	for(auto &macro : macros) {
	    auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(macro);
	    info->on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	    ExtensionUtil::RegisterFunction(db, *info);
	}
	*/
}

} // namespace core

} // namespace spatial
