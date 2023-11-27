---
{
    "id": "st_area",
    "title": "ST_Area",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "point_2D",
                    "type": "POINT_2D"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "linestring_2d",
                    "type": "LINESTRING_2D"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "polygon_2d",
                    "type": "POLYGON_2D"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "box",
                    "type": "BOX_2D"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the area of a geometry.",
    "see_also": [ "st_area_spheroid", "st_perimeter" ],
    "tags": [ "property" ]
}
---

### Description

Compute the area of a geometry.

Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.

The `POINT_2D` and `LINESTRING_2D` variants of this function always return `0.0` but are included for completeness.

### Examples

```sql
select ST_Area('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
-- 1.0
```