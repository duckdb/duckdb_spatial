---
{
    "id": "st_asgeojson",
    "title": "ST_AsGeoJSON",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the geometry as a GeoJSON fragment",
    "tags": [
        "conversion"
    ]
}
---

### Description

Returns the geometry as a GeoJSON fragment. 

This does not return a complete GeoJSON document, only the geometry fragment. To construct a complete GeoJSON document or feature, look into using the DuckDB JSON extension in conjunction with this function.

### Examples

```sql
select ST_AsGeoJSON('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
----
{"type":"Polygon","coordinates":[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[1.0,0.0],[0.0,0.0]]]}

```