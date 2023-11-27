---
{
    "id": "st_astext",
    "title": "ST_AsText",
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
        },
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "point_2d",
                    "type": "POINT_2D"
                }
            ]
        },
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "linestring_2d",
                    "type": "LINESTRING_2D"
                }
            ]
        },
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "polygon_2d",
                    "type": "POLYGON_2D"
                }
            ]
        },
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "box",
                    "type": "BOX_2D"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the geometry as a WKT string",
    "tags": [
        "conversion"
    ]
}
---

### Description

Returns the geometry as a WKT string

### Examples

```sql
select st_astext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
```