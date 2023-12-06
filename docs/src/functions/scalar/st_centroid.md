---
{
    "id": "st_centroid",
    "title": "ST_Centroid",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "point_2d",
                    "type": "POINT_2D"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "linestring_2d",
                    "type": "LINESTRING_2D"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "polygon_2d",
                    "type": "POLYGON_2D"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "box",
                    "type": "BOX_2D"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the centroid of a geometry.",
    "tags": [
        "property"
    ]
}
---

### Description

Calculate the centroid of the geometry

### Examples

```sql
select st_centroid('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
-- POINT(0.5 0.5)
```
