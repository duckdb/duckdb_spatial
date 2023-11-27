---
{
    "type": "scalar_function",
    "title": "ST_NumPoints",
    "id": "st_numpoints",
    "aliases": [ "st_npoints" ],
    "signatures": [
        {
            "returns": "UBIGINT",
            "parameters": [
                {
                    "name": "point",
                    "type": "POINT_2D"
                }
            ]
        },
        {
            "returns": "UBIGINT",
            "parameters": [
                {
                    "name": "line",
                    "type": "LINESTRING_2D"
                }
            ]
        },
        {
            "returns": "UBIGINT",
            "parameters": [
                {
                    "name": "polygon",
                    "type": "POLYGON_2D"
                }
            ]
        },
        {
            "returns": "UBIGINT",
            "parameters": [
                {
                    "name": "geom",
                    "type": "BOX_2D"
                }
            ]
        },
        {
            "returns": "UINTEGER",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "summary": "Returns the number of points within a geometry",
    "tags": [
        "property"
    ]
}
---

### Description

TODO

### Examples

TODO

