---
{
    "type": "scalar_function",
    "title": "ST_FlipCoordinates",
    "id": "st_flipcoordinates",
    "signatures": [
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "point",
                    "type": "POINT_2D"
                }
            ]
        },
        {
            "returns": "LINESTRING_2D",
            "parameters": [
                {
                    "name": "line",
                    "type": "LINESTRING_2D"
                }
            ]
        },
        {
            "returns": "POLYGON_2D",
            "parameters": [
                {
                    "name": "polygon",
                    "type": "POLYGON_2D"
                }
            ]
        },
        {
            "returns": "BOX_2D",
            "parameters": [
                {
                    "name": "box",
                    "type": "BOX_2D"
                }
            ]
        },
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "summary": "Returns a new geometry with the coordinates of the input geometry \"flipped\" so that x = y and y = x.",
    "tags": [
        "construction"
    ]
}
---

### Description

TODO

### Examples

TODO

