---
{
    "type": "scalar_function",
    "title": "ST_RemoveRepeatedPoints",
    "id": "st_removerepeatedpoints",
    "signatures": [
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
            "returns": "LINESTRING_2D",
            "parameters": [
                {
                    "name": "line",
                    "type": "LINESTRING_2D"
                },
                {
                    "name": "distance",
                    "type": "DOUBLE"
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
        },
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                },
                {
                    "name": "distance",
                    "type": "DOUBLE"
                }
            ]
        }
    ],
    "summary": "Returns a new geometry with repeated points removed, optionally within a target distance of eachother.",
    "tags": [
        "construction"
    ]
}
---

### Description

TODO

### Examples

TODO

