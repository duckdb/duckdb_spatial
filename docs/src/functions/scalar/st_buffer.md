---
{
    "type": "scalar_function",
    "title": "ST_Buffer",
    "id": "st_buffer",
    "signatures": [
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
                },
                {
                    "name": "num_triangles",
                    "type": "INTEGER"
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
                },
                {
                    "name": "num_triangles",
                    "type": "INTEGER"
                },
                {
                    "name": "cap_style",
                    "type": "STRING"
                }
                {
                    "name": "join_style",
                    "type": "STRING"
                }
                {
                    "name": "mitre_limit",
                    "type": "DOUBLE"
                }
            ]
        }
    ],
    "summary": "Returns a buffer around the input geometry at the target distance"
}
---

### Description

`geom` is the input geometry.

`distance` is the target distance for the buffer, using the same units as the input geometry.

`num_triangles` represents how many triangles that will be produced to approximate a quarter circle. The larger the number, the smoother the resulting geometry. The default value is 8.

`join_style` must be one of "JOIN_ROUND", "JOIN_MITRE", "JOIN_BEVEL". This parameter is case-insensitive.

`cap_style` must be one of "CAP_ROUND", "CAP_FLAT", "CAP_SQUARE". This parameter is case-insensitive.

`mite_limit` only applies when `join_style` is "JOIN_MITRE". It is the ratio of the distance from the corner to the miter point to the corner radius. The default value is 1.0.

This is a planar operation and will not take into account the curvature of the earth.

### Examples

TODO

