---
{
    "id": "st_distance",
    "title": "ST_Distance",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "geom1",
                    "type": "GEOMETRY"
                },
                {
                    "name": "geom2",
                    "type": "GEOMETRY"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "point1",
                    "type": "POINT_2D"
                },
                {
                    "name": "point2",
                    "type": "POINT_2D"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "point",
                    "type": "POINT_2D"
                },
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
                    "name": "linestring_2d",
                    "type": "LINESTRING_2D"
                },
                {
                    "name": "point",
                    "type": "POINT_2D"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the distance between two geometries.",
    "description": "/functions/scalar/st_distance.md",
    "example": "/functions/scalar/st_distance_example.md",
    "see_also": [
        "st_distance_spheroid"
    ],
    "tags": [
        "property"
    ]
}
---

### Description

Returns the distance between two geometries.

### Examples

```sql
select st_distance('POINT(0 0)'::geometry, 'POINT(1 1)'::geometry);
-- 1.4142135623731
```

End of example
