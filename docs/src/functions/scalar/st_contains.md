---
{
    "id": "st_contains",
    "title": "ST_Contains",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "BOOLEAN",
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
            "returns": "BOOLEAN",
            "parameters": [
                {
                    "name": "polygon_2d",
                    "type": "POLYGON_2D"
                },
                {
                    "name": "point",
                    "type": "POINT_2D"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns true if geom1 contains geom2.",
    "tags": [
        "relation"
    ],
    "see_also": [
        "st_within"
    ]
}
---

### Description

Returns true if geom1 contains geom2.

### Examples

```sql
select st_contains('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry, 'POINT(0.5 0.5)'::geometry);
-- true
```
