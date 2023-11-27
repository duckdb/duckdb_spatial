---
{
    "id": "st_endpoint",
    "title": "ST_EndPoint",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "line",
                    "type": "GEOMETRY"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "line",
                    "type": "LINESTRING_2D"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the end point of a line.",
    "tags": [
        "property"
    ]
}
---

### Description

Returns the end point of a line.

### Examples

```sql
select st_endpoint('LINESTRING(0 0, 1 1)'::geometry);
-- POINT(1 1)
```
