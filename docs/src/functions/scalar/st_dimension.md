---
{
    "id": "st_dimension",
    "title": "ST_Dimension",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "INTEGER",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the dimension of a geometry.",
    "tags": [
        "property"
    ],
    "see_also": [
        "st_geometrytype"
    ]
}
---

### Description

Returns the dimension of a geometry.

### Examples

```sql
select st_dimension('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
-- 2
```
