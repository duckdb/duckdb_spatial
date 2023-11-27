---
{
    "id": "st_dump",
    "title": "ST_Dump",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "STRUCT(geom GEOMETRY, path INTEGER[])[]",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Dumps a geometry into a set of sub-geometries",
    "tags": [
        "construction"
    ]
}
---

### Description

Dumps a geometry into a set of sub-geometries and their "path" in the original geometry.

### Examples

```sql
select st_dump('MULTIPOINT(1 2,3 4)'::geometry);
-- [{'geom': 'POINT(1 2)', 'path': [0]}, {'geom': 'POINT(3 4)', 'path': [1]}]
```