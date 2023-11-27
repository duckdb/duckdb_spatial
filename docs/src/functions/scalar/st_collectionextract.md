---
{
    "id": "st_collectionextract",
    "title": "ST_CollectionExtract",
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
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                },
                {
                    "name": "dimension",
                    "type": "INTEGER"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Extracts a sub-geometry from a collection geometry",
    "tags": [
        "construction"
    ]
}
---

### Description

Extracts a sub-geometry from a collection geometry

### Examples

```sql
select st_collectionextract('MULTIPOINT(1 2,3 4)'::geometry, 1);
-- POINT(1 2)
```
