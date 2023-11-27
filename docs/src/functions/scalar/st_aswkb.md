---
{
    "id": "st_aswkb",
    "title": "ST_AsWKB",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "WKB_BLOB",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the geometry as a WKB blob",
    "tags": [
        "conversion"
    ]
}
---

### Description

Returns the geometry as a WKB blob

### Examples

```sql
select st_aswkb('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
```
