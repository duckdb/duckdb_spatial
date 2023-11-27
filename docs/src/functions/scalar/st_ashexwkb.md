---
{
    "id": "st_ashexwkb",
    "title": "ST_AsHEXWKB",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the geometry as a HEXWKB string",
    "description": "Actually returns HEXEWKB",
    "tags": [
        "conversion"
    ],
    "see_also": [
        "st_aswkb"
    ]
}
---

### Description

Returns the geometry as a HEXWKB string

### Examples

```sql
select st_ashexwkb('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
```