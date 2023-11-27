---
 {
    "id": "st_union",
    "title": "ST_Union",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "GEOMETRY",
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
        }
    ],
    "aliases": [],
    "summary": "Returns the union of two geometries."
}
---

### Description

Returns the union of two geometries.

### Examples

```sql
SELECT ST_AsText(
    ST_Union(
        ST_GeomFromText('POINT(1 2)'), 
        ST_GeomFromText('POINT(3 4)')
    )
);
----
MULTIPOINT (1 2, 3 4) 
```
