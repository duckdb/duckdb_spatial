---
{
    "id": "st_quadkey",
    "title": "ST_QuadKey",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                },
                {
                    "name": "level",
                    "type": "INTEGER"
                }
            ]
        },
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "longitude",
                    "type": "DOUBLE"
                },
                {
                    "name": "latitude",
                    "type": "DOUBLE"
                },
                {
                    "name": "level",
                    "type": "INTEGER"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Computes a quadkey from a given lon/lat point.",
    "see_also": [ ],
    "tags": [ "property" ]
}
---

### Description

Compute the [quadkey](https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system) for a given lon/lat point at a given level.
Note that the the parameter order is __longitude__, __latitude__.

`level` has to be between 1 and 23, inclusive.

The input coordinates will be clamped to the lon/lat bounds of the earth (longitude between -180 and 180, latitude between -85.05112878 and 85.05112878).

Throws for any geometry that is not a `POINT`

### Examples

```sql
SELECT ST_QuadKey(st_point(11.08, 49.45), 10);
-- 1333203202
```