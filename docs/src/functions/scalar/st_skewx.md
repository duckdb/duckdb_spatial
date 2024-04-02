---
{
    "id": "st_skewx",
    "title": "ST_SkewX",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the georeference X skew (or rotation parameter) of the raster",
    "see_also": [ "st_skewy" ],
    "tags": [ "property" ]
}
---

### Description

Returns the georeference X skew (or rotation parameter).
Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.

### Examples

```sql
SELECT ST_SkewX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
