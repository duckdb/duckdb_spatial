---
{
    "id": "st_skewy",
    "title": "ST_SkewY",
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
    "summary": "Returns the georeference Y skew (or rotation parameter) of the raster",
    "see_also": [ "st_skewx" ],
    "tags": [ "property" ]
}
---

### Description

Returns the georeference Y skew (or rotation parameter).
Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.

### Examples

```sql
SELECT ST_SkewY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
