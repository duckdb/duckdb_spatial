---
{
    "id": "st_scalex",
    "title": "ST_ScaleX",
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
    "summary": "Returns the X component of the pixel width in units of coordinate reference system",
    "see_also": [ "st_scaley" ],
    "tags": [ "property" ]
}
---

### Description

Returns the X component of the pixel width in units of coordinate reference system.
Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.

### Examples

```sql
SELECT ST_ScaleX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
