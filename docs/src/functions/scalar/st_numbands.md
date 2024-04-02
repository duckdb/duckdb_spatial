---
{
    "id": "st_numbands",
    "title": "ST_NumBands",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "INT",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the number of bands in the raster",
    "see_also": [ "ST_HasNoBand" ],
    "tags": [ "property" ]
}
---

### Description

Returns the number of bands in the raster.

### Examples

```sql
SELECT ST_NumBands(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
