---
{
    "id": "st_height",
    "title": "ST_Height",
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
    "summary": "Returns the height of the raster in pixels",
    "see_also": [ "st_width" ],
    "tags": [ "property" ]
}
---

### Description

Returns the height of the raster in pixels.

### Examples

```sql
SELECT ST_Height(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
