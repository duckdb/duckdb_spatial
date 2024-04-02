---
{
    "id": "st_pixelwidth",
    "title": "ST_PixelWidth",
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
    "summary": "Returns the width of a pixel in geometric units of the spatial reference system",
    "see_also": [ "st_pixelheight" ],
    "tags": [ "property" ]
}
---

### Description

Returns the width of a pixel in geometric units of the spatial reference system.
In the common case where there is no skew, the pixel width is just the scale ratio between geometric coordinates and raster pixels.

### Examples

```sql
SELECT ST_PixelWidth(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
