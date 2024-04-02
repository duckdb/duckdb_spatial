---
{
    "id": "st_pixelheight",
    "title": "ST_PixelHeight",
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
    "summary": "Returns the height of a pixel in geometric units of the spatial reference system",
    "see_also": [ "st_pixelwidth" ],
    "tags": [ "property" ]
}
---

### Description

Returns the height of a pixel in geometric units of the spatial reference system.
In the common case where there is no skew, the pixel height is just the scale ratio between geometric coordinates and raster pixels.

### Examples

```sql
SELECT ST_PixelHeight(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
