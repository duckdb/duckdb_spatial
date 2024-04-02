---
{
    "id": "st_getbandcolorinterpname",
    "title": "ST_GetBandColorInterpName",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                },
                {
                    "name": "bandnum",
                    "type": "INT"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the color interpretation name of a band in the raster",
    "see_also": [ "ST_GetBandColorInterp" ],
    "tags": [ "property" ]
}
---

### Description

Returns the color interpretation name of a band in the raster.

This is a string in the enumeration:

+ Undefined: Undefined
+ Greyscale: Greyscale
+ Paletted: Paletted (see associated color table)
+ Red: Red band of RGBA image
+ Green: Green band of RGBA image
+ Blue: Blue band of RGBA image
+ Alpha: Alpha (0=transparent, 255=opaque)
+ Hue: Hue band of HLS image
+ Saturation: Saturation band of HLS image
+ Lightness: Lightness band of HLS image
+ Cyan: Cyan band of CMYK image
+ Magenta: Magenta band of CMYK image
+ Yellow: Yellow band of CMYK image
+ Black: Black band of CMYK image
+ YLuminance: Y Luminance
+ CbChroma: Cb Chroma
+ CrChroma: Cr Chroma

### Examples

```sql
SELECT ST_GetBandColorInterpName(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
