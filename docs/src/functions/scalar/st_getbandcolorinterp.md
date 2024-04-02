---
{
    "id": "st_getbandcolorinterp",
    "title": "ST_GetBandColorInterp",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "INT",
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
    "summary": "Returns the color interpretation of a band in the raster",
    "see_also": [ "ST_GetBandColorInterpName" ],
    "tags": [ "property" ]
}
---

### Description

Returns the color interpretation of a band in the raster.

This is a code in the enumeration:

+ Undefined = 0: Undefined
+ GrayIndex = 1: Greyscale
+ PaletteIndex = 2: Paletted (see associated color table)
+ RedBand = 3: Red band of RGBA image
+ GreenBand = 4: Green band of RGBA image
+ BlueBand = 5: Blue band of RGBA image
+ AlphaBand = 6: Alpha (0=transparent, 255=opaque)
+ HueBand = 7: Hue band of HLS image
+ SaturationBand = 8: Saturation band of HLS image
+ LightnessBand = 9: Lightness band of HLS image
+ CyanBand = 10: Cyan band of CMYK image
+ MagentaBand = 11: Magenta band of CMYK image
+ YellowBand = 12: Yellow band of CMYK image
+ BlackBand = 13: Black band of CMYK image
+ YCbCr_YBand = 14: Y Luminance
+ YCbCr_CbBand = 15: Cb Chroma
+ YCbCr_CrBand = 16: Cr Chroma

### Examples

```sql
SELECT ST_GetBandColorInterp(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
