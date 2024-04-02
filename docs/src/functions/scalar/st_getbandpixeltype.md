---
{
    "id": "st_getbandpixeltype",
    "title": "ST_GetBandPixelType",
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
    "summary": "Returns the pixel type of a band in the raster",
    "see_also": [ "ST_GetBandPixelTypeName" ],
    "tags": [ "property" ]
}
---

### Description

Returns the pixel type of a band in the raster.

This is a code in the enumeration:

+ Unknown = 0: Unknown or unspecified type
+ Byte = 1: Eight bit unsigned integer
+ Int8 = 14: 8-bit signed integer
+ UInt16 = 2: Sixteen bit unsigned integer
+ Int16 = 3: Sixteen bit signed integer
+ UInt32 = 4: Thirty two bit unsigned integer
+ Int32 = 5: Thirty two bit signed integer
+ UInt64 = 12: 64 bit unsigned integer
+ Int64 = 13: 64 bit signed integer
+ Float32 = 6: Thirty two bit floating point
+ Float64 = 7: Sixty four bit floating point
+ CInt16 = 8: Complex Int16
+ CInt32 = 9: Complex Int32
+ CFloat32 = 10: Complex Float32
+ CFloat64 = 11: Complex Float64

### Examples

```sql
SELECT ST_GetBandPixelType(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
