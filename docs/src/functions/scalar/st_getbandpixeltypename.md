---
{
    "id": "st_getbandpixeltypename",
    "title": "ST_GetBandPixelTypeName",
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
    "summary": "Returns the pixel type name of a band in the raster",
    "see_also": [ "ST_GetBandPixelType" ],
    "tags": [ "property" ]
}
---

### Description

Returns the pixel type name of a band in the raster.

This is a string in the enumeration:

+ Unknown: Unknown or unspecified type
+ Byte: Eight bit unsigned integer
+ Int8: 8-bit signed integer
+ UInt16: Sixteen bit unsigned integer
+ Int16: Sixteen bit signed integer
+ UInt32: Thirty two bit unsigned integer
+ Int32: Thirty two bit signed integer
+ UInt64: 64 bit unsigned integer
+ Int64: 64 bit signed integer
+ Float32: Thirty two bit floating point
+ Float64: Sixty four bit floating point
+ CInt16: Complex Int16
+ CInt32: Complex Int32
+ CFloat32: Complex Float32
+ CFloat64: Complex Float64

### Examples

```sql
SELECT ST_GetBandPixelTypeName(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
