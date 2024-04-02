---
{
    "id": "st_upperlefty",
    "title": "ST_UpperLeftY",
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
    "summary": "Returns the upper left Y coordinate of raster in projected spatial reference",
    "see_also": [ "ST_UpperLeftX" ],
    "tags": [ "property" ]
}
---

### Description

Returns the upper left Y coordinate of raster in projected spatial reference.

### Examples

```sql
SELECT ST_UpperLeftY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
