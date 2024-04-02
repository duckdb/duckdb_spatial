---
{
    "id": "st_upperleftx",
    "title": "ST_UpperLeftX",
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
    "summary": "Returns the upper left X coordinate of raster in projected spatial reference",
    "see_also": [ "ST_UpperLeftY" ],
    "tags": [ "property" ]
}
---

### Description

Returns the upper left X coordinate of raster in projected spatial reference.

### Examples

```sql
SELECT ST_UpperLeftX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
