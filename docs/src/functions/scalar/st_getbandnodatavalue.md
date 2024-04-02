---
{
    "id": "st_getbandnodatavalue",
    "title": "ST_GetBandNoDataValue",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "DOUBLE",
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
    "summary": "Returns the NODATA value of a band in the raster",
    "see_also": [],
    "tags": [ "property" ]
}
---

### Description

Returns the NODATA value of a band in the raster.

### Examples

```sql
SELECT ST_GetBandNoDataValue(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
