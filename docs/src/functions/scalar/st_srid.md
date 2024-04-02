---
{
    "id": "st_srid",
    "title": "ST_SRID",
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
    "summary": "Returns the spatial reference identifier of the raster",
    "see_also": [ "st_getgeometry" ],
    "tags": [ "property" ]
}
---

### Description

Returns the spatial reference identifier (EPSG code) of the raster.
Refer to [EPSG](https://spatialreference.org/ref/epsg/) for more details.

### Examples

```sql
SELECT ST_SRID(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
