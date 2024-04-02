---
{
    "id": "st_rastertoworldcoordy",
    "title": "ST_RasterToWorldCoordY",
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
                    "name": "col",
                    "type": "INT"
                },
                {
                    "name": "row",
                    "type": "INT"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the upper left Y coordinate of a raster column row in geometric units of the georeferenced raster.",
    "see_also": [ "ST_RasterToWorldCoord", "ST_RasterToWorldCoordX" ],
    "tags": [ "position" ]
}
---

### Description

Returns the upper left Y coordinate of a raster column row in geometric units of the georeferenced raster.
Returned Y is in geometric units of the georeferenced raster.

### Examples

```sql
SELECT ST_RasterToWorldCoordY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
