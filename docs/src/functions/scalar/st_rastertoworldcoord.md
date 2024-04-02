---
{
    "id": "st_rastertoworldcoord",
    "title": "ST_RasterToWorldCoord",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "POINT_2D",
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
    "summary": "Returns the upper left corner as geometric X and Y (longitude and latitude) given a column and row",
    "see_also": [ "ST_RasterToWorldCoordX", "ST_RasterToWorldCoordY" ],
    "tags": [ "position" ]
}
---

### Description

Returns the upper left corner as geometric X and Y (longitude and latitude) given a column and row.
Returned X and Y are in geometric units of the georeferenced raster.

### Examples

```sql
SELECT ST_RasterToWorldCoord(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
