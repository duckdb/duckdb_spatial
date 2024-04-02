---
{
    "id": "st_worldtorastercoord",
    "title": "ST_WorldToRasterCoord",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "RASTER_COORD",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                },
                {
                    "name": "x",
                    "type": "DOUBLE"
                },
                {
                    "name": "y",
                    "type": "DOUBLE"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the upper left corner as column and row given geometric X and Y (longitude and latitude)",
    "see_also": [ "ST_WorldToRasterCoordX", "ST_WorldToRasterCoordY" ],
    "tags": [ "position" ]
}
---

### Description

Returns the upper left corner as column and row given geometric X and Y (longitude and latitude).
Geometric X and Y must be expressed in the spatial reference coordinate system of the raster.

### Examples

```sql
SELECT ST_WorldToRasterCoord(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
