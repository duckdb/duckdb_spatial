---
{
    "id": "st_worldtorastercoordy",
    "title": "ST_WorldToRasterCoordY",
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
    "summary": "Returns the row in the raster given geometric X and Y (longitude and latitude)",
    "see_also": [ "ST_WorldToRasterCoord", "ST_WorldToRasterCoordX" ],
    "tags": [ "position" ]
}
---

### Description

Returns the row in the raster given geometric X and Y (longitude and latitude).
Geometric X and Y must be expressed in the spatial reference coordinate system of the raster.

### Examples

```sql
SELECT ST_WorldToRasterCoordY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
