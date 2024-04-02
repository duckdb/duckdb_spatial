---
{
    "id": "st_getgeometry",
    "title": "ST_GetGeometry",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the polygon representation of the extent of the raster",
    "see_also": [ "st_srid" ],
    "tags": [ "property" ]
}
---

### Description

Returns the polygon representation of the extent of the raster.

### Examples

```sql
SELECT ST_GetGeometry(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
