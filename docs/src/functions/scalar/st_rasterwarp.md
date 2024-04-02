---
{
    "id": "st_rasterwarp",
    "title": "ST_RasterWarp",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "RASTER",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                },
                {
                    "name": "options",
                    "type": "VARCHAR[]"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Performs mosaicing, reprojection and/or warping on a raster",
    "see_also": [ "ST_RasterClip" ],
    "tags": [ "construction" ]
}
---

### Description

Performs mosaicing, reprojection and/or warping on a raster.

`options` is optional, an array of parameters like [GDALWarp](https://gdal.org/programs/gdalwarp.html).

### Examples

```sql
WITH __input AS (
	SELECT
		raster
	FROM
		ST_ReadRaster('./test/data/mosaic/SCL.tif-land-clip00.tiff')
),
SELECT
	ST_RasterWarp(raster, options => ['-r', 'bilinear', '-tr', '40.0', '40.0']) AS warp
FROM
	__input
;
```
