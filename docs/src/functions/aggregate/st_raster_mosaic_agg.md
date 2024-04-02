---
{
    "type": "aggregate_function",
    "title": "ST_RasterMosaic_Agg",
    "id": "st_rastermosaic_agg",
    "signatures": [
        {
            "returns": "RASTER",
            "parameters": [
                {
                    "name": "rasters",
                    "type": "RASTER[]"
                },
                {
                    "name": "options",
                    "type": "VARCHAR[]"
                }
            ]
        }
    ],
    "summary": "Computes a mosaic of a set of input rasters",
    "see_also": [ "st_rasterunion_agg" ],
    "tags": [
        "construction"
    ]
}
---

### Description

Returns a mosaic of a set of raster tiles into a single raster.

Tiles are considered as source rasters of a larger mosaic and the result dataset has as many
bands as one of the input files.

`options` is optional, an array of parameters like [GDALBuildVRT](https://gdal.org/programs/gdalbuildvrt.html).

### Examples

```sql
WITH __input AS (
	SELECT
		1 AS raster_id,
		ST_RasterFromFile(file) AS raster
	FROM
		glob('./test/data/mosaic/*.tiff')
),
SELECT
	ST_RasterMosaic_Agg(raster, options => ['-r', 'bilinear']) AS r
FROM
	__input
GROUP BY
	raster_id
;
```
