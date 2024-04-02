---
{
    "type": "aggregate_function",
    "title": "ST_RasterUnion_Agg",
    "id": "st_rasterunion_agg",
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
    "summary": "Computes the union of a set of input rasters",
    "see_also": [ "st_rastermosaic_agg" ],
    "tags": [
        "construction"
    ]
}
---

### Description

Returns the union of a set of raster tiles into a single raster composed of at least one band.

Each tiles goes into a separate band in the result dataset.

`options` is optional, an array of parameters like [GDALBuildVRT](https://gdal.org/programs/gdalbuildvrt.html).

### Examples

```sql
WITH __input AS (
	SELECT
		1 AS raster_id,
		ST_RasterFromFile(file) AS raster
	FROM
		glob('./test/data/bands/*.tiff')
),
SELECT
	ST_RasterUnion_Agg(raster, options => ['-resolution', 'highest']) AS r
FROM
	__input
GROUP BY
	raster_id
;
```
