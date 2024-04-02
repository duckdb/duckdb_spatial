---
{
    "id": "st_rasterasfile",
    "title": "ST_RasterAsFile",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "RASTER",
            "parameters": [
                {
                    "name": "file",
                    "type": "VARCHAR"
                },
                {
                    "name": "driver",
                    "type": "VARCHAR"
                },
                {
                    "name": "write_options",
                    "type": "VARCHAR[]"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Writes a raster to a file path",
    "see_also": [ ],
    "tags": [ "construction" ]
}
---

### Description

Writes a raster to a file path.

`write_options` is optional, an array of parameters for the GDAL driver specified.

### Examples

```sql
WITH __input AS (
	SELECT
		ST_RasterFromFile(file) AS raster
	FROM
		glob('./test/data/mosaic/SCL.tif-land-clip00.tiff')
)
SELECT
	ST_RasterAsFile(raster, './rasterasfile.tiff', 'Gtiff') AS result
FROM
	__input
;
```
