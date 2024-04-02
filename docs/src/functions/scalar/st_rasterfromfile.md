---
{
    "id": "st_rasterfromfile",
    "title": "ST_RasterfromFile",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "RASTER",
            "parameters": [
                {
                    "name": "file",
                    "type": "VARCHAR"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Loads a raster from a file path",
    "see_also": [ ],
    "tags": [ "construction" ]
}
---

### Description

Loads a raster from a file path.

### Examples

```sql
WITH __input AS (
	SELECT
		ST_RasterFromFile(file) AS raster
	FROM
		glob('./test/data/mosaic/*.tiff')
)
SELECT raster from __input;
```
