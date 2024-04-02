---
{
    "id": "st_rasterclip",
    "title": "ST_RasterClip",
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
                    "name": "geometry",
                    "type": "GEOMETRY"
                },
                {
                    "name": "options",
                    "type": "VARCHAR[]"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns a raster that is clipped by the input geometry",
    "see_also": [ "ST_RasterWarp" ],
    "tags": [ "construction" ]
}
---

### Description

Returns a raster that is clipped by the input geometry.

`options` is optional, an array of parameters like [GDALWarp](https://gdal.org/programs/gdalwarp.html).

### Examples

```sql
WITH __input AS (
	SELECT
		ST_RasterFromFile(file) AS raster
	FROM
		glob('./test/data/mosaic/SCL.tif-land-clip00.tiff')
),
__geometry AS (
	SELECT geom FROM ST_Read('./test/data/mosaic/CATAST_Pol_Township-PNA.gpkg')
)
SELECT
	ST_RasterClip(mosaic,
				 (SELECT geom FROM __geometry LIMIT 1),
				  options =>
					[
						'-r', 'bilinear', '-crop_to_cutline', '-wo', 'CUTLINE_ALL_TOUCHED=TRUE'
					]
	) AS clip
FROM
	__input
;
```
