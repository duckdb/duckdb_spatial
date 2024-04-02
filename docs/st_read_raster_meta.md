---
{
    "type": "table_function",
    "title": "ST_ReadRaster_Meta",
    "id": "st_readraster_meta",
    "signatures": [
        {
            "parameters": [
                {
                    "name": "file",
                    "type": "VARCHAR"
                }
            ]
        },
        {
            "parameters": [
                {
                    "name": "files",
                    "type": "VARCHAR[]"
                }
            ]
        }
    ],
    "summary": "Read metadata from a variety of raster data sources",
    "tags": []
}
---

### Description

The `ST_Read_Meta` table function accompanies the [ST_ReadRaster](#st_readraster) table function, but instead of reading the contents of a file, this function scans the metadata instead.

### Examples

```sql
SELECT
	driver_short_name,
	driver_long_name,
	upper_left_x,
	upper_left_y,
	width,
	height,
	scale_x,
	scale_y,
	skew_x,
	skew_y,
	srid,
	num_bands
FROM
	ST_ReadRaster_Meta('./test/data/mosaic/SCL.tif-land-clip00.tiff')
;
```

```
┌───────────────────┬──────────────────┬──────────────┬──────────────┬───────┬────────┬─────────┬─────────┬────────┬────────┬───────┬───────────┐
│ driver_short_name │ driver_long_name │ upper_left_x │ upper_left_y │ width │ height │ scale_x │ scale_y │ skew_x │ skew_y │ srid  │ num_bands │
│      varchar      │     varchar      │    double    │    double    │ int32 │ int32  │ double  │ double  │ double │ double │ int32 │   int32   │
├───────────────────┼──────────────────┼──────────────┼──────────────┼───────┼────────┼─────────┼─────────┼────────┼────────┼───────┼───────────┤
│ GTiff             │ GeoTIFF          │     541020.0 │    4796640.0 │  3438 │   5322 │    20.0 │   -20.0 │    0.0 │    0.0 │ 32630 │         1 │
└───────────────────┴──────────────────┴──────────────┴──────────────┴───────┴────────┴─────────┴─────────┴────────┴────────┴───────┴───────────┘

```
