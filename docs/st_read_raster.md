---
{
    "type": "table_function",
    "title": "ST_ReadRaster",
    "id": "st_readraster",
    "signatures": [
        {
            "parameters": [
                {
                    "name": "path",
                    "type": "VARCHAR"
                },
                {
                    "name": "open_options",
                    "type": "VARCHAR[]"
                },
                {
                    "name": "allowed_drivers",
                    "type": "VARCHAR[]"
                },
                {
                    "name": "sibling_files",
                    "type": "VARCHAR[]"
                }
            ]
        }
    ],
    "summary": "Loads a raster from a file path",
    "tags": []
}
---

### Description


The `ST_ReadRaster` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading spatial data from a variety of geospatial raster file formats as if they were DuckDB tables.

> See [ST_Drivers](##st_drivers) for a list of supported file formats and drivers.

Except for the `path` parameter, all parameters are optional.

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `path` | VARCHAR | The path to the file to read. Mandatory |
| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. |
| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. |
| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. E.g., the ESRI Shapefile driver requires a .shx file to be present. Although most of the time these can be discovered automatically. |

Note that GDAL is single-threaded, so this table function will not be able to make full use of parallelism.

### Examples

```sql
-- Read a GeoTIFF
SELECT * FROM ST_ReadRaster('some/file/path/filename.tiff');
```

### Replacement scans

By using `ST_ReadRaster`, the spatial extension also provides “replacement scans” for common raster file formats, allowing you to query files of these formats as if they were tables directly.

```sql
SELECT * FROM './path/to/some/shapefile/dataset.tiff';
```

In practice this is just syntax-sugar for calling ST_ReadRaster, so there is no difference in performance. If you want to pass additional options, you should use the ST_ReadRaster table function directly.

The following formats are currently recognized by their file extension:

| Format | Extension |
| ------ | --------- |
| GeoTiff COG | .tif, .tiff |
| Erdas Imagine | .img |
| GDAL Virtual | .vrt |
