---
{
    "type": "table_function",
    "title": "ST_Read_Meta",
    "id": "st_read_meta",
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
    "summary": "Read metadata from a variety of geospatial file formats",
    "tags": []
}
---

### Description

The `ST_Read_Meta` table function accompanies the [ST_Read](#st_read) table function, but instead of reading the contents of a file, this function scans the metadata instead.

Since the data model of the underlying GDAL library is quite flexible, most of the interesting metadata is within the returned `layers` column, which is a somewhat complex nested structure of DuckDB `STRUCT` and `LIST` types.

### Examples
Find the coordinate reference system authority name and code for the first layers first geometry column in the file
```sql
SELECT 
    layers[1].geometry_fields[1].crs.auth_name as name, 
    layers[1].geometry_fields[1].crs.auth_code as code 
FROM st_read_meta('../../tmp/data/amsterdam_roads.fgb');
```

```
┌─────────┬─────────┐
│  name   │  code   │
│ varchar │ varchar │
├─────────┼─────────┤
│ EPSG    │ 3857    │
└─────────┴─────────┘
```

Identify the format driver and the number of layers of a set of files
```sql
SELECT file_name, driver_short_name, len(layers) FROM st_read_meta('../../tmp/data/*');
```

```
┌───────────────────────────────────────────┬───────────────────┬─────────────┐
│                 file_name                 │ driver_short_name │ len(layers) │
│                  varchar                  │      varchar      │    int64    │
├───────────────────────────────────────────┼───────────────────┼─────────────┤
│ ../../tmp/data/amsterdam_roads_50.geojson │ GeoJSON           │           1 │
│ ../../tmp/data/amsterdam_roads.fgb        │ FlatGeobuf        │           1 │
│ ../../tmp/data/germany.osm.pbf            │ OSM               │           5 │
└───────────────────────────────────────────┴───────────────────┴─────────────┘
```
