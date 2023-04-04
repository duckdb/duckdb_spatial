# DuckDB Spatial Extension

## What is this?
This is a prototype of a geospatial extension for DuckDB that adds support for working with spatial data and functions in the form of a `GEOMETRY` type based on the the "Simple Features" geometry model, as well as non-standard specialized columnar DuckDB native geometry types that provide better compression and faster execution in exchange for flexibility.

Please note that this extension is still in a very early stage of development, and the internal storage format for the geometry types may change indiscriminately between commits. We are actively working on it, and we welcome both contributions and feedback. Please see the [function table](#supported-functions) or the [roadmap entries](https://github.com/duckdblabs/duckdb_spatial/labels/roadmap) for the current implementation status.

If you or your organization have any interest in sponsoring development of this extension, or have any particular use cases you'd like to see prioritized or supported, please consider [sponsoring the DuckDB foundation](https://duckdb.org/foundation/) or [contacting DuckDB Labs](https://duckdblabs.com) for commercial support.

## Example Usage
```sql
-- Load the spatial extension and the parquet extension so we can read the NYC 
-- taxi ride data in GeoParquet format
LOAD spatial
LOAD parquet

CREATE TABLE rides AS SELECT * FROM './spatial/test/data/nyc_taxi/yellow_tripdata_2010-01-limit1mil.parquet'

-- Load the NYC taxi zone data from a shapefile using the gdal-based st_read function
CREATE TABLE zones AS SELECT zone, LocationId, borough, ST_GeomFromWKB(wkb_geometry) AS geom 
FROM st_read('./spatial/test/data/nyc_taxi/taxi_zones/taxi_zones.shx');

-- Compare the trip distance to the linear distance between the pickup and dropoff points to figure out how efficient 
-- the taxi drivers are (or how dirty the data is, since some diffs are negative). Transform the coordinates from WGS84 
-- (EPSG:4326) (lat/lon) to the NAD83 / New York Long Island ftUS (ESRI:102718) projection. Calculate the distance in 
-- feet using the ST_Distance and convert to miles (5280 ft/mile). Trips that are smaller than the aerial distance are 
-- likely to be erroneous, so we use this query to filter out some bad data. Although this is not entirely accurate 
-- since the distance we use does not take into account the curvature of the earth, but it is a good enough 
-- approximation for our purposes.
CREATE TABLE cleaned_rides AS SELECT 
    st_point(pickup_latitude, pickup_longitude) as pickup_point,
    st_point(dropoff_latitude, dropoff_longitude) as dropoff_point,
    dropoff_datetime::TIMESTAMP - pickup_datetime::TIMESTAMP as time,
    trip_distance,
    st_distance(
        st_transform(pickup_point, 'EPSG:4326', 'ESRI:102718'), 
        st_transform(dropoff_point, 'EPSG:4326', 'ESRI:102718')) / 5280 as aerial_distance, 
    trip_distance - aerial_distance as diff 
FROM rides 
WHERE diff > 0
ORDER BY diff DESC;

-- Since we dont have spatial indexes yet, use smaller dataset for the following example.
DELETE FROM cleaned_rides WHERE rowid > 5000;

-- Join the taxi rides with the taxi zones to get the start and end zone for each ride using the ST_Within function
-- to check if a point is within a polygon. Again we need to transform the coordinates from WGS84 to the NAD83 since
-- the taxi zone data is in that projection.
CREATE TABLE joined AS 
SELECT 
    pickup_point,
    dropoff_point,
    start_zone.zone as start_zone,
    end_zone.zone as end_zone, 
    trip_distance,
    time,
FROM cleaned_rides 
JOIN zones as start_zone 
ON ST_Within(st_transform(pickup_point, 'EPSG:4326', 'ESRI:102718'), start_zone.geom) 
JOIN zones as end_zone 
ON ST_Within(st_transform(dropoff_point, 'EPSG:4326', 'ESRI:102718'), end_zone.geom)

-- Export the joined table to a GeoJSONSeq file using the GDAL copy function, passing in a layer creation option. 
-- Since GeoJSON only supports a single geometry per feature, we need to use the ST_Collect function to combine the
-- pickup and dropoff points into a single multi point geometry.
COPY (
    SELECT 
        ST_AsWKB(ST_Collect([pickup_point, dropoff_point])) as geom, 
        start_zone, 
        end_zone, 
        time::VARCHAR as trip_time 
    FROM joined) 
TO 'joined.geojsonseq' 
WITH (FORMAT GDAL, DRIVER 'GeoJSONSeq', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES');


-- This extension also provides specialized columnar geometry types (POINT_2D, LINESTRING_2D, POLYGON_2D, BOX_2D) that 
-- can be used to store geometries in a columnar fashion. These types are castable to and from the GEOMETRY type, 
-- generally require less storage, compresses better and should be faster to execute once more functions provide 
-- overloads for them. Here we compare the compression ratio of points stored as POINT_2D with or without DuckDBs 
-- "Patas" double-precision floating-point compression. Note: compression only works on databases not "in memory".

PRAGMA force_compression='uncompressed';

CREATE TABLE rides_columnar AS SELECT 
    ST_Point2D(pickup_latitude, pickup_longitude) as pickup_point, 
    ST_Point2D(dropoff_latitude, dropoff_longitude) as dropoff_point 
FROM './spatial/test/data/nyc_taxi/yellow_tripdata_2010-01-limit1mil.parquet'
WHERE pickup_latitude BETWEEN 39 AND 41 
AND pickup_longitude BETWEEN -75 AND -69
ORDER BY pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude;

CHECKPOINT;

PRAGMA force_compression='patas'; -- switch to patas compression

CREATE TABLE rides_columnar_compressed AS SELECT * FROM rides_columnar;

CHECKPOINT;

SELECT uncompressed::DOUBLE / compressed::DOUBLE AS ratio FROM (
SELECT
    (SELECT count(DISTINCT block_id) FROM pragma_storage_info('rides_columnar') WHERE segment_type = 'DOUBLE')
        AS uncompressed,
    (SELECT count(DISTINCT block_id) FROM pragma_storage_info('rides_columnar_compressed') WHERE segment_type = 'DOUBLE') 
        AS compressed
)
-- 1.3x compression ratio!

```

## How to build
This extension is based on the [DuckDB extension template](https://github.com/duckdb/extension-template).
You need a recent version of CMake (3.20) and a C++11 compatible compiler.
If you're cross-compiling, you need a host sqlite3 executable in your path, otherwise the build should create and use its own sqlite3 executable. (This is required for creating the PROJ database).
You also need OpenSSL on your system. On ubuntu you can install it with `sudo apt install libssl-dev`, on macOS you can install it with `brew install openssl`.

We bundle all the other required dependencies in the `third_party` directory, which should be automatically built and statically linked into the extension. This may take some time the first time you build, but subsequent builds should be much faster.

We also highly recommend that you install [Ninja](https://ninja-build.org) which you can select when building by setting the `GEN=ninja` environment variable.

```
git clone --recurse-submodules https://github.com/duckdblabs/duckdb_spatial.git
cd duckdb_spatial
make debug
```
You can then invoke the built DuckDB (with the extension statically linked)
```
./build/debug/duckdb
```

Please see the Makefile for more options, or the extension template documentation for more details.

## Limitations and Roadmap

The main limitations of this extension currently are:
- No support for higher-dimensional geometries (XYZ, XYZM, XYM)
- No support for spherical geometry (e.g. lat/lon coordinates)
- No support for spatial indexing.

These are all things that we want to address eventually, have a look at the open issues for more details. 
Please feel free to also open an issue if you have a specific use case that you would like to see supported.


## Internals and technical details

### Multi-tiered Geometry Type System
This extension implements 5 different geometry types. Like almost all geospatial databases we include a `GEOMETRY` type that (at least strives) to follow the Simple Features geometry model. This includes support for the standard subtypes, such as `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` that we all know and love, internally represented in a row-wise fashion on top of DuckDB `BLOB`s. The internal binary format is very similar to the one used by PostGIS - basically `double` aligned WKB, and we may eventually look into enforcing the format to be properly compatible with PostGIS (which may be useful for the PostGIS scanner extension). Most functions that are implemented for this type uses the [GEOS library](https://github.com/libgeos/geos), which is a battle-tested C++ port of the famous `JTS` library, to perform the actual operations on the geometries.

While having a flexible and dynamic `GEOMETRY` type is great to have, it is comparatively rare to work with columns containing mixed-geometries after the initial import and cleanup step. In fact, in most OLAP use cases you will probably only have a single geometry type in a table, and in those cases you're paying the performance cost to de/serialize and branch on the internal geometry format unneccessarily, i.e. you're paying for flexibility you're not using. For those cases we implement a set of non-standard DuckDB "native" geometry types, `POINT_2D`, `LINESTRING_2D`, `POLYGON_2D`, and `BOX_2D`. These types are built on DuckDBs `STRUCT` and `LIST` types, and are stored in a columnar fashion with the coordinate dimensions stored in separate "vectors". This makes it possible to leverage DuckDB's per-column statistics, compress much more efficiently and perform spatial operations on these geometries without having to de/serialize them first. Storing the coordinate dimensions into separate vectors also allows casting and converting between geometries with multiple different dimensions basically for free. And if you truly need to mix a couple of different geometry types, you can always use a DuckDB [UNION type](https://duckdb.org/docs/sql/data_types/union).

For now only a small amount of spatial functions are overloaded for these native types, but since they can be implicitly cast to `GEOMETRY` you can always use any of the functions that are implemented for `GEOMETRY` on them as well in the meantime while we work on adding more (although with a de/serialization penalty).

This extension also includes a `WKB_BLOB` type as an alias for `BLOB` that is used to indicate that the blob contains valid WKB encoded geometry.

### Per-thread Arena Allocation for Geometry Objects
When materializing the `GEOMETRY` type objects from the internal binary format we use per-thread arena allocation backed by DuckDB's buffer manager to amortize the contention and performance cost of performing lots of small heap allocations and frees, which allows us to utilizes DuckDB's multi-threaded vectorized out-of-core execution fully. While most spatial functions are implemented by wrapping `GEOS`, which requires an extra copy/allocation step anyway, the plan is to incrementally implementat our own versions of the simpler functions that can operate directly on our own `GEOMETRY` representation in order to greatly accelerate geospatial processing.

### Embedded PROJ Database
[PROJ](https://proj.org/#) is a generic coordinate transformation library that transforms geospatial coordinates from one projected coordinate reference system (CRS) to another. This extension experiments with including an embedded version of the PROJ database inside the extension binary itself so that you don't have to worry about installing the PROJ library separately. This also opens up the possibility to use this functionality in WASM.

### Embedded GDAL based Input/Output Functions
[GDAL](https://github.com/OSGeo/gdal) is a translator library for raster and vector geospatial data formats. This extension includes and exposes a subset of the GDAL vector drivers through the `ST_Read` and `COPY ... TO ... WITH (FORMAT GDAL)` table and copy functions respectively to read and write geometry data from and to a variety of file formats as if they were DuckDB tables. We currently support the over 50 GDAL formats - check for yourself by running `SELECT * FROM st_list_drivers();`!

Note that far from all of these formats have been tested properly, if you run into any issues please first [consult the GDAL docs](https://gdal.org/drivers/vector/index.html), or open an issue here on GitHub.


`ST_Read` also supports limited support for predicate pushdown and spatial filtering (if the underlying GDAL driver supports it), but column pruning (projection pushdown) while technically feasible is not yet implemented. 
`ST_Read` also allows using GDAL's virtual filesystem abstractions to read data from remote sources such as S3, or from compressed archives such as zip files. 

**Note**: This functionality does not make full use of parallelism due to GDAL not being thread-safe, so you should expect this to be slower than using e.g. the DuckDB Parquet extension to read the same GeoParquet or DuckDBs native csv reader to read csv files. Once we implement support for reading more vector formats natively through this extension (e.g. GeoJSON, GeoBuf, ShapeFile) we will probably split this entire GDAL part into a separate extension.


## Supported Functions

GEOS - functions that are implemented using the [GEOS](https://trac.osgeo.org/geos/) library

DuckDB - functions that are implemented natively in this extension that are capable of operating directly on the DuckDB types

CAST(GEOMETRY) - functions that are supported by implicitly casting to `GEOMETRY` and then using the `GEOMETRY` implementation

We are actively working on implementing more functions, and will update this table as we go.
Again, please feel free to open an issue if there is a particular function you would like to see implemented. Contributions are also welcome!


| Scalar functions            | GEOMETRY | POINT_2D                 | LINESTRING_2D            | POLYGON_2D               | BOX_2D                    |
| --------------------------- | -------- | ------------------------ | ------------------------ | ------------------------ | ------------------------- |
| ST_Point                    | DuckDB   | DuckDB                   |                          |                          |                           |
| ST_Area                     | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | DuckDB                    |
| ST_AsText                   | GEOS     | DuckDB                   | DuckDB                   | DuckDB                   | CAST(GEOMETRY as POLYGON) |
| ST_AsWKB                    | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | DuckDB                    |
| ST_Boundary                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Buffer                   | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Centroid                 | GEOS     | DuckDB                   | DuckDB                   | DuckDB                   | DuckDB                    |
| ST_Collect                  | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | DuckDB                    |
| ST_Contains                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | DuckDB or CAST(GEOMETRY) | CAST(GEOMETRY as POLYGON) |
| ST_ConvexHull               | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_CoveredBy                | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Covers                   | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Crosses                  | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Difference               | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Disjoint                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Distance                 | GEOS     | DuckDB or CAST(GEOMETRY) | DuckDB or CAST(GEOMETRY) | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_DWithin                  | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Envelope                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Equals                   | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_GeomFromText             | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_GeomFromWKB              | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | CAST(GEOMETRY as POLYGON) |
| ST_GeometryType             | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | CAST(GEOMETRY as POLYGON) |
| ST_Intersection             | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Intersects               | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_IsClosed                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_IsEmpty                  | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | CAST(GEOMETRY as POLYGON) |
| ST_IsRing                   | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_IsSimple                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_IsValid                  | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Length                   | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   |                           |
| ST_Normalize                | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Overlaps                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_SimplifyPreserveTopology | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Simplify                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Touches                  | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Union                    | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Within                   | GEOS     | DuckDB or CAST(GEOMETRY) | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_X                        | GEOS     | DuckDB                   | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Y                        | GEOS     | DuckDB                   | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)            |
