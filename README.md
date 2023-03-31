# DuckDB Geometry Extension

## What is this?
This is a prototype of a geospatial extension for DuckDB that adds support for geometry types and functions.

If you or your organization have any interest in sponsoring development of this extension, or have any particular use cases you'd like to see prioritized or supported, please consider [sponsoring the DuckDB foundation](https://duckdb.org/foundation/) or [contacting DuckDB Labs](https://duckdblabs.com) for commercial support.

## Main Features

### Multi-tiered Geometry Type System
This extension implements 5 different geometry types. Like almost all geospatial databases we include a `GEOMETRY` type that (at least strives) to follow the Simple Features geometry model. This includes support for the standard subtypes, such as `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` that we all know and love, internally represented in a row-wise fashion on top of DuckDB `BLOB`s. The internal binary format is very similar to the one used by PostGIS - basically `double` aligned WKB, and we may eventually look into enforcing the format to be properly compatible with PostGIS (which may be useful for the PostGIS scanner extension). Most functions that are implemented for this type uses the [GEOS library](https://github.com/libgeos/geos), which is a battle-tested C++ port of the famous `JTS` library, to perform the actual operations on the geometries.

While having a flexible and dynamic `GEOMETRY` type is great to have, experience shows us that once you've imported a bunch of geospatial data in your database and finished the initial cleaning process, it is comparatively rare to work with columns containing mixed-geometries. In fact, in most OLAP use cases you will probably only have a single geometry type in a table, and in those cases you're paying the performance cost to de/serialize and branch on the internal geometry format unneccessarily, i.e. you're paying for flexibility you're not using. For those cases we implement a set of non-standard DuckDB "native" geometry types, `POINT_2D`, `LINESTRING_2D`, `POLYGON_2D`, and `BOX_2D`. These types are built on DuckDBs `STRUCT` and `LIST` types, and are stored in a columnar fashion with the coordinate dimensions stored in separate "vectors". This makes it possible to leverage DuckDB's per-column statistics, compress much more efficiently and perform spatial operations on these geometries without having to de/serialize them first. Storing the coordinate dimensions into separate vectors also allows casting and converting between geometries with multiple dimensions basically for free. And if you truly need to mix a couple of different geometry types, you can always use a DuckDB [UNION type](https://duckdb.org/docs/sql/data_types/union).

For now only a small amount of spatial functions are overloaded for these native types and we plan to add a lot more in the future, but since they can be implicitly cast to `GEOMETRY` you can always use any of the functions that are implemented for `GEOMETRY` on them as well in the meantime (although with a de/serialization penalty).

This extension also includes a `WKB_BLOB` type as an alias for `BLOB` that is used to indicate that the blob contains valid WKB encoded geometry.

### Per-thread Arena Allocation for Geometry Objects
When materializing the `GEOMETRY` type objects from the internal binary format we use per-thread aren allocation backed by DuckDB's buffer manager to amortize the contention and performance cost of performing lots of small heap allocations and frees, which allows us to utilizes DuckDB's multi-threaded vectorized execution fully. While most spatial functions are implemented by wrapping `GEOS`, which requires an extra copy/allocation step anyway, we want to incrementally add our own implementations of the simpler functions that can operate directly on our own `GEOMETRY` representation to greatly accelerate geospatial processing.

### Embedded PROJ Database
[PROJ](https://proj.org/#) is a generic coordinate transformation library that transforms geospatial coordinates from one projected coordinate reference system (CRS) to another. This extension experiments with including an embedded version of the PROJ database inside the extension binary itself so that you don't have to worry about installing the PROJ library separately. This also potentially makes it possible to use this functionality in WASM.

### Embedded GDAL based Input/Output Functions
[GDAL](https://github.com/OSGeo/gdal) is a translator library for raster and vector geospatial data formats. This extension includes and exposes a subset of the GDAL vector drivers through the `ST_Read` and `ST_Write` table and copy functions respectively to read and write geometry data from and to a variety of file formats as if they were DuckDB tables. We currently support the over 50 GDAL formats, check for yourself by running:
```
SELECT * FROM st_list_drivers();
```
Note that far from all of these formats have been tested properly, if you run into any issues please first [consult the GDAL docs](https://gdal.org/drivers/vector/index.html), or open an issue here on GitHub.


`ST_Read` also supports limited support for predicate pushdown and spatial filtering (if the underlying GDAL driver supports it), but column pruning (projection pushdown) while technically feasible is not yet implemented. 
`ST_Read` also allows using GDAL's virtual filesystem abstractions to read data from remote sources such as S3, or from compressed archives such as zip files. 

**Note**: This functionality does not make full use of parallelism due to GDAL not being thread-safe, so you should expect this to be slower than using e.g. the DuckDB Parquet extension to read the same GeoParquet or DuckDBs native csv reader to read csv files. Once we implement support for reading more vector formats natively through this extension (e.g. GeoJSON, GeoBuf, ShapeFile) we will probably split this entire GDAL part into a separate extension.


## Limitations and Roadmap

The main limitations of this extension currently are:
- No support for higher-dimensional geometries (XYZ, XYZM, XYM)
- No support for spherical geometry (e.g. lat/lon coordinates)
- No support for spatial indexing.

These are all things that we want to address eventually, have a look at the open issues for more details. 
Please feel free to also open an issue if you have a specific use case that you would like to see supported.

## Function Support/Implementation Table

GEOS - functions that are implemented using the [GEOS](https://trac.osgeo.org/geos/) library
DuckDB - functions that are implemented natively in this extension that are capable of operating directly on the DuckDB geometry data type
CAST(GEOMETRY) - functions that are supported by implicitly casting to `GEOMETRY` and then using the `GEOMETRY` implementation.

We are currently working on implementing more functions, and will update this table as we go.
Again, please feel free to open an issue if there is a particular function you would like to see implemented. Contributions are also welcome!


| Scalar functions            | GEOMETRY | POINT_2D                 | LINESTRING_2D            | POLYGON_2D               | BOX_2D                    |
| --------------------------- | -------- | ------------------------ | ------------------------ | ------------------------ | ------------------------- |
| ST_Point                    | DuckDB   | DuckDB                   |                          |                          |                           |
| ST_Area                     | DuckDB   | DuckDB                   | DuckDB                   | DuckDB                   | DuckDB                    |
| ST_AsText                   | GEOS     | DuckDB                   | DuckDB                   | DuckDB                   | CAST(GEOMETRY as POLYGON) |
| ST_AsWKB                    | BROKEN   | BROKEN                   | BROKEN                   | BROKEN                   | BROKEN                    |
| ST_Boundary                 | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
| ST_Buffer                   | GEOS     | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY)           | CAST(GEOMETRY as POLYGON) |
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
| ST_Y                        | GEOS     | DuckDB                   |                          |                          |                           |


# How to build
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

Please see the Makefile for more options.
