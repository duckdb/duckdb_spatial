# DBSCAN engine tests

These tests compile the production DBSCAN engine and packed R-tree using Spatial's
existing point and box types. Only the checked-out DuckDB headers and a C++11
compiler are required; GDAL, GEOS, and PROJ are not linked.

```sh
cmake -S test/unit -B build/dbscan-unit -DCMAKE_BUILD_TYPE=Release
cmake --build build/dbscan-unit
ctest --test-dir build/dbscan-unit --output-on-failure
```

`make test_dbscan` runs the same checks. They are also prerequisites of the normal
Makefile test targets and run on Linux and macOS in CI. Checks remain active when
`NDEBUG` is defined. To enable ASan and UBSan, configure a separate build directory
with `-DDBSCAN_SANITIZERS=ON`.

Coverage includes small and multi-level trees, rebuilds, random neighborhood
comparisons against brute force, duplicate points, extreme finite coordinates,
border-point adoption, and cancellation during construction and clustering.

Window integration is covered separately by
`test/sql/cluster/st_cluster_dbscan.test`, run by DuckDB's SQL test runner against
the built extension. It covers parallel row mapping, SQL partition isolation,
FILTER, frame semantics, invalid parameters, zero parameters, and NULL points.
Those tests require the companion DuckDB API changes documented in
`patches/duckdb/README.md`.

`test_scale_benchmark.cpp` is an optional manual benchmark, not a correctness test
or evidence of PostGIS parity.
