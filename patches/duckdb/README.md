# Custom window input context

DBSCAN needs the SQL partition boundaries and absolute input row for each custom
window callback. The DuckDB revision that this branch started from does not expose
them. `custom-window-row-context.patch` contains the companion DuckDB API change;
it does not alter the existing callback signatures.
`reject-window-distinct.patch` adds a binder error for DISTINCT on window-only
callbacks, whose executor cannot implement it.

The submodule includes this change for local testing. Before upstreaming Spatial,
submit the companion patches to DuckDB and replace the local submodule revision and
CI's `duckdb_version` with a published revision containing it. Do not publish an
extension compiled against this patch for an unpatched DuckDB release: the C++
window input layout has changed.

For a checkout of the original DuckDB revision, apply the patch explicitly:

```sh
git -C duckdb apply ../patches/duckdb/custom-window-row-context.patch
git -C duckdb apply ../patches/duckdb/reject-window-distinct.patch
```

Keep Linux and macOS build directories and vcpkg executables separate. CMake
caches contain absolute paths and cannot be moved between those environments.

The local companion commits are `ecf5c194` (row context) and `d2540618`
(DISTINCT validation), based on `bd77d596`. Their changes are included as patches
so reviewers can apply them without access to unpublished Git objects. The
current CI distribution still targets v1.5.4 and must be advanced before merging
or publishing this feature; changing the Spatial submodule alone does not update
that distribution target.
