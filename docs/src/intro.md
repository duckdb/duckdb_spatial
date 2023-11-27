
The spatial extension provides support for geospatial data processing in DuckDB.

## Installing and Loading

To install and load the DuckDB `spatial` extension, simply run:

```sql
INSTALL spatial;
LOAD spatial;
``` 

You can also get the latest beta version of the extension for the latest stable release of DuckDB. This version is built and reuploaded every time a pull request is merged into the `main` branch on spatial extensions GitHub repository. 
Install it by executing:

```sql
FORCE INSTALL spatial FROM 'http://nightly-extensions.duckdb.org';
```
Note that this will overwrite any existing `spatial` extension installed for the current version of DuckDB.