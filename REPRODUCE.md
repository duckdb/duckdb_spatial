1. Generate proj.db

```c++
#include <stdio.h>
#include "spatial/src/spatial/proj/proj_db.c"

int main(void)
{
    for (unsigned int i = 0; i < proj_db_len; i++)
        putchar(proj_db[i]);
    return 0;
}
```

g++ file.cpp -o generate_proj && ./generate_proj > proj.db


2. Launch http-server from the same folder where proj.db is

3. `python3 duckdb/scripts/run_tests_one_by_one.py ./build/release/test/unittest '/Users/carlo/duckdblabs/duckdb_spatial/test/sql/*`


TODO:

* Sort out context cleanup (see failure in unittester)
* Make location configurable via DuckDB option (should there be a MAP[extension_name] -> (MAP[option_name] -> option_value) to store extension options needed at load time ?
* Figure out whether proj.db should be fetched on load or anyhow proper strategy. Or maybe native have it backed in (who cares) but optionally configurable, and Wasm has it always remote
