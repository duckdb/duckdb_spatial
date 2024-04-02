---
{
    "id": "st_value",
    "title": "ST_Value",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                },
                {
                    "name": "bandnum",
                    "type": "INT"
                },
                {
                    "name": "col",
                    "type": "INT"
                },
                {
                    "name": "row",
                    "type": "INT"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the value of a given band in a given column, row pixel",
    "see_also": [ ],
    "tags": [ "property" ]
}
---

### Description

Returns the value of a given band in a given column, row pixel.
Band numbers start at 1 and band is assumed to be 1 if not specified.

### Examples

```sql
SELECT ST_Value(raster, 1, 0, 0) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
