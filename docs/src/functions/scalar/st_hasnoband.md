---
{
    "id": "st_hasnoband",
    "title": "ST_HasNoBand",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "BOOL",
            "parameters": [
                {
                    "name": "raster",
                    "type": "RASTER"
                },
                {
                    "name": "bandnum",
                    "type": "INT"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns true if there is no band with given band number",
    "see_also": [ "ST_NumBands" ],
    "tags": [ "property" ]
}
---

### Description

Returns true if there is no band with given band number.
Band numbers start at 1 and band is assumed to be 1 if not specified.

### Examples

```sql
SELECT ST_HasNoBand(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
```
