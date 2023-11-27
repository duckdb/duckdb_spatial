# DuckDB Spatial Docs

This directory contains the source code for the DuckDB Spatial documentation.

## Building the docs

The full `docs.md` file is generated from the source files in this directory (`/docs/src/`) using a simple `generate.py` python3 script that lightly formats and concatenates a bunch of separate markdown files. To build the docs, simply run:

```bash
python3 generate.py
```

and the `docs.md` file will be generated in the parent directory (`/docs/docs.md`)

## Writing docs
The docs are written in Markdown, with some additional metadata stored in a json "frontmatter". The metadata is used to generate the `docs.md` file. Each function has its own file, with the filename corresponding to the function name. For example, the `ST_AsGeoJSON` function is documented in the `/functions/scalar/st_asgeojson.md` file. These file are the source of truth for the documentation, and the `docs.md` file is generated from these files. 

**Do not edit the `docs.md` file directly**.

### Frontmatter
The frontmatter is a json object that contains metadata about the documentation item. The frontmatter is enclosed in a pair of `---` lines, and is the first thing in the file. For example, the frontmatter for the `ST_AsGeoJSON` function is:

```json
---
{
    "id": "st_asgeojson",
    "title": "ST_AsGeoJSON",
    "type": "scalar_function",
    "signatures": [
        {
            "returns": "VARCHAR",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "aliases": [],
    "summary": "Returns the geometry as a GeoJSON fragment",
    "tags": [
        "conversion"
    ]
}
---
```

The frontmatter always contains the following fields:

| Field | Description |
| --- | --- |
| `type` | The documentation item type. e.g. `scalar_function`, `aggregate_function`, `table_function` |
| `id` | The documentation item id (unique, used internally by the doc generator) |

#### Functions 
Scalar and aggregate functions have the following additional fields:

| Field | Description |
| --- | --- |
| `title` | The function title (capitalized, human friendly name) |
| `signatures` | An array of function signatures. See signatures below |
| `aliases` | An array of function aliases. |
| `summary` | A short summary of the function |
| `tags` | An array of tags for the function. e.g. `conversion`, `geometry`, `measurement` |

Since functions can be overloaded, the `signatures` field is an array of function signatures. Each signature has the following fields:

| Field | Description |
| --- | --- |
| `returns` | The return type of the function, in SQL |
| `parameters` | An array of function parameters. See parameters below |

Each function parameter has the following fields:

| Field | Description |
| --- | --- |
| `name` | The parameter name |
| `type` | The parameter type, in SQL |

