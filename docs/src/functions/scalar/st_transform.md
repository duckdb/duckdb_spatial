---
{
    "type": "scalar_function",
    "title": "ST_Transform",
    "id": "st_transform",
    "signatures": [
        {
            "returns": "BOX_2D",
            "parameters": [
                {
                    "name": "box",
                    "type": "BOX_2D"
                },
                {
                    "name": "source_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "target_crs",
                    "type": "VARCHAR"
                }
            ]
        },
        {
            "returns": "BOX_2D",
            "parameters": [
                {
                    "name": "box",
                    "type": "BOX_2D"
                },
                {
                    "name": "source_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "target_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "always_xy",
                    "type": "BOOLEAN"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "point",
                    "type": "POINT_2D"
                },
                {
                    "name": "source_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "target_crs",
                    "type": "VARCHAR"
                }
            ]
        },
        {
            "returns": "POINT_2D",
            "parameters": [
                {
                    "name": "point",
                    "type": "POINT_2D"
                },
                {
                    "name": "source_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "target_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "always_xy",
                    "type": "BOOLEAN"
                }
            ]
        },
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                },
                {
                    "name": "source_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "target_crs",
                    "type": "VARCHAR"
                }
            ]
        },
        {
            "returns": "GEOMETRY",
            "parameters": [
                {
                    "name": "geom",
                    "type": "GEOMETRY"
                },
                {
                    "name": "source_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "target_crs",
                    "type": "VARCHAR"
                },
                {
                    "name": "always_xy",
                    "type": "BOOLEAN"
                }
            ]
        }
    ],
    "summary": "Transforms a geometry between two coordinate systems",
    "tags": [
        "construction"
    ]
}
---

### Description

Transforms a geometry between two coordinate systems. The source and target coordinate systems can be specified using any format that the [PROJ library](https://proj.org) supports. 

The optional `always_xy` parameter can be used to force the input and output geometries to be interpreted as having a [northing, easting] coordinate axis order regardless of what the source and target coordinate system definition says. This is particularly useful when transforming to/from the [WGS84/EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (what most people think of when they hear "longitude"/"latitude" or "GPS coordinates"), which is defined as having a [latitude, longitude] axis order even though [longitude, latitude] is commonly used in practice (e.g. in [GeoJSON](https://tools.ietf.org/html/rfc7946)). More details available in the [PROJ documentation](https://proj.org/en/9.3/faq.html#why-is-the-axis-ordering-in-proj-not-consistent).

DuckDB spatial vendors its own static copy of the PROJ database of coordinate systems, so if you have your own installation of PROJ on your system the available coordinate systems may differ to what's available in other GIS software.

### Examples

```sql 

-- Transform a geometry from EPSG:4326 to EPSG:3857 (WGS84 to WebMercator)
-- Note that since WGS84 is defined as having a [latitude, longitude] axis order
-- we follow the standard and provide the input geometry using that axis order, 
-- but the output will be [northing, easting] because that is what's defined by 
-- WebMercator.

SELECT ST_AsText(
    ST_Transform(
        st_point(52.373123, 4.892360), 
        'EPSG:4326', 
        'EPSG:3857'
    )
);
----
POINT (544615.0239773799 6867874.103539125) 

-- Alternatively, let's say we got our input point from e.g. a GeoJSON file, 
-- which uses WGS84 but with [longitude, latitude] axis order. We can use the 
-- `always_xy` parameter to force the input geometry to be interpreted as having 
-- a [northing, easting] axis order instead, even though the source coordinate
-- system definition says otherwise.

SELECT ST_AsText(
    ST_Transform(
        -- note the axis order is reversed here
        st_point(4.892360, 52.373123),
        'EPSG:4326', 
        'EPSG:3857',
        always_xy := true
    )
);
----
POINT (544615.0239773799 6867874.103539125)


```
