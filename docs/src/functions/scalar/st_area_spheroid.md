---
{
    "type": "scalar_function",
    "title": "ST_Area_Spheroid",
    "id": "st_area_spheroid",
    "signatures": [
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "polygon",
                    "type": "POLYGON_2D"
                }
            ]
        },
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "geometry",
                    "type": "GEOMETRY"
                }
            ]
        }
    ],
    "summary": "Returns the area of a geometry in meters, using an ellipsoidal model of the earth",
    "tags": [
        "property",
        "spheroid"
    ]
}
---

### Description

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the area is returned in square meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library, calculating the area using an ellipsoidal model of the earth. This is a highly accurate method for calculating the area of a polygon taking the curvature of the earth into account, but is also the slowest.

Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.


### Examples

TODO

