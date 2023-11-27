---
{
    "type": "scalar_function",
    "title": "ST_Distance_Spheroid",
    "id": "st_distance_spheroid",
    "signatures": [
        {
            "returns": "DOUBLE",
            "parameters": [
                {
                    "name": "point",
                    "type": "POINT_2D"
                },
                {
                    "name": "p2",
                    "type": "POINT_2D"
                }
            ]
        }
    ],
    "summary": "Returns the distance between two geometries in meters using a ellipsoidal model of the earths surface",
    "tags": [
        "relation",
        "spheroid"
    ]
}
---

### Description

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the distance is returned in meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library to solve the [inverse geodesic problem](https://en.wikipedia.org/wiki/Geodesics_on_an_ellipsoid#Solution_of_the_direct_and_inverse_problems), calculating the distance between two points using an ellipsoidal model of the earth. This is a highly accurate method for calculating the distance between two arbitrary points taking the curvature of the earths surface into account, but is also the slowest.

### Examples

```sql
-- Note: the coordinates are in WGS84 and [latitude, longitude] axis order
-- Whats the distance between New York and Amsterdam (JFK and AMS airport)?
SELECT st_distance_spheroid(
    st_point(40.6446, 73.7797),     
    st_point(52.3130, 4.7725)
);
----
5243187.666873225
-- Roughly 5243km!
```