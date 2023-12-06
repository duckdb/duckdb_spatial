# DuckDB Spatial Extension

__Table of contents__

- [Introduction](#introduction)
- [Scalar Functions](#scalar-functions)
- [Aggregate Functions](#aggregate-functions)
- [Table Functions](#table-functions)
- [Functions by tag](#functions-by-tag)
# Introduction


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
# Scalar Functions

| Name | Summary |
| ---- | ----------- |
| [ST_Area](##st_area) | Returns the area of a geometry. |
| [ST_Area_Spheroid](##st_area_spheroid) | Returns the area of a geometry in meters, using an ellipsoidal model of the earth |
| [ST_AsGeoJSON](##st_asgeojson) | Returns the geometry as a GeoJSON fragment |
| [ST_AsHEXWKB](##st_ashexwkb) | Returns the geometry as a HEXWKB string |
| [ST_AsText](##st_astext) | Returns the geometry as a WKT string |
| [ST_AsWKB](##st_aswkb) | Returns the geometry as a WKB blob |
| [ST_Boundary](##st_boundary) | Returns the "boundary" of a geometry |
| [ST_Buffer](##st_buffer) | Returns a buffer around the input geometry at the target distance |
| [ST_Centroid](##st_centroid) | Returns the centroid of a geometry. |
| [ST_Collect](##st_collect) | Collects geometries into a collection geometry |
| [ST_CollectionExtract](##st_collectionextract) | Extracts a sub-geometry from a collection geometry |
| [ST_Contains](##st_contains) | Returns true if geom1 contains geom2. |
| [ST_ContainsProperly](##st_containsproperly) | Returns true if geom1 "properly contains" geom2 |
| [ST_ConvexHull](##st_convexhull) | Returns the convex hull enclosing the geometry |
| [ST_CoveredBy](##st_coveredby) | Returns true if geom1 is "covered" by geom2 |
| [ST_Covers](##st_covers) | Returns if geom1 "covers" geom2 |
| [ST_Crosses](##st_crosses) | Returns true if geom1 "crosses" geom2 |
| [ST_Difference](##st_difference) | Returns the "difference" between two geometries |
| [ST_Dimension](##st_dimension) | Returns the dimension of a geometry. |
| [ST_Disjoint](##st_disjoint) | Returns if two geometries are disjoint |
| [ST_Distance](##st_distance) | Returns the distance between two geometries. |
| [ST_Distance_Spheroid](##st_distance_spheroid) | Returns the distance between two geometries in meters using a ellipsoidal model of the earths surface |
| [ST_Dump](##st_dump) | Dumps a geometry into a set of sub-geometries |
| [ST_DWithin](##st_dwithin) | Returns if two geometries are within a target distance of eachother |
| [ST_DWithin_Spheroid](##st_dwithin_spheroid) | Returns if two POINT_2D's are within a target distance in meters, using an ellipsoidal model of the earths surface |
| [ST_EndPoint](##st_endpoint) | Returns the end point of a line. |
| [ST_Envelope](##st_envelope) | Returns the minimum bounding box for the input geometry as a polygon geometry. |
| [ST_Equals](##st_equals) | Compares two geometries for equality |
| [ST_Extent](##st_extent) | Returns the minimal bounding box enclosing the input geometry |
| [ST_ExteriorRing](##st_exteriorring) | Returns the exterior ring (shell) of a polygon geometry. |
| [ST_FlipCoordinates](##st_flipcoordinates) | Returns a new geometry with the coordinates of the input geometry "flipped" so that x = y and y = x. |
| [ST_GeometryType](##st_geometrytype) | Returns a 'GEOMETRY_TYPE' enum identifying the input geometry type. |
| [ST_GeomFromGeoJSON](##st_geomfromgeojson) | Deserializes a GEOMETRY from a GeoJSON fragment. |
| [ST_GeomFromHEXEWKB](##st_geomfromhexewkb) | Deserialize a GEOMETRY from a HEXEWKB encoded string |
| [ST_GeomFromHEXWKB](##st_geomfromhexwkb) | Creates a GEOMETRY from a HEXWKB string |
| [ST_GeomFromText](##st_geomfromtext) | Deserializes a GEOMETRY from a WKT string, optionally ignoring invalid geometries |
| [ST_GeomFromWKB](##st_geomfromwkb) | Deserializes a GEOMETRY from a WKB encoded blob |
| [ST_Intersection](##st_intersection) | Returns the "intersection" of geom1 and geom2 |
| [ST_Intersects](##st_intersects) | Returns true if two geometries intersects |
| [ST_Intersects_Extent](##st_intersects_extent) | Returns true if the extent of two geometries intersects |
| [ST_IsClosed](##st_isclosed) | Returns true if a geometry is "closed" |
| [ST_IsEmpty](##st_isempty) | Returns true if the geometry is "empty" |
| [ST_IsRing](##st_isring) | Returns true if the input line geometry is a ring (both ST_IsClosed and ST_IsSimple). |
| [ST_IsSimple](##st_issimple) | Returns true if the input geometry is "simple" |
| [ST_IsValid](##st_isvalid) | Returns true if the geometry is topologically "valid" |
| [ST_Length](##st_length) | Returns the length of the input line geometry |
| [ST_Length_Spheroid](##st_length_spheroid) | Returns the length of the input geometry in meters, using a ellipsoidal model of the earth |
| [ST_LineMerge](##st_linemerge) | "Merges" the input line geometry, optionally taking direction into account. |
| [ST_LineString2DFromWKB](##st_linestring2dfromwkb) | Deserialize a LINESTRING_2D from a WKB encoded geometry blob |
| [ST_MakeEnvelope](##st_makeenvelope) | Returns a minimal bounding box polygon enclosing the input geometry |
| [ST_MakeLine](##st_makeline) | Creates a LINESTRING geometry from a pair or list of input points |
| [ST_MakePolygon](##st_makepolygon) | Creates a polygon from a shell geometry and an optional set of holes |
| [ST_Normalize](##st_normalize) | Returns a "normalized" version of the input geometry. |
| [ST_NumGeometries](##st_numgeometries) | Returns the number of component geometries in a collection geometry |
| [ST_NumInteriorRings](##st_numinteriorrings) | Returns the number if interior rings of a polygon |
| [ST_NumPoints](##st_numpoints) | Returns the number of points within a geometry |
| [ST_Overlaps](##st_overlaps) | Returns true if geom1 "overlaps" geom2 |
| [ST_Perimeter](##st_perimeter) | Returns the length of the perimeter of the geometry |
| [ST_Perimeter_Spheroid](##st_perimeter_spheroid) | Returns the length of the perimeter in meters using an ellipsoidal model of the earths surface |
| [ST_Point](##st_point) | Creates a GEOMETRY point |
| [ST_Point2D](##st_point2d) | Creates a POINT_2D |
| [ST_Point2DFromWKB](##st_point2dfromwkb) | Deserialize a POINT_2D from a WKB encoded geometry blob |
| [ST_Point3D](##st_point3d) | Creates a POINT_3D |
| [ST_Point4D](##st_point4d) | Creates a POINT_4D |
| [ST_PointN](##st_pointn) | Returns the n'th vertex from the input geometry as a point geometry |
| [ST_PointOnSurface](##st_pointonsurface) | Returns a point that is guaranteed to be on the surface of the input geometry. Sometimes a useful alternative to ST_Centroid. |
| [ST_Polygon2DFromWKB](##st_polygon2dfromwkb) | Deserialize a POLYGON_2D from a WKB encoded blob |
| [ST_QuadKey](##st_quadkey) | Computes a quadkey from a given lon/lat point. |
| [ST_ReducePrecision](##st_reduceprecision) | Returns the geometry with all vertices reduced to the target precision |
| [ST_RemoveRepeatedPoints](##st_removerepeatedpoints) | Returns a new geometry with repeated points removed, optionally within a target distance of eachother. |
| [ST_Reverse](##st_reverse) | Returns a new version of the input geometry with the order of its vertices reversed |
| [ST_Simplify](##st_simplify) | Simplifies the input geometry by collapsing edges smaller than 'distance' |
| [ST_SimplifyPreserveTopology](##st_simplifypreservetopology) | Returns a simplified geometry but avoids creating invalid topologies |
| [ST_StartPoint](##st_startpoint) | Returns the first point of a line geometry |
| [ST_Touches](##st_touches) | Returns true if geom1 "touches" geom2 |
| [ST_Transform](##st_transform) | Transforms a geometry between two coordinate systems |
| [ST_Union](##st_union) | Returns the union of two geometries. |
| [ST_Within](##st_within) | Returns true if geom1 is "within" geom2 |
| [ST_X](##st_x) | Returns the X coordinate of a point geometry, or NULL if not a point or empty |
| [ST_XMax](##st_xmax) | Returns the maximum x coordinate of a geometry |
| [ST_XMin](##st_xmin) | Returns the minimum x coordinate of a geometry |
| [ST_Y](##st_y) | Returns the y coordinate of a point geometry, or NULL if not a point or empty. |
| [ST_YMax](##st_ymax) | Returns the maximum y coordinate of a geometry |
| [ST_YMin](##st_ymin) | Returns the minimum y coordinate of a geometry |

## ST_Area

_Returns the area of a geometry._

- __DOUBLE__ ST_Area(geom __GEOMETRY__)
- __DOUBLE__ ST_Area(point_2D __POINT_2D__)
- __DOUBLE__ ST_Area(linestring_2d __LINESTRING_2D__)
- __DOUBLE__ ST_Area(polygon_2d __POLYGON_2D__)
- __DOUBLE__ ST_Area(box __BOX_2D__)

### Description

Compute the area of a geometry.

Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.

The `POINT_2D` and `LINESTRING_2D` variants of this function always return `0.0` but are included for completeness.

### Examples

```sql
select ST_Area('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
-- 1.0
```

## ST_Area_Spheroid

_Returns the area of a geometry in meters, using an ellipsoidal model of the earth_

- __DOUBLE__ ST_Area_Spheroid(polygon __POLYGON_2D__)
- __DOUBLE__ ST_Area_Spheroid(geometry __GEOMETRY__)

### Description

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the area is returned in square meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library, calculating the area using an ellipsoidal model of the earth. This is a highly accurate method for calculating the area of a polygon taking the curvature of the earth into account, but is also the slowest.

Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.


### Examples

TODO

## ST_AsGeoJSON

_Returns the geometry as a GeoJSON fragment_

- __VARCHAR__ ST_AsGeoJSON(geom __GEOMETRY__)

### Description

Returns the geometry as a GeoJSON fragment. 

This does not return a complete GeoJSON document, only the geometry fragment. To construct a complete GeoJSON document or feature, look into using the DuckDB JSON extension in conjunction with this function.

### Examples

```sql
select ST_AsGeoJSON('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
----
{"type":"Polygon","coordinates":[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[1.0,0.0],[0.0,0.0]]]}

```

## ST_AsHEXWKB

_Returns the geometry as a HEXWKB string_

- __VARCHAR__ ST_AsHEXWKB(geom __GEOMETRY__)

### Description

Returns the geometry as a HEXWKB string

### Examples

```sql
select st_ashexwkb('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
```

## ST_AsText

_Returns the geometry as a WKT string_

- __VARCHAR__ ST_AsText(geom __GEOMETRY__)
- __VARCHAR__ ST_AsText(point_2d __POINT_2D__)
- __VARCHAR__ ST_AsText(linestring_2d __LINESTRING_2D__)
- __VARCHAR__ ST_AsText(polygon_2d __POLYGON_2D__)
- __VARCHAR__ ST_AsText(box __BOX_2D__)

### Description

Returns the geometry as a WKT string

### Examples

```sql
select st_astext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
```

## ST_AsWKB

_Returns the geometry as a WKB blob_

- __WKB_BLOB__ ST_AsWKB(geom __GEOMETRY__)

### Description

Returns the geometry as a WKB blob

### Examples

```sql
select st_aswkb('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
```

## ST_Boundary

_Returns the "boundary" of a geometry_

- __GEOMETRY__ ST_Boundary(geometry __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Buffer

_Returns a buffer around the input geometry at the target distance_

- __GEOMETRY__ ST_Buffer(geom __GEOMETRY__, distance __DOUBLE__)
- __GEOMETRY__ ST_Buffer(geom __GEOMETRY__, distance __DOUBLE__, num_triangles __INTEGER__)

### Description

TODO

### Examples

TODO

## ST_Centroid

_Returns the centroid of a geometry._

- __GEOMETRY__ ST_Centroid(geom __GEOMETRY__)
- __POINT_2D__ ST_Centroid(point_2d __POINT_2D__)
- __POINT_2D__ ST_Centroid(linestring_2d __LINESTRING_2D__)
- __POINT_2D__ ST_Centroid(polygon_2d __POLYGON_2D__)
- __POINT_2D__ ST_Centroid(box __BOX_2D__)

### Description

Calculate the centroid of the geometry

### Examples

```sql
select st_centroid('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
-- POINT(0.5 0.5)
```

End of example

## ST_Collect

_Collects geometries into a collection geometry_

- __GEOMETRY__ ST_Collect(geometries __GEOMETRY[]__)

### Description

Collects a list of geometries into a collection geometry. 
- If all geometries are `POINT`'s, a `MULTIPOINT` is returned. 
- If all geometries are `LINESTRING`'s, a `MULTILINESTRING` is returned. 
- If all geometries are `POLYGON`'s, a `MULTIPOLYGON` is returned. 
- Otherwise if the input collection contains a mix of geometry types, a `GEOMETRYCOLLECTION` is returned.

Empty and `NULL` geometries are ignored. If all geometries are empty or `NULL`, a `GEOMETRYCOLLECTION EMPTY` is returned.

### Examples

```sql
-- With all POINT's, a MULTIPOINT is returned
SELECT ST_Collect([ST_Point(1, 2), ST_Point(3, 4)]);
----
MULTIPOINT (1 2, 3 4)

-- With mixed geometry types, a GEOMETRYCOLLECTION is returned
SELECT ST_Collect([ST_Point(1, 2), ST_GeomFromText('LINESTRING(3 4, 5 6)')]);
----
GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))

-- Note that the empty geometry is ignored, so the result is a MULTIPOINT
SELECT ST_Collect([ST_Point(1, 2), NULL, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')]);
----
MULTIPOINT (1 2)

-- If all geometries are empty or NULL, a GEOMETRYCOLLECTION EMPTY is returned
SELECT ST_Collect([NULL, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')]);
----
GEOMETRYCOLLECTION EMPTY
```

Tip: You can use the `ST_Collect` function together with the `list()` aggregate function to collect multiple rows of geometries into a single geometry collection:

```sql
CREATE TABLE points (geom GEOMETRY);

INSERT INTO points VALUES (ST_Point(1, 2)), (ST_Point(3, 4));

SELECT ST_Collect(list(geom)) FROM points;
----
MULTIPOINT (1 2, 3 4)
```

## ST_CollectionExtract

_Extracts a sub-geometry from a collection geometry_

- __GEOMETRY__ ST_CollectionExtract(geom __GEOMETRY__)
- __GEOMETRY__ ST_CollectionExtract(geom __GEOMETRY__, dimension __INTEGER__)

### Description

Extracts a sub-geometry from a collection geometry

### Examples

```sql
select st_collectionextract('MULTIPOINT(1 2,3 4)'::geometry, 1);
-- POINT(1 2)
```

## ST_Contains

_Returns true if geom1 contains geom2._

- __BOOLEAN__ ST_Contains(geom1 __GEOMETRY__, geom2 __GEOMETRY__)
- __BOOLEAN__ ST_Contains(polygon_2d __POLYGON_2D__, point __POINT_2D__)

### Description

Returns true if geom1 contains geom2.

### Examples

```sql
select st_contains('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry, 'POINT(0.5 0.5)'::geometry);
-- true
```

## ST_ContainsProperly

_Returns true if geom1 "properly contains" geom2_

- __BOOLEAN__ ST_ContainsProperly(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_ConvexHull

_Returns the convex hull enclosing the geometry_

- __GEOMETRY__ ST_ConvexHull(col0 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_CoveredBy

_Returns true if geom1 is "covered" by geom2_

- __BOOLEAN__ ST_CoveredBy(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Covers

_Returns if geom1 "covers" geom2_

- __BOOLEAN__ ST_Covers(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Crosses

_Returns true if geom1 "crosses" geom2_

- __BOOLEAN__ ST_Crosses(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Difference

_Returns the "difference" between two geometries_

- __GEOMETRY__ ST_Difference(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Dimension

_Returns the dimension of a geometry._

- __INTEGER__ ST_Dimension(geom __GEOMETRY__)

### Description

Returns the dimension of a geometry.

### Examples

```sql
select st_dimension('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
-- 2
```

## ST_Disjoint

_Returns if two geometries are disjoint_

- __BOOLEAN__ ST_Disjoint(col0 __GEOMETRY__, col1 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Distance

_Returns the distance between two geometries._

- __DOUBLE__ ST_Distance(geom1 __GEOMETRY__, geom2 __GEOMETRY__)
- __DOUBLE__ ST_Distance(point1 __POINT_2D__, point2 __POINT_2D__)
- __DOUBLE__ ST_Distance(point __POINT_2D__, linestring_2d __LINESTRING_2D__)
- __DOUBLE__ ST_Distance(linestring_2d __LINESTRING_2D__, point __POINT_2D__)

### Description

Returns the distance between two geometries.

### Examples

```sql
select st_distance('POINT(0 0)'::geometry, 'POINT(1 1)'::geometry);
-- 1.4142135623731
```

End of example

## ST_Distance_Spheroid

_Returns the distance between two geometries in meters using a ellipsoidal model of the earths surface_

- __DOUBLE__ ST_Distance_Spheroid(point __POINT_2D__, p2 __POINT_2D__)

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

## ST_Dump

_Dumps a geometry into a set of sub-geometries_

- __STRUCT(geom GEOMETRY, path INTEGER[])[]__ ST_Dump(geom __GEOMETRY__)

### Description

Dumps a geometry into a set of sub-geometries and their "path" in the original geometry.

### Examples

```sql
select st_dump('MULTIPOINT(1 2,3 4)'::geometry);
-- [{'geom': 'POINT(1 2)', 'path': [0]}, {'geom': 'POINT(3 4)', 'path': [1]}]
```

## ST_DWithin

_Returns if two geometries are within a target distance of eachother_

- __BOOLEAN__ ST_DWithin(geom1 __GEOMETRY__, geom2 __GEOMETRY__, distance __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_DWithin_Spheroid

_Returns if two POINT_2D's are within a target distance in meters, using an ellipsoidal model of the earths surface_

- __DOUBLE__ ST_DWithin_Spheroid(p1 __POINT_2D__, p2 __POINT_2D__, distance __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_EndPoint

_Returns the end point of a line._

- __GEOMETRY__ ST_EndPoint(line __GEOMETRY__)
- __POINT_2D__ ST_EndPoint(line __LINESTRING_2D__)

### Description

Returns the end point of a line.

### Examples

```sql
select st_endpoint('LINESTRING(0 0, 1 1)'::geometry);
-- POINT(1 1)
```

## ST_Envelope

_Returns the minimum bounding box for the input geometry as a polygon geometry._

- __GEOMETRY__ ST_Envelope(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Equals

_Compares two geometries for equality_

- __BOOLEAN__ ST_Equals(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Extent

_Returns the minimal bounding box enclosing the input geometry_

- __BOX_2D__ ST_Extent(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_ExteriorRing

_Returns the exterior ring (shell) of a polygon geometry._

- __LINESTRING_2D__ ST_ExteriorRing(polygon __POLYGON_2D__)
- __GEOMETRY__ ST_ExteriorRing(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_FlipCoordinates

_Returns a new geometry with the coordinates of the input geometry "flipped" so that x = y and y = x._

- __POINT_2D__ ST_FlipCoordinates(point __POINT_2D__)
- __LINESTRING_2D__ ST_FlipCoordinates(line __LINESTRING_2D__)
- __POLYGON_2D__ ST_FlipCoordinates(polygon __POLYGON_2D__)
- __BOX_2D__ ST_FlipCoordinates(box __BOX_2D__)
- __GEOMETRY__ ST_FlipCoordinates(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_GeometryType

_Returns a 'GEOMETRY_TYPE' enum identifying the input geometry type._

- __ANY__ ST_GeometryType(point __POINT_2D__)
- __ANY__ ST_GeometryType(line __LINESTRING_2D__)
- __ANY__ ST_GeometryType(polygon __POLYGON_2D__)
- __ANY__ ST_GeometryType(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_GeomFromGeoJSON

_Deserializes a GEOMETRY from a GeoJSON fragment._

- __GEOMETRY__ ST_GeomFromGeoJSON(geojson __VARCHAR__)

### Description

TODO

### Examples

TODO

## ST_GeomFromHEXEWKB

_Deserialize a GEOMETRY from a HEXEWKB encoded string_

- __GEOMETRY__ ST_GeomFromHEXEWKB(hexewkb __VARCHAR__)

### Description

TODO

### Examples

TODO

## ST_GeomFromHEXWKB

_Creates a GEOMETRY from a HEXWKB string_

- __GEOMETRY__ ST_GeomFromHEXWKB(hexwkb __VARCHAR__)

### Description

TODO

### Examples

TODO

## ST_GeomFromText

_Deserializes a GEOMETRY from a WKT string, optionally ignoring invalid geometries_

- __GEOMETRY__ ST_GeomFromText(wkt __VARCHAR__)
- __GEOMETRY__ ST_GeomFromText(wkt __VARCHAR__, ignore_invalid __BOOLEAN__)

### Description

TODO

### Examples

TODO

## ST_GeomFromWKB

_Deserializes a GEOMETRY from a WKB encoded blob_

- __GEOMETRY__ ST_GeomFromWKB(blob __WKB_BLOB__)
- __GEOMETRY__ ST_GeomFromWKB(blob __BLOB__)

### Description

TODO

### Examples

TODO

## ST_Intersection

_Returns the "intersection" of geom1 and geom2_

- __GEOMETRY__ ST_Intersection(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Intersects

_Returns true if two geometries intersects_

- __BOOLEAN__ ST_Intersects(box1 __BOX_2D__, box2 __BOX_2D__)
- __BOOLEAN__ ST_Intersects(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Intersects_Extent

_Returns true if the extent of two geometries intersects_

- __BOOLEAN__ ST_Intersects_Extent(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_IsClosed

_Returns true if a geometry is "closed"_

- __BOOLEAN__ ST_IsClosed(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_IsEmpty

_Returns true if the geometry is "empty"_

- __BOOLEAN__ ST_IsEmpty(line __LINESTRING_2D__)
- __BOOLEAN__ ST_IsEmpty(polygon __POLYGON_2D__)
- __BOOLEAN__ ST_IsEmpty(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_IsRing

_Returns true if the input line geometry is a ring (both ST_IsClosed and ST_IsSimple)._

- __BOOLEAN__ ST_IsRing(line __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_IsSimple

_Returns true if the input geometry is "simple"_

- __BOOLEAN__ ST_IsSimple(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_IsValid

_Returns true if the geometry is topologically "valid"_

- __BOOLEAN__ ST_IsValid(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Length

_Returns the length of the input line geometry_

- __DOUBLE__ ST_Length(line __LINESTRING_2D__)
- __DOUBLE__ ST_Length(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Length_Spheroid

_Returns the length of the input geometry in meters, using a ellipsoidal model of the earth_

- __DOUBLE__ ST_Length_Spheroid(line __LINESTRING_2D__)
- __DOUBLE__ ST_Length_Spheroid(geom __GEOMETRY__)

### Description

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the length is returned in square meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library, calculating the length using an ellipsoidal model of the earth. This is a highly accurate method for calculating the length of a line geometry taking the curvature of the earth into account, but is also the slowest.

Returns `0.0` for any geometry that is not a `LINESTRING`, `MULTILINESTRING` or `GEOMETRYCOLLECTION` containing line geometries.


### Examples

TODO

## ST_LineMerge

_"Merges" the input line geometry, optionally taking direction into account._

- __GEOMETRY__ ST_LineMerge(linework __GEOMETRY__)
- __GEOMETRY__ ST_LineMerge(linework __GEOMETRY__, directed __BOOLEAN__)

### Description

TODO

### Examples

TODO

## ST_LineString2DFromWKB

_Deserialize a LINESTRING_2D from a WKB encoded geometry blob_

- __LINESTRING_2D__ ST_LineString2DFromWKB(blob __WKB_BLOB__)

### Description

TODO

### Examples

TODO

## ST_MakeEnvelope

_Returns a minimal bounding box polygon enclosing the input geometry_

- __GEOMETRY__ ST_MakeEnvelope(col0 __DOUBLE__, col1 __DOUBLE__, col2 __DOUBLE__, col3 __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_MakeLine

_Creates a LINESTRING geometry from a pair or list of input points_

- __GEOMETRY__ ST_MakeLine(points __GEOMETRY[]__)
- __GEOMETRY__ ST_MakeLine(start_point __GEOMETRY__, end_point __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_MakePolygon

_Creates a polygon from a shell geometry and an optional set of holes_

- __GEOMETRY__ ST_MakePolygon(shell __GEOMETRY__, holes __GEOMETRY[]__)
- __GEOMETRY__ ST_MakePolygon(shell __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Normalize

_Returns a "normalized" version of the input geometry._

- __GEOMETRY__ ST_Normalize(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_NumGeometries

_Returns the number of component geometries in a collection geometry_

- __INTEGER__ ST_NumGeometries(geom __GEOMETRY__)


__aliases:__ st_ngeometries
### Description

TODO

### Examples

TODO

## ST_NumInteriorRings

_Returns the number if interior rings of a polygon_

- __INTEGER__ ST_NumInteriorRings(polygon __POLYGON_2D__)
- __INTEGER__ ST_NumInteriorRings(geom __GEOMETRY__)


__aliases:__ st_ninteriorrings
### Description

TODO

### Examples

TODO

## ST_NumPoints

_Returns the number of points within a geometry_

- __UBIGINT__ ST_NumPoints(point __POINT_2D__)
- __UBIGINT__ ST_NumPoints(line __LINESTRING_2D__)
- __UBIGINT__ ST_NumPoints(polygon __POLYGON_2D__)
- __UBIGINT__ ST_NumPoints(geom __BOX_2D__)
- __UINTEGER__ ST_NumPoints(geom __GEOMETRY__)


__aliases:__ st_npoints
### Description

TODO

### Examples

TODO

## ST_Overlaps

_Returns true if geom1 "overlaps" geom2_

- __BOOLEAN__ ST_Overlaps(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Perimeter

_Returns the length of the perimeter of the geometry_

- __DOUBLE__ ST_Perimeter(box __BOX_2D__)
- __DOUBLE__ ST_Perimeter(geom __POLYGON_2D__)
- __DOUBLE__ ST_Perimeter(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Perimeter_Spheroid

_Returns the length of the perimeter in meters using an ellipsoidal model of the earths surface_

- __DOUBLE__ ST_Perimeter_Spheroid(polygon __POLYGON_2D__)
- __DOUBLE__ ST_Perimeter_Spheroid(geom __GEOMETRY__)

### Description

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the length is returned in meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library, calculating the perimeter using an ellipsoidal model of the earth. This is a highly accurate method for calculating the perimeter of a polygon taking the curvature of the earth into account, but is also the slowest.

Returns `0.0` for any geometry that is not a `POLYGON`, `MULTIPOLYGON` or `GEOMETRYCOLLECTION` containing polygon geometries.

### Examples

TODO

## ST_Point

_Creates a GEOMETRY point_

- __GEOMETRY__ ST_Point(x __DOUBLE__, y __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_Point2D

_Creates a POINT_2D_

- __POINT_2D__ ST_Point2D(x __DOUBLE__, y __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_Point2DFromWKB

_Deserialize a POINT_2D from a WKB encoded geometry blob_

- __POINT_2D__ ST_Point2DFromWKB(blob __WKB_BLOB__)

### Description

TODO

### Examples

TODO

## ST_Point3D

_Creates a POINT_3D_

- __POINT_3D__ ST_Point3D(x __DOUBLE__, y __DOUBLE__, z __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_Point4D

_Creates a POINT_4D_

- __POINT_4D__ ST_Point4D(x __DOUBLE__, y __DOUBLE__, z __DOUBLE__, m __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_PointN

_Returns the n'th vertex from the input geometry as a point geometry_

- __GEOMETRY__ ST_PointN(geom __GEOMETRY__, n __INTEGER__)
- __POINT_2D__ ST_PointN(line __LINESTRING_2D__, n __INTEGER__)

### Description

TODO

### Examples

TODO

## ST_PointOnSurface

_Returns a point that is guaranteed to be on the surface of the input geometry. Sometimes a useful alternative to ST_Centroid._

- __GEOMETRY__ ST_PointOnSurface(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Polygon2DFromWKB

_Deserialize a POLYGON_2D from a WKB encoded blob_

- __POLYGON_2D__ ST_Polygon2DFromWKB(blob __WKB_BLOB__)

### Description

TODO

### Examples

TODO

## ST_QuadKey

_Computes a quadkey from a given lon/lat point._

- __VARCHAR__ ST_QuadKey(geom __GEOMETRY__, level __INTEGER__)
- __VARCHAR__ ST_QuadKey(longitude __DOUBLE__, latitude __DOUBLE__, level __INTEGER__)

### Description

Compute the [quadkey](https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system) for a given lon/lat point at a given level.
Note that the the parameter order is __longitude__, __latitude__.

`level` has to be between 1 and 23, inclusive.

The input coordinates will be clamped to the lon/lat bounds of the earth (longitude between -180 and 180, latitude between -85.05112878 and 85.05112878).

Throws for any geometry that is not a `POINT`

### Examples

```sql
SELECT ST_QuadKey(st_point(11.08, 49.45), 10);
-- 1333203202
```

## ST_ReducePrecision

_Returns the geometry with all vertices reduced to the target precision_

- __GEOMETRY__ ST_ReducePrecision(geom __GEOMETRY__, precision __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_RemoveRepeatedPoints

_Returns a new geometry with repeated points removed, optionally within a target distance of eachother._

- __LINESTRING_2D__ ST_RemoveRepeatedPoints(line __LINESTRING_2D__)
- __LINESTRING_2D__ ST_RemoveRepeatedPoints(line __LINESTRING_2D__, distance __DOUBLE__)
- __GEOMETRY__ ST_RemoveRepeatedPoints(geom __GEOMETRY__)
- __GEOMETRY__ ST_RemoveRepeatedPoints(geom __GEOMETRY__, distance __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_Reverse

_Returns a new version of the input geometry with the order of its vertices reversed_

- __GEOMETRY__ ST_Reverse(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Simplify

_Simplifies the input geometry by collapsing edges smaller than 'distance'_

- __GEOMETRY__ ST_Simplify(geom __GEOMETRY__, distance __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_SimplifyPreserveTopology

_Returns a simplified geometry but avoids creating invalid topologies_

- __GEOMETRY__ ST_SimplifyPreserveTopology(geom __GEOMETRY__, distance __DOUBLE__)

### Description

TODO

### Examples

TODO

## ST_StartPoint

_Returns the first point of a line geometry_

- __GEOMETRY__ ST_StartPoint(geom __GEOMETRY__)
- __POINT_2D__ ST_StartPoint(line __LINESTRING_2D__)

### Description

TODO

### Examples

TODO

## ST_Touches

_Returns true if geom1 "touches" geom2_

- __BOOLEAN__ ST_Touches(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Transform

_Transforms a geometry between two coordinate systems_

- __BOX_2D__ ST_Transform(box __BOX_2D__, source_crs __VARCHAR__, target_crs __VARCHAR__)
- __BOX_2D__ ST_Transform(box __BOX_2D__, source_crs __VARCHAR__, target_crs __VARCHAR__, always_xy __BOOLEAN__)
- __POINT_2D__ ST_Transform(point __POINT_2D__, source_crs __VARCHAR__, target_crs __VARCHAR__)
- __POINT_2D__ ST_Transform(point __POINT_2D__, source_crs __VARCHAR__, target_crs __VARCHAR__, always_xy __BOOLEAN__)
- __GEOMETRY__ ST_Transform(geom __GEOMETRY__, source_crs __VARCHAR__, target_crs __VARCHAR__)
- __GEOMETRY__ ST_Transform(geom __GEOMETRY__, source_crs __VARCHAR__, target_crs __VARCHAR__, always_xy __BOOLEAN__)

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

## ST_Union

_Returns the union of two geometries._

- __GEOMETRY__ ST_Union(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

Returns the union of two geometries.

### Examples

```sql
SELECT ST_AsText(
    ST_Union(
        ST_GeomFromText('POINT(1 2)'), 
        ST_GeomFromText('POINT(3 4)')
    )
);
----
MULTIPOINT (1 2, 3 4) 
```

## ST_Within

_Returns true if geom1 is "within" geom2_

- __BOOLEAN__ ST_Within(point __POINT_2D__, polygon __POLYGON_2D__)
- __BOOLEAN__ ST_Within(geom1 __GEOMETRY__, geom2 __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_X

_Returns the X coordinate of a point geometry, or NULL if not a point or empty_

- __DOUBLE__ ST_X(point __POINT_2D__)
- __DOUBLE__ ST_X(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_XMax

_Returns the maximum x coordinate of a geometry_

- __DOUBLE__ ST_XMax(box __BOX_2D__)
- __DOUBLE__ ST_XMax(point __POINT_2D__)
- __DOUBLE__ ST_XMax(line __LINESTRING_2D__)
- __DOUBLE__ ST_XMax(polygon __POLYGON_2D__)
- __DOUBLE__ ST_XMax(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_XMin

_Returns the minimum x coordinate of a geometry_

- __DOUBLE__ ST_XMin(box __BOX_2D__)
- __DOUBLE__ ST_XMin(point __POINT_2D__)
- __DOUBLE__ ST_XMin(line __LINESTRING_2D__)
- __DOUBLE__ ST_XMin(polygon __POLYGON_2D__)
- __DOUBLE__ ST_XMin(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Y

_Returns the y coordinate of a point geometry, or NULL if not a point or empty._

- __DOUBLE__ ST_Y(point __POINT_2D__)
- __DOUBLE__ ST_Y(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_YMax

_Returns the maximum y coordinate of a geometry_

- __DOUBLE__ ST_YMax(box __BOX_2D__)
- __DOUBLE__ ST_YMax(point __POINT_2D__)
- __DOUBLE__ ST_YMax(line __LINESTRING_2D__)
- __DOUBLE__ ST_YMax(polygon __POLYGON_2D__)
- __DOUBLE__ ST_YMax(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_YMin

_Returns the minimum y coordinate of a geometry_

- __DOUBLE__ ST_YMin(box __BOX_2D__)
- __DOUBLE__ ST_YMin(point __POINT_2D__)
- __DOUBLE__ ST_YMin(line __LINESTRING_2D__)
- __DOUBLE__ ST_YMin(polygon __POLYGON_2D__)
- __DOUBLE__ ST_YMin(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

# Aggregate Functions

| Name | Summary |
| ---- | ----------- |
| [ST_Envelope_Agg](##st_envelope_agg) | Computes a minimal-bounding-box polygon 'enveloping' the set of input geometries |
| [ST_Intersection_Agg](##st_intersection_agg) | Computes the intersection of a set of geometries |
| [ST_Union_Agg](##st_union_agg) | Computes the union of a set of input geometries |

## ST_Envelope_Agg

_Computes a minimal-bounding-box polygon 'enveloping' the set of input geometries_

- __GEOMETRY__ ST_Envelope_Agg(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Intersection_Agg

_Computes the intersection of a set of geometries_

- __GEOMETRY__ ST_Intersection_Agg(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

## ST_Union_Agg

_Computes the union of a set of input geometries_

- __GEOMETRY__ ST_Union_Agg(geom __GEOMETRY__)

### Description

TODO

### Examples

TODO

# Table Functions

## ST_Drivers

_Returns the list of supported GDAL drivers_

- ST_Drivers()

### Description

Returns the list of supported GDAL drivers and file formats for [ST_Read](##st_read).

Note that far from all of these drivers have been tested properly, and some may require additional options to be passed to work as expected. If you run into any issues please first consult the [consult the GDAL docs](https://gdal.org/drivers/vector/index.html).

### Examples

```sql
SELECT * FROM ST_Drivers();
```

|   short_name   |                      long_name                       | can_create | can_copy | can_open |                      help_url                      |
|----------------|------------------------------------------------------|------------|----------|----------|----------------------------------------------------|
| ESRI Shapefile | ESRI Shapefile                                       | true       | false    | true     | https://gdal.org/drivers/vector/shapefile.html     |
| MapInfo File   | MapInfo File                                         | true       | false    | true     | https://gdal.org/drivers/vector/mitab.html         |
| UK .NTF        | UK .NTF                                              | false      | false    | true     | https://gdal.org/drivers/vector/ntf.html           |
| LVBAG          | Kadaster LV BAG Extract 2.0                          | false      | false    | true     | https://gdal.org/drivers/vector/lvbag.html         |
| S57            | IHO S-57 (ENC)                                       | true       | false    | true     | https://gdal.org/drivers/vector/s57.html           |
| DGN            | Microstation DGN                                     | true       | false    | true     | https://gdal.org/drivers/vector/dgn.html           |
| OGR_VRT        | VRT - Virtual Datasource                             | false      | false    | true     | https://gdal.org/drivers/vector/vrt.html           |
| Memory         | Memory                                               | true       | false    | true     |                                                    |
| CSV            | Comma Separated Value (.csv)                         | true       | false    | true     | https://gdal.org/drivers/vector/csv.html           |
| GML            | Geography Markup Language (GML)                      | true       | false    | true     | https://gdal.org/drivers/vector/gml.html           |
| GPX            | GPX                                                  | true       | false    | true     | https://gdal.org/drivers/vector/gpx.html           |
| KML            | Keyhole Markup Language (KML)                        | true       | false    | true     | https://gdal.org/drivers/vector/kml.html           |
| GeoJSON        | GeoJSON                                              | true       | false    | true     | https://gdal.org/drivers/vector/geojson.html       |
| GeoJSONSeq     | GeoJSON Sequence                                     | true       | false    | true     | https://gdal.org/drivers/vector/geojsonseq.html    |
| ESRIJSON       | ESRIJSON                                             | false      | false    | true     | https://gdal.org/drivers/vector/esrijson.html      |
| TopoJSON       | TopoJSON                                             | false      | false    | true     | https://gdal.org/drivers/vector/topojson.html      |
| OGR_GMT        | GMT ASCII Vectors (.gmt)                             | true       | false    | true     | https://gdal.org/drivers/vector/gmt.html           |
| GPKG           | GeoPackage                                           | true       | true     | true     | https://gdal.org/drivers/vector/gpkg.html          |
| SQLite         | SQLite / Spatialite                                  | true       | false    | true     | https://gdal.org/drivers/vector/sqlite.html        |
| WAsP           | WAsP .map format                                     | true       | false    | true     | https://gdal.org/drivers/vector/wasp.html          |
| OpenFileGDB    | ESRI FileGDB                                         | true       | false    | true     | https://gdal.org/drivers/vector/openfilegdb.html   |
| DXF            | AutoCAD DXF                                          | true       | false    | true     | https://gdal.org/drivers/vector/dxf.html           |
| CAD            | AutoCAD Driver                                       | false      | false    | true     | https://gdal.org/drivers/vector/cad.html           |
| FlatGeobuf     | FlatGeobuf                                           | true       | false    | true     | https://gdal.org/drivers/vector/flatgeobuf.html    |
| Geoconcept     | Geoconcept                                           | true       | false    | true     |                                                    |
| GeoRSS         | GeoRSS                                               | true       | false    | true     | https://gdal.org/drivers/vector/georss.html        |
| VFK            | Czech Cadastral Exchange Data Format                 | false      | false    | true     | https://gdal.org/drivers/vector/vfk.html           |
| PGDUMP         | PostgreSQL SQL dump                                  | true       | false    | false    | https://gdal.org/drivers/vector/pgdump.html        |
| OSM            | OpenStreetMap XML and PBF                            | false      | false    | true     | https://gdal.org/drivers/vector/osm.html           |
| GPSBabel       | GPSBabel                                             | true       | false    | true     | https://gdal.org/drivers/vector/gpsbabel.html      |
| WFS            | OGC WFS (Web Feature Service)                        | false      | false    | true     | https://gdal.org/drivers/vector/wfs.html           |
| OAPIF          | OGC API - Features                                   | false      | false    | true     | https://gdal.org/drivers/vector/oapif.html         |
| EDIGEO         | French EDIGEO exchange format                        | false      | false    | true     | https://gdal.org/drivers/vector/edigeo.html        |
| SVG            | Scalable Vector Graphics                             | false      | false    | true     | https://gdal.org/drivers/vector/svg.html           |
| ODS            | Open Document/ LibreOffice / OpenOffice Spreadsheet  | true       | false    | true     | https://gdal.org/drivers/vector/ods.html           |
| XLSX           | MS Office Open XML spreadsheet                       | true       | false    | true     | https://gdal.org/drivers/vector/xlsx.html          |
| Elasticsearch  | Elastic Search                                       | true       | false    | true     | https://gdal.org/drivers/vector/elasticsearch.html |
| Carto          | Carto                                                | true       | false    | true     | https://gdal.org/drivers/vector/carto.html         |
| AmigoCloud     | AmigoCloud                                           | true       | false    | true     | https://gdal.org/drivers/vector/amigocloud.html    |
| SXF            | Storage and eXchange Format                          | false      | false    | true     | https://gdal.org/drivers/vector/sxf.html           |
| Selafin        | Selafin                                              | true       | false    | true     | https://gdal.org/drivers/vector/selafin.html       |
| JML            | OpenJUMP JML                                         | true       | false    | true     | https://gdal.org/drivers/vector/jml.html           |
| PLSCENES       | Planet Labs Scenes API                               | false      | false    | true     | https://gdal.org/drivers/vector/plscenes.html      |
| CSW            | OGC CSW (Catalog  Service for the Web)               | false      | false    | true     | https://gdal.org/drivers/vector/csw.html           |
| VDV            | VDV-451/VDV-452/INTREST Data Format                  | true       | false    | true     | https://gdal.org/drivers/vector/vdv.html           |
| MVT            | Mapbox Vector Tiles                                  | true       | false    | true     | https://gdal.org/drivers/vector/mvt.html           |
| NGW            | NextGIS Web                                          | true       | true     | true     | https://gdal.org/drivers/vector/ngw.html           |
| MapML          | MapML                                                | true       | false    | true     | https://gdal.org/drivers/vector/mapml.html         |
| PMTiles        | ProtoMap Tiles                                       | true       | false    | true     | https://gdal.org/drivers/vector/pmtiles.html       |
| JSONFG         | OGC Features and Geometries JSON                     | true       | false    | true     | https://gdal.org/drivers/vector/jsonfg.html        |
| TIGER          | U.S. Census TIGER/Line                               | false      | false    | true     | https://gdal.org/drivers/vector/tiger.html         |
| AVCBin         | Arc/Info Binary Coverage                             | false      | false    | true     | https://gdal.org/drivers/vector/avcbin.html        |
| AVCE00         | Arc/Info E00 (ASCII) Coverage                        | false      | false    | true     | https://gdal.org/drivers/vector/avce00.html        |

## ST_Read

_Import data from a variety of geospatial file formats_

- ST_Read(path __VARCHAR__, sequential_layer_scan __BOOLEAN__, max_batch_size __INTEGER__, keep_wkb __BOOLEAN__, layer __VARCHAR__, allowed_drivers __VARCHAR[]__, spatial_filter __WKB_BLOB__, spatial_filter_box __BOX_2D__, sibling_files __VARCHAR[]__, open_options __VARCHAR[]__)

### Description


The `ST_Read` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading spatial data from a variety of geospatial vector file formats as if they were DuckDB tables.

> See [ST_Drivers](##st_drivers) for a list of supported file formats and drivers.

Except for the `path` parameter, all parameters are optional.

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `path` | VARCHAR | The path to the file to read. Mandatory |
| `sequential_layer_scan` | BOOLEAN | If set to true, the table function will scan through all layers sequentially and return the first layer that matches the given layer name. This is required for some drivers to work properly, e.g., the OSM driver. |
| `spatial_filter` | WKB_BLOB | If set to a WKB blob, the table function will only return rows that intersect with the given WKB geometry. Some drivers may support efficient spatial filtering natively, in which case it will be pushed down. Otherwise the filtering is done by GDAL which may be much slower. |
| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. E.g., the GeoJSON driver supports a FLATTEN_NESTED_ATTRIBUTES=YES option to flatten nested attributes. |
| `layer` | VARCHAR | The name of the layer to read from the file. If NULL, the first layer is returned. Can also be a layer index (starting at 0). |
| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. |
| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. E.g., the ESRI Shapefile driver requires a .shx file to be present. Although most of the time these can be discovered automatically. |
| `spatial_filter_box` | BOX_2D | If set to a BOX_2D, the table function will only return rows that intersect with the given bounding box. Similar to spatial_filter. |
| `keep_wkb` | BOOLEAN | If set, the table function will return geometries in a wkb_geometry column with the type WKB_BLOB (which can be cast to BLOB) instead of GEOMETRY. This is useful if you want to use DuckDB with more exotic geometry subtypes that DuckDB spatial doesnt support representing in the GEOMETRY type yet. |

Note that GDAL is single-threaded, so this table function will not be able to make full use of parallelism.

### Examples

```sql
-- Read a Shapefile
SELECT * FROM ST_Read('some/file/path/filename.shp');

-- Read a GeoJSON file
CREATE TABLE my_geojson_table AS SELECT * FROM ST_Read('some/file/path/filename.json');

```

### Replacement scans

By using `ST_Read`, the spatial extension also provides “replacement scans” for common geospatial file formats, allowing you to query files of these formats as if they were tables directly.

```sql
SELECT * FROM './path/to/some/shapefile/dataset.shp';
```

In practice this is just syntax-sugar for calling ST_Read, so there is no difference in performance. If you want to pass additional options, you should use the ST_Read table function directly.

The following formats are currently recognized by their file extension:

| Format | Extension |
| ------ | --------- |
| ESRI ShapeFile | .shp |
| GeoPackage | .gpkg |
| FlatGeoBuf | .fgb |

## ST_ReadOSM

_Reads compressed OpenStreetMap data_

- ST_ReadOSM(path __VARCHAR__)

### Description

The ST_ReadOsm() table function enables reading compressed OpenStreetMap data directly from a `.osm.pbf file.`

This function uses multithreading and zero-copy protobuf parsing which makes it a lot faster than using the `ST_Read()` OSM driver, however it only outputs the raw OSM data (Nodes, Ways, Relations), without constructing any geometries. For simple node entities (like PoI's) you can trivially construct POINT geometries, but it is also possible to construct LINESTRING and POLYGON geometries by manually joining refs and nodes together in SQL, although with available memory usually being a limiting factor.

### Examples

```sql
SELECT *
FROM ST_ReadOSM('tmp/data/germany.osm.pbf')
WHERE tags['highway'] != []
LIMIT 5;
```

```
┌──────────────────────┬────────┬──────────────────────┬─────────┬────────────────────┬────────────┬───────────┬────────────────────────┐
│         kind         │   id   │         tags         │  refs   │        lat         │    lon     │ ref_roles │       ref_types        │
│ enum('node', 'way'…  │ int64  │ map(varchar, varch…  │ int64[] │       double       │   double   │ varchar[] │ enum('node', 'way', …  │
├──────────────────────┼────────┼──────────────────────┼─────────┼────────────────────┼────────────┼───────────┼────────────────────────┤
│ node                 │ 122351 │ {bicycle=yes, butt…  │         │         53.5492951 │   9.977553 │           │                        │
│ node                 │ 122397 │ {crossing=no, high…  │         │ 53.520990100000006 │ 10.0156924 │           │                        │
│ node                 │ 122493 │ {TMC:cid_58:tabcd_…  │         │ 53.129614600000004 │  8.1970173 │           │                        │
│ node                 │ 123566 │ {highway=traffic_s…  │         │ 54.617268200000005 │  8.9718171 │           │                        │
│ node                 │ 125801 │ {TMC:cid_58:tabcd_…  │         │ 53.070685000000005 │  8.7819939 │           │                        │
└──────────────────────┴────────┴──────────────────────┴─────────┴────────────────────┴────────────┴───────────┴────────────────────────┘
```

The `ST_ReadOSM()` function also provides a "replacement scan" to enable reading from a file directly as if it were a table. This is just syntax sugar for calling `ST_ReadOSM()` though. Example:
    
```sql
SELECT * FROM 'tmp/data/germany.osm.pbf' LIMIT 5;
```

# Functions by tag

__construction__
- [ST_Envelope_Agg](##st_envelope_agg)
- [ST_Intersection_Agg](##st_intersection_agg)
- [ST_Union_Agg](##st_union_agg)
- [ST_Boundary](##st_boundary)
- [ST_Collect](##st_collect)
- [ST_CollectionExtract](##st_collectionextract)
- [ST_Difference](##st_difference)
- [ST_Dump](##st_dump)
- [ST_Envelope](##st_envelope)
- [ST_ExteriorRing](##st_exteriorring)
- [ST_FlipCoordinates](##st_flipcoordinates)
- [ST_LineMerge](##st_linemerge)
- [ST_MakeLine](##st_makeline)
- [ST_MakePolygon](##st_makepolygon)
- [ST_Normalize](##st_normalize)
- [ST_Point](##st_point)
- [ST_Point2D](##st_point2d)
- [ST_Point3D](##st_point3d)
- [ST_Point4D](##st_point4d)
- [ST_PointN](##st_pointn)
- [ST_PointOnSurface](##st_pointonsurface)
- [ST_RemoveRepeatedPoints](##st_removerepeatedpoints)
- [ST_Reverse](##st_reverse)
- [ST_SimplifyPreserveTopology](##st_simplifypreservetopology)
- [ST_StartPoint](##st_startpoint)
- [ST_Transform](##st_transform)

__property__
- [ST_Area](##st_area)
- [ST_Area_Spheroid](##st_area_spheroid)
- [ST_Centroid](##st_centroid)
- [ST_Dimension](##st_dimension)
- [ST_Distance](##st_distance)
- [ST_EndPoint](##st_endpoint)
- [ST_GeometryType](##st_geometrytype)
- [ST_Intersects](##st_intersects)
- [ST_IsClosed](##st_isclosed)
- [ST_IsEmpty](##st_isempty)
- [ST_IsRing](##st_isring)
- [ST_IsSimple](##st_issimple)
- [ST_IsValid](##st_isvalid)
- [ST_Length](##st_length)
- [ST_Length_Spheroid](##st_length_spheroid)
- [ST_NumGeometries](##st_numgeometries)
- [ST_NumInteriorRings](##st_numinteriorrings)
- [ST_NumPoints](##st_numpoints)
- [ST_Perimeter](##st_perimeter)
- [ST_Perimeter_Spheroid](##st_perimeter_spheroid)
- [ST_QuadKey](##st_quadkey)
- [ST_X](##st_x)
- [ST_XMax](##st_xmax)
- [ST_XMin](##st_xmin)
- [ST_Y](##st_y)
- [ST_YMax](##st_ymax)
- [ST_YMin](##st_ymin)

__spheroid__
- [ST_Area_Spheroid](##st_area_spheroid)
- [ST_Distance_Spheroid](##st_distance_spheroid)
- [ST_Length_Spheroid](##st_length_spheroid)
- [ST_Perimeter_Spheroid](##st_perimeter_spheroid)

__conversion__
- [ST_AsGeoJSON](##st_asgeojson)
- [ST_AsHEXWKB](##st_ashexwkb)
- [ST_AsText](##st_astext)
- [ST_AsWKB](##st_aswkb)
- [ST_GeomFromGeoJSON](##st_geomfromgeojson)
- [ST_GeomFromHEXEWKB](##st_geomfromhexewkb)
- [ST_GeomFromHEXWKB](##st_geomfromhexwkb)
- [ST_GeomFromText](##st_geomfromtext)
- [ST_GeomFromWKB](##st_geomfromwkb)
- [ST_LineString2DFromWKB](##st_linestring2dfromwkb)
- [ST_Point2DFromWKB](##st_point2dfromwkb)
- [ST_Polygon2DFromWKB](##st_polygon2dfromwkb)

__relation__
- [ST_Contains](##st_contains)
- [ST_ContainsProperly](##st_containsproperly)
- [ST_CoveredBy](##st_coveredby)
- [ST_Covers](##st_covers)
- [ST_Crosses](##st_crosses)
- [ST_Distance_Spheroid](##st_distance_spheroid)
- [ST_DWithin](##st_dwithin)
- [ST_DWithin_Spheroid](##st_dwithin_spheroid)
- [ST_Intersection](##st_intersection)
- [ST_Intersects_Extent](##st_intersects_extent)
- [ST_Overlaps](##st_overlaps)
- [ST_Touches](##st_touches)
- [ST_Within](##st_within)

