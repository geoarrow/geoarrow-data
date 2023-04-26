
# geoarrow-data

This repository is a collection of recipies to make a few large-ish data sets available in [GeoParquet](https://github.com/opengeospatial/geoparquet)/[GeoArrow](https://github.com/geoarrow/geoarrow) format to facilitate testing, prototyping, and benchmarking implementations.

## [Examples](example)

Toy example files covering the grid of geometry types (point, linestring, polygon, multipoint, multilinestring, multipolygon, geometrycollection), dimensions (xy, xyz, xym, xyzm), and encodings (wkt, wkb, struct coordinates, interleaved coordinates).

## [Microsoft U.S. Buliding Footprints](microsoft-buildings)

A geoarrow port of a large (130 million features) polygon data set.

## [U.S. Zip Codes](us-zip-codes)

A geoarrow port of the U.S. Census Bureau's zip code polygons data set.
