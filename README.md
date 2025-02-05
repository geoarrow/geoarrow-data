
# geoarrow-data

This repository is a collection of recipies to make a few large-ish data sets available in [GeoParquet](https://github.com/opengeospatial/geoparquet)/[GeoArrow](https://github.com/geoarrow/geoarrow) format to facilitate testing, prototyping, and benchmarking implementations.

## [Examples](example#readme)

Toy example files covering the grid of geometry types (point, linestring, polygon, multipoint, multilinestring, multipolygon, geometrycollection), dimensions (xy, xyz, xym, xyzm), and encodings (wkt, wkb, struct coordinates, interleaved coordinates).

## [Coordinate Reference System Examples](example-crs#readme)

Example files containing the range of coordinate reference system encodings allowed by the GeoArrow specification, including files representing the same spatial content in several coordinate systems.

## [Nova Scotia Water](ns-water#readme)

A geoarrow port of the Nova Scotia Hydrometric Network. This is useful as an example of a projected and aligned set of layers with realisitc attributes.

## [Microsoft U.S. Buliding Footprints](microsoft-buildings#readme)

A geoarrow port of a large (130 million features) point data set.

## [Natural Earth](natural-earth#readme)

Selected [Natural Earth](https://www.naturalearthdata.com/) layers as examples of global data. Also includes examples with spherical edges and boxes that span the antimeridian.

## [Quadrangles](quadrangles#readme)

United States Geological Survey (USGS) map sheet outlines as real-world examples of the "box" type.
