
# Microsoft U.S. Buliding Footprints

The [Microsoft U.S. Building Footprints](https://github.com/microsoft/USBuildingFootprints) data set is a collection of ~130 million polygon outlines of buildings derived from satellite imagery. The version of the data set in this repository is repackaged as several files and is compressed using ZSTD compression to minimize file size and facilitate downloading. A version of the data set as a point (polygon centroids) is also provided. Features are sorted by Hilbert code and written in row groups that contain ~70 MB of coordinates each. In addition to geometry, `src_file_index` (integer from 0 to 50 identifying the source .geojson index) and `src_index` (index within the source file) attribures are provided to identify features.

The original data is licensed under the [Open Data Commons Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/) and is made available here under the same license.

<!-- begin file listing -->

## Data (latest-dev)

Building centroids (point):

- microsoft-buildings-1 ([parquet/point](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-point-1.parquet), [parquet/interleaved point](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-point-1.parquet))
- microsoft-buildings-2 ([parquet/point](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-point-2.parquet), [parquet/interleaved point](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-point-2.parquet))

Building footprints (polygon):

- microsoft-buildings-1 ([parquet/wkb](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-wkb-1.parquet), [parquet/polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-polygon-1.parquet), [parquet/interleaved polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-polygon-1.parquet))
- microsoft-buildings-2 ([parquet/wkb](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-wkb-2.parquet), [parquet/polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-polygon-2.parquet), [parquet/interleaved polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-polygon-2.parquet))
- microsoft-buildings-3 ([parquet/wkb](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-wkb-3.parquet), [parquet/polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-polygon-3.parquet), [parquet/interleaved polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-polygon-3.parquet))
- microsoft-buildings-4 ([parquet/wkb](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-wkb-4.parquet), [parquet/polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-polygon-4.parquet), [parquet/interleaved polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-polygon-4.parquet))
- microsoft-buildings-5 ([parquet/wkb](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-wkb-5.parquet), [parquet/polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-polygon-5.parquet), [parquet/interleaved polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-polygon-5.parquet))
- microsoft-buildings-6 ([parquet/wkb](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-wkb-6.parquet), [parquet/polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-polygon-6.parquet), [parquet/interleaved polygon](https://github.com/geoarrow/geoarrow-data/releases/download/latest-dev/microsoft-buildings-interleaved-polygon-6.parquet))
