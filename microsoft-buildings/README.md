
# Microsoft U.S. Buliding Footprints

The [Microsoft U.S. Building Footprints](https://github.com/microsoft/USBuildingFootprints) data set is a collection of ~130 million polygon outlines of buildings derived from satellite imagery. The version of the data set in this repository is repackaged as several files and is compressed using ZSTD compression to minimize file size and facilitate downloading. A version of the data set as a point (polygon centroids) is also provided. Features are sorted by Hilbert code and written in row groups that contain ~70 MB of coordinates each. In addition to geometry, `src_file_index` (integer from 0 to 50 identifying the source .geojson index) and `src_index` (index within the source file) attribures are provided to identify features.

The original data is licensed under the [Open Data Commons Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/) and is made available here under the same license.

<!-- begin file listing -->


## Data (v0.1.0)

- point ([fgb/zip](https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/microsoft-buildings-point.fgb.zip), [arrow](https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/microsoft-buildings-point.arrow), [arrow/interleaved](https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/microsoft-buildings-point-interleaved.arrow))
