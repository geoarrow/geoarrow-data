
# CRS Examples

The data here is a single polygon representing the out line of the state of Vermont (as sourced from [Natural Earth](https://www.naturalearthdata.com/) level 1 administrative boundary data) with various coordinate reference systems (CRS) and various CRS representations representations. The CRS values used are:

- **OGC:CRS84**: Longitude/latitude on the WGS84 ellipsoid with the axis order of the coordinates specified as such.
- **EPSG:4326**: Latitude/longitude on the WGS84 ellipsoid. While the stated axis order according to the CRS definition in latitude, longitude; the GeoArrow and GeoParquet standards requires that consumers and producers write longitude, latitude. This is sometimes called "natural" or "traditional GIS" order and is the most common way that data declared as EPSG:4326 is found in the wild.
- **UTM Zone 18N** (i.e., EPSG:32620)
- **Custom orthographic**: An orthographc projection centred on the centroid of the state of Vermont that does not have any identifier in any database.

The OGC:CRS84 version of the data is also included using the CRS representations stated in the GeoArrow specification (`wkt2:2019`, `authority_code`, and omitted to indicate that the producer did not know what the CRS encoding was).

All versions of Natural Earth raster + vector map data redistributed from this repository are in the public domain.

<!-- begin file listing -->
