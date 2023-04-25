
library(wk)
library(sf)
library(arrow)

examples_wk_wkt <- wk::wk_example_wkt[-1]

examples_wkt <- lapply(examples_wk_wkt, function(x) {
  attributes(x) <- NULL
  x
})

examples_wkb <- lapply(examples_wk_wkt, function(x) {
  out <-unclass(sf::st_as_binary(sf::st_as_sfc(x)))
  out[is.na(x)] <- list(NULL)
  out
})


wkb_schema <- schema(
  geometry = binary()
) |>
  nanoarrow::as_nanoarrow_schema()
wkb_schema$children$geometry$metadata[["ARROW:extension:name"]] <-
  "geoarrow.wkb"
wkb_schema <- as_schema(wkb_schema)

wkt_schema <- schema(
  geometry = utf8()
) |>
  nanoarrow::as_nanoarrow_schema()
wkt_schema$children$geometry$metadata[["ARROW:extension:name"]] <-
  "geoarrow.wkt"
wkt_schema <- as_schema(wkt_schema)

geo_metadata_template <- '{
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "crs": null,
      "geometry_types": ["%s"]
    }
  },
  "primary_column": "geometry",
  "version": "1.0.0-dev"
}'

geometry_type_map <- c(
  "Point", "LineString", "Polygon",
  "MultiPoint", "MultiLineString", "MultiPolygon",
  "GeometryCollection"
)

for (nm in names(examples_wk_wkt)) {
  geometry_type <- geometry_type_map[wk::wk_meta(examples_wk_wkt[[nm]])$geometry_type[1]]

  wkt_schema$metadata$geo <- sprintf(geo_metadata_template, geometry_type)
  wkb_schema$metadata$geo <- sprintf(geo_metadata_template, geometry_type)

  table_wkt <- Table$create(geometry = examples_wkt[[nm]], schema = wkt_schema)
  table_wkb <- Table$create(geometry = examples_wkb[[nm]], schema = wkb_schema)

  write_parquet(table_wkt, glue::glue("example/{nm}-wkt.parquet"), compression = "uncompressed")
  write_parquet(table_wkb, glue::glue("example/{nm}-wkb.parquet"),  compression = "uncompressed")
}
