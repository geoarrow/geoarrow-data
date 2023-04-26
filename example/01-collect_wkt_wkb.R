
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

geo_metadata_template_wkb <- '{
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

geo_metadata_template_wkt <- '{
  "columns": {
    "geometry": {
      "encoding": "geoarrow.wkt",
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
  meta0 <- wk::wk_meta(examples_wk_wkt[[nm]])[1, , drop = FALSE]
  dims <- if (meta0$has_z && meta0$has_m) {
    " ZM"
  } else if (meta0$has_z) {
    " Z"
  } else if (meta0$has_m) {
    " M"
  } else {
    ""
  }

  geometry_type <- paste0(geometry_type_map[meta0$geometry_type], dims)

  wkt_schema$metadata$geo <- sprintf(geo_metadata_template_wkt, geometry_type)
  wkb_schema$metadata$geo <- sprintf(geo_metadata_template_wkb, geometry_type)

  table_wkt <- Table$create(geometry = examples_wkt[[nm]], schema = wkt_schema)
  table_wkb <- Table$create(geometry = examples_wkb[[nm]], schema = wkb_schema)

  write_parquet(
    table_wkt,
    glue::glue("example/{nm}-wkt.parquet"),
    compression = "uncompressed"
  )
  write_parquet(
    table_wkb,
    glue::glue("example/{nm}-wkb.parquet"),
    compression = "uncompressed"
  )
}
