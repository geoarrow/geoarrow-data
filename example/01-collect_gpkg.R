library(wk)
library(sf)

# Don't use nc or geometrycollection
examples_wk_wkt <- wk::wk_example_wkt
examples_wk_wkt <-
  examples_wk_wkt[!grepl("geometrycollection", names(examples_wk_wkt))]

yaml::write_yaml(examples_wk_wkt, "example_src.yaml")


for (nm in names(examples_wk_wkt)) {
  df <- data.frame(row_number = seq_along(examples_wk_wkt[[nm]]))
  df$geometry <- sf::st_as_sfc(examples_wk_wkt[[nm]])
  sf <- sf::st_as_sf(df)
  sf::write_sf(sf, glue::glue("example/example-{nm}.gpkg"))
}

# write manifest.yaml
list(
  group = "example",
  format = c(
    "gpkg", "arrow", "arrow/interleaved", "arrow/wkt", "arrow/wkb"
  ),
  file_location = "repo",
  files = lapply(names(examples_wk_wkt), function(x) list(name = x))
) |>
  yaml::write_yaml("example/manifest.yaml")
