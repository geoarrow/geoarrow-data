
library(arrow)

feature_codes <- readxl::read_excel("ns-water/nshn_v2/NSHN_FEATURECODES.xls")
arrow::write_parquet(
  feature_codes,
  "ns-water/ns-water-feature_codes.parquet",
  compression = "uncompressed"
)

file.copy(
  "ns-water/nshn_v2/NSHN Attribute_Specs.pdf",
  "ns-water/ns-water-attribute_specs.pdf"
)

list(
  group = "ns-water",
  format = c("gpkg", "parquet", "parquet/interleaved", "parquet/wkb"),
  file_location = "release",
  files = list.files("ns-water", "\\.gpkg$") |>
    stringr::str_remove("ns-water-") |>
    stringr::str_remove(".gpkg") |>
    lapply(function(x) list(name = x))
) |>
  yaml::write_yaml("ns-water/manifest.yaml")
