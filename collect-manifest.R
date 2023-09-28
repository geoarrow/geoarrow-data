
library(tidyverse)

tag <- "v0.1.0"

files <- tibble(
  group = c("example", "ns-water", "microsoft-buildings"),
  manifest_file = glue::glue("{group}/manifest.yaml"),
  manifest = lapply(manifest_file, yaml::read_yaml)
) |>
  unnest_wider(manifest, names_sep = "_") |>
  unnest_longer(manifest_files) |>
  unnest_wider(manifest_files, names_sep = "_") |>
  mutate(
    manifest_files_format = coalesce(manifest_files_format, manifest_format),
  ) |>
  select(
    group,
    name = manifest_files_name,
    format = manifest_files_format,
    file_location = manifest_file_location
  )

files_main <- files |>
  unnest_longer(format) |>
  mutate(
    tag = tag,
    release = if_else(tag == "main", "latest-dev", tag),
    prefixed_name = if_else(name == group, name, paste0(group, "-", name)),
    format_postfix = case_when(
      format == "gpkg" ~ ".gpkg",
      format == "fgb/zip" ~ ".fgb.zip",
      format == "arrow" ~ ".arrow",
      format == "arrow/interleaved" ~ "-interleaved.arrow",
      format == "arrow/wkb" ~ "-wkb.arrow",
      format == "arrow/wkt" ~ "-wkt.arrow"
    ),
    file_name = paste0(prefixed_name, format_postfix),
    url = if_else(
      file_location == "release",
      glue::glue("https://github.com/geoarrow/geoarrow-data/releases/download/{release}/{file_name}"),
      glue::glue("https://raw.githubusercontent.com/geoarrow/geoarrow-data/{tag}/{group}/{file_name}")
    )
  ) |>
  select(
    group,
    name,
    format,
    file_location,
    tag,
    url
  )

groups_collected <- files_main |>
  mutate(
    group = as_factor(group)
  ) |>
  group_by(group, tag) |>
  summarise(
    files = list(transpose(tibble::tibble(name, format, file_location, url)))
  ) |>
  ungroup() |>
  mutate(group = as.character(group))

groups_collected |>
  transpose() |>
  yaml::write_yaml("manifest.yaml")
