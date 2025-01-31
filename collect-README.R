
library(tidyverse)
library(jsonlite)

manifest <- jsonlite::read_json("manifest.json")

files <- tibble::tibble(
  group_desc = manifest$groups
) |>
  unnest_wider(group_desc) |>
  unnest_longer(files) |>
  unnest_wider(files, names_sep = "_")

readme_content <- files |>
  mutate(
    group = as_factor(name),
    tag = manifest$ref,
    name = as_factor(files_name),
    format = as_factor(files_format),
    url = files_url
  ) |>
  mutate(
    md_link = glue::glue("[{format}]({url})")
  ) |>
  group_by(group, tag, name) |>
  summarise(
    link_summary = paste(md_link, collapse = ", "),
    bullet = glue::glue("- {name[1]} ({link_summary})"),
    .groups = "drop_last"
  ) |>
  select(-link_summary) |>
  summarise(
    file_listing = paste(bullet, collapse = "\n"),
    file_summary = glue::glue("## Files\n\n{file_listing}"),
    .groups = "drop_last"
  ) |>
  select(-file_listing) |>
  summarise(
    file_summary = paste0(file_summary, collapse = "\n\n"),
    .groups = "drop"
  ) |>
  mutate(readme_file = glue::glue("{group}/README.md"))

readme_content

for (i in seq_len(nrow(readme_content))) {
  f <- readme_content$readme_file[i]
  content <- readr::read_lines(f)
  target <- which(content == "<!-- begin file listing -->")
  stopifnot(length(target) == 1L)

  readr::write_lines(content[1:target], f)
  readr::write_lines("\n", f, append = TRUE)
  readr::write_lines(readme_content$file_summary[i], f, append = TRUE)
}
