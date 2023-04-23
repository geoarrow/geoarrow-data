
library(tidyverse)
library(furrr)
library(arrow)
plan(multisession)

parquets <- list.files(
  "microsoft-building-footprints/by_state",
  ".parquet$",
  full.names = TRUE
) |>
  str_subset("_meta", negate = TRUE)

chunk_apply <- function(wkb_arrow, f = identity, ..., chunk_size = 65536) {
  chunks <- wk::wk_chunk_strategy_feature(chunk_size = chunk_size)(NULL, wkb_arrow$length())
  out <- vector("list", nrow(chunks))
  for (i in seq_len(nrow(chunks))) {
    chunk_arrow <- wkb_arrow$Slice(chunks$from[i] - 1, chunks$to[i] - chunks$from[i] + 1)

    blob <- chunk_arrow |>
      nanoarrow::as_nanoarrow_array() |>
      nanoarrow::convert_array()
    attributes(blob) <- NULL
    wkb <- wk::new_wk_wkb(blob, crs = wk::wk_crs_longlat())
    out[[i]] <- f(wkb, ...)
  }

  out
}

# We're going to do a huge spatial sort here by hilbert code! To do it we need
# a full-on bbox of the whole dataset. Thankfully, the dataset doesn't contain
# buildings to the west of 180 in Alaska
bboxes_by_state <- future_map(parquets, function(f) {
  wkb_arrow <- read_parquet(f, col_select = "geometry", as_data_frame = FALSE)[[1]]
  chunk_bboxes <- chunk_apply(wkb_arrow, wk::wk_bbox)
  wk::wk_bbox(vctrs::vec_c(!!!chunk_bboxes))
}, .progress = TRUE)

extent <- wk::wk_bbox(vctrs::vec_c(!!!bboxes_by_state))
bboxes_by_state <- NULL

# Generate a _meta.parquet version of each .parquet that contains some cached
# information about each building. In particular, the hilbert code is used
# to ensure that we can write row groups covering vagely similar areas.
future_walk(parquets, function(f) {
  wkb_arrow <- read_parquet(f, col_select = "geometry", as_data_frame = FALSE)[[1]]
  file_out <- stringr::str_replace(f, "\\.parquet", "_meta.parquet")
  stream <- LocalFileSystem$create()$OpenOutputStream(file_out)

  schema <- arrow::schema(
    file_index = arrow::int32(),
    index = arrow::int32(),
    hilbert_code = arrow::int32(),
    s2_cell = arrow::int64(),
    lonlat = arrow::struct(x = arrow::float64(), y = arrow::float64())
  )

  writer <- ParquetFileWriter$create(
    schema,
    stream,
    properties = ParquetWriterProperties$create(
      column_names = names(schema),
      compression = "zstd",
      write_statistics = FALSE
    )
  )
  on.exit(try({
    writer$Close()
    stream$close()
  }))

  i_start <- 0L
  chunk_apply(wkb_arrow, function(x) {
    geos <- geos::as_geos_geometry(x)
    hilbert_code <- geos::geos_hilbert_code(geos, extent = extent)
    pt <- geos::geos_point_on_surface(geos)
    lonlat <- s2::as_s2_lnglat(pt)
    cell <- bit64::as.integer64(s2::as_s2_cell(lonlat))

    table <- arrow::arrow_table(
      file_index = match(f, parquets) - 1L,
      index = i_start + seq_along(x) - 1L,
      hilbert_code = hilbert_code,
      s2_cell = cell,
      lonlat = as.data.frame(lonlat)
    )

    i_start <<- i_start + length(x)
    writer$WriteTable(table, length(x))
  })
}, .progress = TRUE)

meta_parquets <- list.files(
  "microsoft-building-footprints/by_state",
  "_meta.parquet$",
  full.names = TRUE
)

meta_dataset <- open_dataset(meta_parquets)

meta_hilbert <- meta_dataset |>
  arrange(hilbert_code) |>
  select(file_index, index, hilbert_code) |>
  collect()

arrow::write_parquet(
  meta_hilbert,
  "microsoft-building-footprints/by_state/00hilbert_meta.parquet",
  # Roughly 100 MB of point coordinates per chunk
  chunk_size = 65536
)
