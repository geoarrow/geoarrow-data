
library(tidyverse)
library(arrow)

parquet_size_hint <- 2 ^ 30

parquets <- list.files(
  "microsoft-buildings/by_state",
  ".parquet$",
  full.names = TRUE
) |>
  str_subset("_meta", negate = TRUE)

meta_parquets <- list.files(
  "microsoft-buildings/by_state",
  "_meta.parquet$",
  full.names = TRUE
) |>
  str_subset("00", negate = TRUE)

meta_reader <- ParquetFileReader$create(
  "microsoft-buildings/by_state/00hilbert_meta.parquet"
)

points_schema <- schema(
  src_file_index = int32(),
  src_index = int32(),
  geometry = struct(x = float64(), y = float64())
) |>
  nanoarrow::as_nanoarrow_schema()

points_schema$children$geometry$metadata[["ARROW:extension:name"]] <-
  "geoarrow.point"
points_schema$children$geometry$metadata[["ARROW:extension:metadata"]] <-
  sprintf('{"crs":%s}', wk::wk_crs_projjson(wk::wk_crs_longlat()))
points_schema <- as_schema(points_schema)

points_i <- 0L
points_stream <- NULL
points_writer <- NULL

reinit_points_writer <- function() {
  if (!is.null(points_stream)) {
    points_writer$Close()
  }

  if (!is.null(points_stream)) {
    points_stream$close()
  }

  points_i <<- points_i + 1L

  points_stream <<- LocalFileSystem$create()$OpenOutputStream(
    sprintf(
      "microsoft-buildings/microsoft-buildings-point-%d.parquet",
      points_i
    )
  )

  points_writer <<- ParquetFileWriter$create(
    points_schema,
    points_stream,
    properties = ParquetWriterProperties$create(
      column_names = names(points_schema),
      compression = "zstd"
    )
  )
}

reinit_points_writer()

pb <- progress::progress_bar$new(total = meta_reader$num_row_groups)
for (i in seq_len(meta_reader$num_row_groups) - 1L) {
  pb$tick()

  group <- as.data.frame(meta_reader$ReadRowGroup(i, 0:1))
  group$file <- meta_parquets[group$file_index + 1]
  group$dst_index <- seq_len(nrow(group)) - 1L
  n_distinct(group$file)

  out <- group |>
    reframe(
      dst_index,
      src_file_index = file_index,
      src_index = index,
      as.data.frame(
        read_parquet(
          file[1],
          as_data_frame = FALSE
        )[index + 1L, "lonlat", drop = FALSE]
      ),
      .by = file
    ) |>
    arrange(dst_index) |>
    select(dst_index, src_file_index, src_index, geometry = lonlat)

  stopifnot(identical(out$dst_index, group$dst_index))
  out$dst_index <- NULL

  points_writer$WriteTable(as_arrow_table(out, points_schema), nrow(out))

  if (points_stream$tell() >= parquet_size_hint) {
    reinit_points_writer()
  }
}

points_writer$Close()
points_stream$close()
