
library(tidyverse)
library(arrow)

# We need files less than 2 GB to upload them as release assets; however,
# translating these into interleaved format makes them slightly bigger
# so we need wiggle room.
parquet_size_hint <- 1.5 * 2 ^ 30

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

wkb_schema <- schema(
  src_file_index = int32(),
  src_index = int32(),
  geometry = binary()
) |>
  nanoarrow::as_nanoarrow_schema()

points_schema$children$geometry$metadata[["ARROW:extension:name"]] <-
  "geoarrow.point"
points_schema$children$geometry$metadata[["ARROW:extension:metadata"]] <-
  sprintf('{"crs":%s}', wk::wk_crs_projjson(wk::wk_crs_longlat()))
points_schema <- as_schema(points_schema)
points_schema$metadata$geo <- '{
  "columns": {
    "geometry": {
      "encoding": "geoarrow.point",
      "geometry_types": ["Point"]
    }
  },
  "primary_column": "geometry",
  "version": "1.0.0-dev"
}'

wkb_schema$children$geometry$metadata[["ARROW:extension:name"]] <-
  "geoarrow.wkb"
wkb_schema$children$geometry$metadata[["ARROW:extension:metadata"]] <-
  sprintf('{"crs":%s}', wk::wk_crs_projjson(wk::wk_crs_longlat()))
wkb_schema <- as_schema(wkb_schema)
wkb_schema$metadata$geo <- '{
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Polygon"]
    }
  },
  "primary_column": "geometry",
  "version": "1.0.0-dev"
}'

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
      "microsoft-buildings/microsoft-buildings-point_%d.parquet",
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

wkb_i <- 0L
wkb_stream <- NULL
wkb_writer <- NULL

reinit_wkb_writer <- function() {
  if (!is.null(wkb_stream)) {
    wkb_writer$Close()
  }

  if (!is.null(wkb_stream)) {
    wkb_stream$close()
  }

  wkb_i <<- wkb_i + 1L

  wkb_stream <<- LocalFileSystem$create()$OpenOutputStream(
    sprintf(
      "microsoft-buildings/microsoft-buildings-polygon_%d-wkb.parquet",
      wkb_i
    )
  )

  wkb_writer <<- ParquetFileWriter$create(
    wkb_schema,
    wkb_stream,
    properties = ParquetWriterProperties$create(
      column_names = names(wkb_schema),
      compression = "zstd"
    )
  )
}

reinit_points_writer()
reinit_wkb_writer()

pb <- progress::progress_bar$new(total = meta_reader$num_row_groups)
for (i in seq_len(meta_reader$num_row_groups) - 1L) {
  pb$tick()

  group <- as.data.frame(meta_reader$ReadRowGroup(i, 0:1))
  group$meta_file <- meta_parquets[group$file_index + 1]
  group$src_file <- parquets[group$file_index + 1]
  group$dst_index <- seq_len(nrow(group)) - 1L

  out_points <- group |>
    reframe(
      dst_index,
      src_file_index = file_index,
      src_index = index,
      as.data.frame(
        read_parquet(
          meta_file[1],
          as_data_frame = FALSE
        )[index + 1L, "lonlat", drop = FALSE]
      ),
      .by = meta_file
    ) |>
    arrange(dst_index) |>
    select(dst_index, src_file_index, src_index, geometry = lonlat)

  out_wkb <- group |>
    reframe(
      dst_index,
      src_file_index = file_index,
      src_index = index,
      as.data.frame(
        read_parquet(
          src_file[1],
          as_data_frame = FALSE
        )[index + 1L, "geometry", drop = FALSE]
      ),
      .by = src_file
    ) |>
    arrange(dst_index) |>
    select(dst_index, src_file_index, src_index, geometry)

  stopifnot(identical(out_points$dst_index, group$dst_index))
  stopifnot(identical(out_wkb$dst_index, group$dst_index))
  out_points$dst_index <- NULL
  out_wkb$dst_index <- NULL

  points_writer$WriteTable(as_arrow_table(out_points, points_schema), nrow(out_points))
  wkb_writer$WriteTable(as_arrow_table(out_wkb, points_schema), nrow(out_wkb))

  if (points_stream$tell() >= parquet_size_hint) {
    reinit_points_writer()
  }

  if (wkb_stream$tell() >= parquet_size_hint) {
    reinit_wkb_writer()
  }
}

points_writer$Close()
points_stream$close()

wkb_writer$Close()
points_stream$close()
