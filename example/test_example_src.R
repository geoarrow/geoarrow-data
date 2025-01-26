library(testthat)
library(wk)

src <- yaml::read_yaml("example/example_src.yaml")

# Check wk roundtrip
for (item in src) {
  is_null <- vapply(item, is.null, logical(1))
  item[is_null] <- NA_character_
  item <- as.character(item)
  roundtrip <- wk_handle(wk::wkt(item), wkt_writer()) |> unclass()
  expect_equal(roundtrip, item)
}
