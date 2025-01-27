
# This script downloads the zipped GeoJSON files from the source
# https://github.com/microsoft/USBuildingFootprints

library(stringr)
library(glue)
library(tibble)
library(furrr)
plan(multisession)

base_url <-
  "https://minedbuildings.z5.web.core.windows.net/legacy/usbuildings-v2"
by_state_path <- "microsoft-buildings/by_state"

if (dir.exists(by_state_path)) {
  unlink(by_state_path, recursive = TRUE)
}

dir.create(by_state_path)

states_zip <- c(
  "Alabama.geojson.zip", "Alaska.geojson.zip",
  "Arizona.geojson.zip", "Arkansas.geojson.zip", "California.geojson.zip",
  "Colorado.geojson.zip", "Connecticut.geojson.zip", "Delaware.geojson.zip",
  "DistrictofColumbia.geojson.zip", "Florida.geojson.zip",
  "Georgia.geojson.zip",
  "Hawaii.geojson.zip", "Idaho.geojson.zip", "Illinois.geojson.zip",
  "Indiana.geojson.zip", "Iowa.geojson.zip", "Kansas.geojson.zip",
  "Kentucky.geojson.zip", "Louisiana.geojson.zip", "Maine.geojson.zip",
  "Maryland.geojson.zip", "Massachusetts.geojson.zip", "Michigan.geojson.zip",
  "Minnesota.geojson.zip", "Mississippi.geojson.zip", "Missouri.geojson.zip",
  "Montana.geojson.zip", "Nebraska.geojson.zip", "Nevada.geojson.zip",
  "NewHampshire.geojson.zip", "NewJersey.geojson.zip", "NewMexico.geojson.zip",
  "NewYork.geojson.zip", "NorthCarolina.geojson.zip", "NorthDakota.geojson.zip",
  "Ohio.geojson.zip", "Oklahoma.geojson.zip", "Oregon.geojson.zip",
  "Pennsylvania.geojson.zip", "RhodeIsland.geojson.zip",
  "SouthCarolina.geojson.zip",
  "SouthDakota.geojson.zip", "Tennessee.geojson.zip", "Texas.geojson.zip",
  "Utah.geojson.zip", "Vermont.geojson.zip", "Virginia.geojson.zip",
  "Washington.geojson.zip", "WestVirginia.geojson.zip", "Wisconsin.geojson.zip",
  "Wyoming.geojson.zip")

# Download by state in parallel
future_walk(states_zip, function(state_zip) {
  curl::curl_download(
    file.path(base_url, state_zip),
    file.path(by_state_path, state_zip)
  )
}, .progress = TRUE)
