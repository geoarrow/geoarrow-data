from pathlib import Path

import geopandas


# Original source:
# https://www.naturalearthdata.com/


here = Path(__file__).parent


def download_vermont():
    pd = geopandas.read_file(
        "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_1_states_provinces.zip"
    )

    pd = pd[["geometry"]][pd["name"] == "Vermont"]


def write_parquet_crs84(crs_list, label_list, lazy=True):
    pd = None

    for crs, label in zip(crs_list, label_list):
        out = here / "files" / f"example-crs_vermont-{label}.parquet"
        if lazy and out.exists():
            continue

        if pd is None:
            pd = download_vermont()

        pd.to_crs(crs).to_parquet(out, compression=None)

    return out


if __name__ == "__main__":
    write_parquet_crs84(
        [
            "OGC:CRS84",
            "EPSG:4326",
            "EPSG:32618",
            "+proj=ortho +lat_0=43.88 +lon_0=-72.69 +ellps=WGS84",
        ],
        ["crs84", "4326", "utm", "custom"],
    )
