import zipfile
from pathlib import Path

import geoarrow.pyarrow as ga
import geopandas
import pyarrow as pa
from geoarrow.pyarrow import io
from geoarrow.rust.io import write_flatgeobuf
from pyarrow import ipc

# Original source:
# https://www.naturalearthdata.com/


here = Path(__file__).parent
CRSES = [
    "OGC:CRS84",
    "EPSG:4326",
    "EPSG:32618",
    "+proj=ortho +lat_0=43.88 +lon_0=-72.69 +ellps=WGS84",
]
CRS_LABELS = ["crs84", "4326", "utm", "custom"]


def download_vermont():
    pd = geopandas.read_file(
        "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_1_states_provinces.zip"
    )

    return pd[["geometry"]][pd["name"] == "Vermont"]


def write_parquet(lazy=True):
    pd = None

    for crs, label in zip(CRSES, CRS_LABELS):
        out = here / "files" / f"example-crs_vermont-{label}.parquet"
        if lazy and out.exists():
            continue

        if pd is None:
            pd = download_vermont()

        pd.to_crs(crs).to_parquet(out, compression=None)

    return out


def write_geoarrow(lazy=True):
    for label in CRS_LABELS:
        out = here / "files" / f"example-crs_vermont-{label}-wkb.arrows"
        if lazy and out.exists():
            continue

        tab = io.read_geoparquet_table(
            here / "files" / f"example-crs_vermont-{label}.parquet"
        )
        with ipc.new_stream(out, tab.schema) as writer:
            writer.write_table(tab)


def write_fgb(lazy=True):
    for label in CRS_LABELS:
        out = here / "files" / f"example-crs_vermont-{label}-wkb.fgb.zip"
        if lazy and out.exists():
            continue

        tab = io.read_geoparquet_table(
            here / "files" / f"example-crs_vermont-{label}.parquet"
        )

        # geoarrow-rust needs "native" and not WKB-encoding
        tab = pa.table({"geometry": ga.as_geoarrow(tab["geometry"])})

        with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as fzip:
            with fzip.open(out.name.replace(".zip", ""), "w", force_zip64=True) as f:
                write_flatgeobuf(tab, f, write_index=False)


if __name__ == "__main__":
    write_parquet()
    write_geoarrow()
    write_fgb()
