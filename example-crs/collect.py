import json
from pathlib import Path

import geoarrow.pyarrow as ga
import geopandas
import pyarrow as pa
import pyproj
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


def write_geoarrow():
    for label in CRS_LABELS:
        out = here / "files" / f"example-crs_vermont-{label}_wkb.arrows"

        tab = io.read_geoparquet_table(
            here / "files" / f"example-crs_vermont-{label}.parquet"
        )
        with ipc.new_stream(out, tab.schema) as writer:
            writer.write_table(tab)


def write_fgb():
    for label in CRS_LABELS:
        out = here / "files" / f"example-crs_vermont-{label}.fgb"

        tab = io.read_geoparquet_table(
            here / "files" / f"example-crs_vermont-{label}.parquet"
        )

        # geoarrow-rust needs "native" and not WKB-encoding
        tab = pa.table({"geometry": ga.as_geoarrow(tab["geometry"])})
        with open(out, "wb") as f:
            write_flatgeobuf(tab, f, write_index=False)


def write_geoarrow_alternative_crses():
    tab = io.read_geoparquet_table(here / "files" / "example-crs_vermont-crs84.parquet")

    # Construct these metadatas by hand since that's the whole point of this data
    extension_metadata = {
        "wkt2": {"crs": pyproj.CRS(tab["geometry"].type.crs).to_wkt(), "crs_type": "wkt2"},
        "authority_code": {"crs": "OGC:CRS84", "crs_type": "authority_code"},
        "unknown": {"crs": "OGC:CRS84"},
    }

    for name, ext_metadata in extension_metadata.items():
        metadata = {
            "ARROW:extension:name": "geoarrow.wkb",
            "ARROW:extension:metadata": json.dumps(ext_metadata),
        }
        schema = pa.schema([pa.field("geometry", pa.binary(), metadata)])
        tab_out = pa.table([tab["geometry"].chunk(0).storage], schema=schema)
        out = here / "files" / f"example-crs_vermont-crs84-{name}.arrows"
        with ipc.new_stream(out, schema) as writer:
            writer.write_table(tab_out)


if __name__ == "__main__":
    write_parquet()
    write_geoarrow()
    write_fgb()
    write_geoarrow_alternative_crses()
