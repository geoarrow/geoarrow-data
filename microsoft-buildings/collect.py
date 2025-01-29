import os
import shutil
import urllib.request
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import geoarrow.pyarrow as ga
import geoarrow.types as gat
import pyarrow as pa
import pyarrow.compute as pc
from geoarrow.pyarrow import io
from geoarrow.rust.compute import centroid
from geoarrow.rust.io import read_geojson, read_parquet, write_flatgeobuf, write_parquet
from pyarrow import ipc, parquet

here = Path(__file__).parent

url_base = "https://minedbuildings.z5.web.core.windows.net/legacy/usbuildings-v2"
cache = here / "by_state"


def download_geojson_zip(state, lazy=True):
    url = f"{url_base}/{state}.geojson.zip"
    dst = cache / f"{state}.geojson.zip"
    if lazy and dst.exists():
        return state

    dst_tmp = f"{dst}.tmp"
    with urllib.request.urlopen(url) as fin, open(dst_tmp, "wb") as fout:
        shutil.copyfileobj(fin, fout)

    if dst.exists():
        os.unlink(dst)

    os.rename(dst_tmp, dst)
    return state


def read_geojson_zip(state, ndigits=8):
    src = cache / f"{state}.geojson.zip"
    with zipfile.ZipFile(src) as fzip:
        with fzip.open(fzip.namelist()[0]) as f:
            tab = read_geojson(f)
            centres = centroid(tab["geometry"])

    # Round to ndigits decimal places
    centres = pa.chunked_array(centres)
    assert centres.type.coord_type == ga.CoordType.INTERLEAVED
    assert centres.type.dimensions == ga.Dimensions.XY
    storage_out = []
    for chunk in centres.chunks:
        values = pc.round(chunk.storage.values, ndigits)
        storage = pa.FixedSizeListArray.from_arrays(
            values, type=centres.type.storage_type
        )
        storage_out.append(storage)

    storage = pa.chunked_array(storage_out, centres.type.storage_type)
    return state, centres.type.wrap_array(storage)


def cache_state_geoparquet(state, ndigits=8, lazy=True):
    out = cache / f"{state}.parquet"
    out_tmp = cache / f"{state}.parquet.tmp"
    if lazy and out.exists():
        return state

    state, centres = read_geojson_zip(state, ndigits)
    tab = pa.table({"geometry": centres})

    write_parquet(tab, out_tmp)
    os.rename(out_tmp, out)
    return state


def write_geoparquet(lazy=True):
    out = here / "files" / "microsoft-buildings_point.parquet"
    out_tmp = here / "files" / "microsoft-buildings_point.parquet.tmp"
    if lazy and out.exists():
        return

    with duckdb.connect() as con:
        con.load_extension("spatial")
        glob_in = here / "by_state" / "*.parquet"
        con.sql(
            f"""
        COPY (SELECT *
        FROM "{glob_in}"
        ORDER BY ST_Hilbert(
            geometry,
            (
                SELECT ST_Extent(ST_Extent_Agg(COLUMNS(geometry)))::BOX_2D
                FROM "{glob_in}"
            )
        )) TO '{out_tmp}' (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE '100000');
        """
        )

    os.rename(out_tmp, out)


def convert_arrow(tab_wkb, type):
    geom = tab_wkb["geometry"]
    assert geom.type == pa.binary()

    if type.encoding == ga.Encoding.WKB:
        geom = type.with_crs(ga.OGC_CRS84).wrap_array(geom)
    else:
        geom = ga.with_crs(
            ga.as_geoarrow(geom, coord_type=type.coord_type), ga.OGC_CRS84
        )

    return pa.table({"geometry": geom})


def write_arrow(type, out, lazy=True):
    out_tmp = f"{out}.tmp"
    if lazy and out.exists():
        return out

    # Process batch-by-batch to keep the same structure and ordering
    schema = pa.schema({"geometry": type.with_crs(ga.OGC_CRS84)})
    options = ipc.IpcWriteOptions(compression="zstd")
    with (
        parquet.ParquetFile(here / "files" / "microsoft-buildings_point.parquet") as f,
        ipc.new_stream(out_tmp, schema, options=options) as writer,
    ):
        for i in range(f.num_row_groups):
            batch = convert_arrow(f.read_row_group(i), type)
            writer.write_table(batch, max_chunksize=100_000)

    os.rename(out_tmp, out)
    return out


def write_geoparquet_native(lazy=True):
    out_tmp = here / "files" / "microsoft-buildings_point_native.parquet.tmp"
    out = here / "files" / "microsoft-buildings_point_native.parquet"
    if lazy and out.exists():
        return out

    tab = io.read_geoparquet_table(here / "files" / "microsoft-buildings_point.parquet")
    io.write_geoparquet_table(
        tab,
        out_tmp,
        compression="zstd",
        write_batch_size=100_000,
        write_bbox=True,
        write_geometry_types=True,
        geometry_encoding="point",
    )

    os.rename(out_tmp, out)


def write_fgb(lazy=True):
    out_tmp = here / "files" / "microsoft-buildings_point.fgb.zip.tmp"
    out_fgb = here / "files" / "microsoft-buildings_point.fgb"
    out = here / "files" / "microsoft-buildings_point.fgb.zip"
    if lazy and out.exists():
        return out

    tab = read_parquet(here / "files" / "microsoft-buildings_point.parquet")

    write_flatgeobuf(tab, out_fgb, write_index=False)

    # Could also use zipfile.ZipFile() here, but zip on the command line
    # has more intuitive defaults
    if out_tmp.exists():
        os.unlink(out_tmp)

    wdir = os.getcwd()
    try:
        os.chdir(out_tmp.parent)
        if os.system(f"zip {out_tmp.name} {out_fgb}") != 0:
            raise ValueError("zip command failed")

        os.unlink(out_fgb)
        os.rename(out_tmp, out)
    finally:
        os.chdir(wdir)

    return out


def main():
    # Out-of-memory is an issue if this is set too high (California is
    # the main issue)
    pool = ProcessPoolExecutor(max_workers=3)

    print("Downloading...")
    futures = [pool.submit(download_geojson_zip, state) for state in STATES]
    for future in as_completed(futures):
        print(future.result())

    print("Converting GeoJSON footprints to GeoParquet centroids...")
    futures = [pool.submit(cache_state_geoparquet, state) for state in STATES]
    for future in as_completed(futures):
        print(future.result())

    print("Writing GeoParquet (WKB)...")
    write_geoparquet()

    print("Writing GeoParquet (native)...")
    write_geoparquet_native()

    print("Writing FlatGeoBuf...")
    write_fgb()

    print("Writing Arrow...")
    futures = []
    futures.append(
        pool.submit(
            write_arrow,
            gat.wkb().to_pyarrow(),
            here / "files" / "microsoft-buildings_point_wkb.arrows",
        )
    )
    futures.append(
        pool.submit(
            write_arrow,
            gat.point().to_pyarrow(),
            here / "files" / "microsoft-buildings_point.arrows",
        )
    )
    futures.append(
        pool.submit(
            write_arrow,
            gat.point().to_pyarrow().with_coord_type(ga.CoordType.INTERLEAVED),
            here / "files" / "microsoft-buildings_point_interleaved.arrows",
        )
    )
    for future in as_completed(futures):
        print(future.result())


# We're deliberately omitting Alaska and Hawaii here becuase we need the
# output files to be <2GB for distribution via GitHub Releases, and with
# both of them the _wkb.arrows output is very close to the limit
STATES = [
    "Alabama",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "DistrictofColumbia",
    "Florida",
    "Georgia",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "NewHampshire",
    "NewJersey",
    "NewMexico",
    "NewYork",
    "NorthCarolina",
    "NorthDakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "RhodeIsland",
    "SouthCarolina",
    "SouthDakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "WestVirginia",
    "Wisconsin",
    "Wyoming",
]

assert len(STATES) == 49

if __name__ == "__main__":
    main()
