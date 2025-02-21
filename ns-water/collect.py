import os
import shutil
import urllib.request
from pathlib import Path

import geoarrow.pyarrow as ga
import geoarrow.types as gat
import pyarrow as pa
from geoarrow.pyarrow import io
from geoarrow.rust.io import write_flatgeobuf
from pyarrow import compute as pc
from pyarrow import ipc

here = Path(__file__).parent
cache = here / "cache"

# https://nsgi.novascotia.ca/gdd/

# Nova Scotia Topographic Database - Digital Terrain Model (DTM)
url_dtm = "https://nsgi.novascotia.ca/WSF_DDS/DDS.svc/DownloadFile?tkey=kNNpTdP4QuNRSYtt&id=36729"
# unzip -l dtm.zip
# Archive:  dtm.zip
#   Length      Date    Time    Name
# ---------  ---------- -----   ----
#         5  10-28-2020 16:48   LF_DTM_POINT_10K.cpg
# 2127468874  10-28-2020 16:48   LF_DTM_POINT_10K.dbf
#       602  04-18-2024 08:21   LF_DTM_POINT_10K.prj
#  92974116  10-28-2020 17:00   LF_DTM_POINT_10K.sbn
#    951180  10-28-2020 17:00   LF_DTM_POINT_10K.sbx
# 503272276  10-28-2020 16:48   LF_DTM_POINT_10K.shp
#      7832  04-18-2024 08:21   LF_DTM_POINT_10K.shp.xml
#  91504132  10-28-2020 16:48   LF_DTM_POINT_10K.shx
#   7704576  02-21-2019 17:44   NSTDB_10k.style
#     45663  11-27-2019 15:33   NSTDB_FEATURECODES.txt
#    115200  11-27-2019 15:30   NSTDB_FEATURECODES.xls
#     30220  02-21-2019 17:44   NSTDB_Topo.ttf
#       297  03-04-2019 11:13   README.txt
# ---------                     -------
# 2824074973                     13 files

# Nova Scotia Hydrographic Network (NSHN)
url_nshn = "https://nsgi.novascotia.ca/WSF_DDS/DDS.svc/DownloadFile?tkey=kNNpTdP4QuNRSYtt&id=38906"
# unzip -l nshn.zip
# Archive:  nshn.zip
#   Length      Date    Time    Name
# ---------  ---------- -----   ----
#    117003  04-24-2019 10:11   NSHN Attribute_Specs.pdf
#     12926  06-22-2020 10:01   NSHN_FEATURECODES.txt
#     50688  06-22-2020 10:02   NSHN_FEATURECODES.xls
#    131265  10-28-2020 18:37   nshn_v2_ba_line.dbf
#       609  10-28-2020 17:39   nshn_v2_ba_line.prj
#   1606352  10-28-2020 18:37   nshn_v2_ba_line.shp
#      2140  10-28-2020 18:37   nshn_v2_ba_line.shx
#      4472  10-28-2020 18:37   nshn_v2_ba_point.dbf
#       609  10-28-2020 17:39   nshn_v2_ba_point.prj
#      2124  10-28-2020 18:37   nshn_v2_ba_point.shp
#       468  10-28-2020 18:37   nshn_v2_ba_point.shx
#      7388  10-28-2020 18:37   nshn_v2_ba_poly.dbf
#       609  10-28-2020 17:39   nshn_v2_ba_poly.prj
#   2928500  10-28-2020 18:37   nshn_v2_ba_poly.shp
#       468  10-28-2020 18:37   nshn_v2_ba_poly.shx
#   7740300  10-28-2020 18:37   nshn_v2_la_poly.dbf
#       609  10-28-2020 17:39   nshn_v2_la_poly.prj
# 292054164  10-28-2020 18:37   nshn_v2_la_poly.shp
#    192396  10-28-2020 18:37   nshn_v2_la_poly.shx
#   3212845  10-28-2020 18:37   nshn_v2_wa_cent.dbf
#       609  10-28-2020 17:40   nshn_v2_wa_cent.prj
#   2570272  10-28-2020 18:37   nshn_v2_wa_cent.shp
#    467404  10-28-2020 18:37   nshn_v2_wa_cent.shx
#  32155778  10-28-2020 18:37   nshn_v2_wa_junc.dbf
#       609  10-28-2020 17:41   nshn_v2_wa_junc.prj
#  13604372  10-28-2020 18:37   nshn_v2_wa_junc.shp
#   2473604  10-28-2020 18:37   nshn_v2_wa_junc.shx
# 668844002  10-28-2020 18:37   nshn_v2_wa_line.dbf
#       609  10-28-2020 17:47   nshn_v2_wa_line.prj
# 779824044  10-28-2020 18:37   nshn_v2_wa_line.shp
#   3866244  10-28-2020 18:37   nshn_v2_wa_line.shx
#   6614378  10-28-2020 18:37   nshn_v2_wa_point.dbf
#       609  10-28-2020 18:03   nshn_v2_wa_point.prj
#   1966460  10-28-2020 18:37   nshn_v2_wa_point.shp
#    357620  10-28-2020 18:37   nshn_v2_wa_point.shx
#  61826578  10-28-2020 18:37   nshn_v2_wa_poly.dbf
#       609  10-28-2020 18:04   nshn_v2_wa_poly.prj
# 841040660  10-28-2020 18:37   nshn_v2_wa_poly.shp
#   1358916  10-28-2020 18:37   nshn_v2_wa_poly.shx
#   7704576  02-21-2019 16:44   NSTDB_10k.style
#     45663  11-27-2019 14:33   NSTDB_FEATURECODES.txt
#    115200  11-27-2019 14:30   NSTDB_FEATURECODES.xls
#     30220  02-21-2019 16:44   NSTDB_Topo.ttf
#       297  03-04-2019 10:13   README.txt
# ---------                     -------
# 2732935268                     44 files


def download_archive(url, dst, lazy=True):
    if lazy and dst.exists():
        return dst

    dst_tmp = f"{dst}.tmp"
    with urllib.request.urlopen(url) as fin, open(dst_tmp, "wb") as fout:
        shutil.copyfileobj(fin, fout)

    os.rename(dst_tmp, dst)
    return dst


def read_shp(shp):
    tab = io.read_pyogrio_table(shp)
    hilbert = ga.to_geopandas(tab["geometry"]).hilbert_distance()
    sort_indices = pc.sort_indices(pa.array(hilbert))
    return tab.take(sort_indices)


def write_geoparquet(tab, out, lazy=True):
    if lazy and out.exists():
        return out

    out_tmp = f"{out}.tmp"
    io.write_geoparquet_table(
        tab,
        out_tmp,
        compression="zstd",
        write_batch_size=100_000,
        write_bbox=True,
        write_geometry_types=True,
        geometry_encoding="WKB",
    )
    os.rename(out_tmp, out)
    return out


def write_geoparquet_native(tab, out, lazy=True):
    if lazy and out.exists():
        return out

    out_tmp = f"{out}.tmp"
    io.write_geoparquet_table(
        tab,
        out_tmp,
        compression="zstd",
        write_batch_size=100_000,
        write_bbox=True,
        write_geometry_types=True,
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )
    os.rename(out_tmp, out)
    return out


def convert_arrow(tab_wkb, type):
    if type.encoding == ga.Encoding.WKB:
        return tab_wkb

    i = tab_wkb.schema.get_field_index("geometry")
    geom = ga.with_coord_type(tab_wkb["geometry"], coord_type=type.coord_type)
    return tab_wkb.set_column(i, "geometry", geom)


def write_arrow(tab_wkb, type, out, lazy=True):
    if lazy and out.exists():
        return out

    out_tmp = f"{out}.tmp"
    options = ipc.IpcWriteOptions(compression="zstd")
    tab = convert_arrow(tab_wkb, type)
    with (
        ipc.new_stream(out_tmp, tab.schema, options=options) as writer,
    ):
        writer.write_table(tab, max_chunksize=100_000)

    os.rename(out_tmp, out)
    return out


def write_fgb(tab_wkb, out, lazy=True):
    if lazy and out.exists():
        return out

    out_tmp = f"{out}.tmp"

    with open(out_tmp, "wb") as f:
        tab_native = convert_arrow(tab_wkb, gat.type_spec(gat.CoordType.SEPARATED))
        write_flatgeobuf(tab_native, f, write_index=False)

    os.rename(out_tmp, out)
    return out


def write_files(src, name, lazy=True):
    print(f"Preparing {name}...")

    print(f"- Reading '{src}'...")
    tab = read_shp(src)

    print("- Writing geoparquet (WKB)...")
    write_geoparquet(tab, here / "files" / f"ns-water_{name}_geo.parquet", lazy=lazy)

    print("- Writing geoparquet (native)...")
    write_geoparquet_native(
        tab, here / "files" / f"ns-water_{name}_native.parquet", lazy=lazy
    )

    print("- Writing arrows/wkb")
    write_arrow(
        tab, gat.wkb(), here / "files" / f"ns-water_{name}_wkb.arrows", lazy=lazy
    )

    print("- Writing arrows")
    write_arrow(
        tab,
        gat.type_spec(coord_type=gat.CoordType.SEPARATED),
        here / "files" / f"ns-water_{name}.arrows",
        lazy=lazy,
    )

    print("- Writing arrows/interleaved")
    write_arrow(
        tab,
        gat.type_spec(coord_type=gat.CoordType.INTERLEAVED),
        here / "files" / f"ns-water_{name}_interleaved.arrows",
        lazy=lazy,
    )

    print("- Writing FlatGeoBuf")
    write_fgb(tab, here / "files" / f"ns-water_{name}.fgb", lazy=lazy)


def extract_archive_to_cache(zip_file):
    os.system(f"unzip -n -d {cache} {zip_file}")


if __name__ == "__main__":
    print("Downloading Digital Terrain Model...")
    download_archive(url_dtm, cache / "dtm.zip")

    print("Downloading Hydro Network...")
    download_archive(url_nshn, cache / "nshn.zip")

    print("Extracting archives...")
    extract_archive_to_cache(cache / "dtm.zip")
    extract_archive_to_cache(cache / "nshn.zip")

    assets = {
        "elevation": "LF_DTM_POINT_10K.shp",
        "water-junc": "nshn_v2_wa_junc.shp",
        "water-poly": "nshn_v2_wa_poly.shp",
        "water-point": "nshn_v2_wa_point.shp",
        "water-line": "nshn_v2_wa_line.shp",
        "land-poly": "nshn_v2_la_poly.shp",
    }

    for name, shp in assets.items():
        write_files(cache / shp, name, lazy=False)
