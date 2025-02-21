from pathlib import Path

import geoarrow.pyarrow as ga
import geoarrow.types as gat
import geopandas
import pyarrow as pa
from geoarrow.pyarrow import io
from geoarrow.rust.io import write_flatgeobuf

here = Path(__file__).parent

# Obtained from
# https://www.arcgis.com/home/item.html?id=078eb80010984ddeb4bb5850889e5e9f


def write_box(lazy=True):
    out = here / "files" / "quadrangles_100k_box.arrows"
    if lazy and out.exists():
        return

    pd = geopandas.read_file(
        here / "cache" / "USGS_100k_Topo_Map_Boundaries/v107/topoq100.gdb"
    )
    sheets = pd[["USGS_QD_ID", "geometry"]][~pd["USGS_QD_ID"].isna()]
    bounds = sheets.geometry.bounds.round(4)
    bounds_array = pa.StructArray.from_arrays(
        [pa.array(bounds[col]) for col in bounds],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    box_array = gat.box().to_pyarrow().with_crs(gat.OGC_CRS84).wrap_array(bounds_array)

    table = pa.table({"quadrangle_id": sheets["USGS_QD_ID"], "geometry": box_array})
    with pa.ipc.new_stream(out, table.schema) as writer:
        writer.write_table(table)


def write_everything_else():
    box_file = here / "files" / "quadrangles_100k_box.arrows"
    with pa.ipc.open_stream(box_file) as reader:
        table = reader.read_all()

    wkb_stream = here / "files" / "quadrangles_100k_wkb.arrows"
    stream = here / "files" / "quadrangles_100k.arrows"
    interleaved_stream = here / "files" / "quadrangles_100k_interleaved.arrows"
    geoparquet = here / "files" / "quadrangles_100k_geo.parquet"
    geoparquet_native = here / "files" / "quadrangles_100k_native.parquet"
    fgb = here / "files" / "quadrangles_100k.fgb"

    table_wkb = table.set_column(1, "geometry", ga.as_wkb(table["geometry"]))
    with pa.ipc.new_stream(wkb_stream, table_wkb.schema) as writer:
        writer.write_table(table_wkb)

    # as_geoarrow() can't quite handle the box type yet
    table_arrow = table.set_column(
        1,
        "geometry",
        ga.as_geoarrow(table_wkb["geometry"], coord_type=ga.CoordType.SEPARATED),
    )
    with pa.ipc.new_stream(stream, table_arrow.schema) as writer:
        writer.write_table(table_arrow)

    table_interleaved = table.set_column(
        1,
        "geometry",
        ga.as_geoarrow(table_wkb["geometry"], coord_type=ga.CoordType.INTERLEAVED),
    )
    with pa.ipc.new_stream(interleaved_stream, table_interleaved.schema) as writer:
        writer.write_table(table_interleaved)

    # write_geoparquet_table() can't quite handle the box type
    io.write_geoparquet_table(table_wkb, geoparquet)
    io.write_geoparquet_table(
        table_wkb,
        geoparquet_native,
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )

    with open(fgb, "wb") as f:
        write_flatgeobuf(table_interleaved, f, write_index=False)


if __name__ == "__main__":
    write_box()
    write_everything_else()
