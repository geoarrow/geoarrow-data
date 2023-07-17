import pyarrow as pa
import pyarrow.parquet as parquet
import pyarrow.ipc as ipc
import geoarrow.pyarrow as ga
import glob
import os
import re


def write_ipc(tab, filename):
    with open(filename, "wb") as f, ipc.new_stream(f, tab.schema) as stream:
        stream.write_table(tab)


for f in glob.glob("example/*-wkt.parquet"):
    table = parquet.read_table(f)
    geom = table.column("geometry")
    geo_meta = table.schema.metadata[b"geo"].decode("UTF-8")

    type_out = None
    geometry_type_str = os.path.basename(f)
    if geometry_type_str.startswith("example-point"):
        type_out = ga.point()
    elif geometry_type_str.startswith("example-linestring"):
        type_out = ga.linestring()
    elif geometry_type_str.startswith("example-polygon"):
        type_out = ga.polygon()
    elif geometry_type_str.startswith("example-multipoint"):
        type_out = ga.multipoint()
    elif geometry_type_str.startswith("example-multilinestring"):
        type_out = ga.multilinestring()
    elif geometry_type_str.startswith("example-multipolygon"):
        type_out = ga.multipolygon()
    else:
        # e.g., geometrycollection
        continue

    geo_meta_out = re.sub("geoarrow.wkt", type_out.extension_name, geo_meta)
    schema_out = pa.schema([pa.field("geometry", type_out)]).with_metadata(
        {"geo": geo_meta_out}
    )

    geom_out = pa.chunked_array([array.as_geoarrow(type_out) for array in geom.chunks])

    table_out = pa.table([geom_out], schema=schema_out)
    parquet.write_table(
        table_out, re.sub("-wkt.parquet", ".parquet", f), compression="NONE"
    )
    write_ipc(table_out, re.sub("-wkt.parquet", ".arrows", f))

    type_out = type_out.with_coord_type(ga.CoordType.INTERLEAVED)
    schema_out = pa.schema([pa.field("geometry", type_out)]).with_metadata(
        {"geo": geo_meta_out}
    )

    geom_out = pa.chunked_array([array.as_geoarrow(type_out) for array in geom.chunks])

    table_out = pa.table([geom_out], schema=schema_out)
    parquet.write_table(
        table_out, re.sub("-wkt.parquet", "-interleaved.parquet", f), compression="NONE"
    )
    write_ipc(table_out, re.sub("-wkt.parquet", "-interleaved.arrows", f))
