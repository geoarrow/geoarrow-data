import pathlib

import geoarrow.pyarrow as ga
import geoarrow.types as gat
import pyarrow as pa
import yaml
from pyarrow import ipc

here = pathlib.Path(__file__).parent


def read_examples():
    examples = {}
    for example_yml in ["example_src", "example_src_generated"]:
        with open(here / f"{example_yml}.yaml") as f:
            examples.update(yaml.safe_load(f))

    # Add one example so that we have one GEOMETRY example with mixed dimensions
    examples["geometry_mixed_dimensions"] = (
        examples["geometry"]
        + examples["geometry_z"]
        + examples["geometry_m"]
        + examples["geometry_zm"]
    )

    return examples


def write_tsv_from_wkt(ex, dst):
    with open(dst, "w") as f:
        f.write("geometry\n")
        for item in ex:
            f.write("" if item is None else item)
            f.write("\n")


def write_native_from_wkt(ex, coord_type, dst):
    array = ga.as_geoarrow(ex, coord_type=coord_type)
    assert array.type.encoding == gat.Encoding.GEOARROW
    assert array.type.coord_type == coord_type

    table = pa.table({"wkt": ex, "geometry": array})
    with ipc.new_stream(dst, table.schema) as writer:
        writer.write_table(table)


def write_wkb_from_wkt(ex, dst):
    array = ga.as_wkb(ex)
    assert array.type.encoding == gat.Encoding.WKB

    table = pa.table({"wkt": ex, "geometry": array})
    with ipc.new_stream(dst, table.schema) as writer:
        writer.write_table(table)


def write_wkt_from_wkt(ex, dst):
    array = ga.as_wkt(ex)
    assert array.type.encoding == gat.Encoding.WKT

    table = pa.table({"wkt": ex, "geometry": array})
    with ipc.new_stream(dst, table.schema) as writer:
        writer.write_table(table)
