import pathlib
import shutil

import geoarrow.pyarrow as ga
import geoarrow.types as gat
import pyarrow as pa
import yaml
from pyarrow import ipc
from geoarrow.pyarrow import io

here = pathlib.Path(__file__).parent


def read_examples():
    examples = {}
    for example_yml in ["example_src", "example_src_generated"]:
        with open(here / f"{example_yml}.yaml") as f:
            examples.update(yaml.safe_load(f))

    # Add one example so that we have one GEOMETRY example with mixed dimensions
    examples["geometry-mixed-dimensions"] = (
        examples["geometry"]
        + examples["geometry-z"]
        + examples["geometry-m"]
        + examples["geometry-zm"]
    )

    return examples


def read_manifest():
    with open(here / "manifest.yaml") as f:
        return yaml.safe_load(f)

def write_tsv(ex, dst):
    with open(dst, "w") as f:
        f.write("geometry\n")
        for item in ex:
            f.write("" if item is None else item)
            f.write("\n")


def write_native(ex, coord_type, dst):
    array = ga.as_geoarrow(ex, coord_type=coord_type)
    assert array.type.encoding == gat.Encoding.GEOARROW
    assert array.type.coord_type == coord_type

    table = pa.table({"wkt": ex, "geometry": array})
    with ipc.new_stream(dst, table.schema) as writer:
        writer.write_table(table)


def write_wkb(ex, dst):
    array = ga.as_wkb(ex)
    assert array.type.encoding == gat.Encoding.WKB

    table = pa.table({"wkt": ex, "geometry": array})
    with ipc.new_stream(dst, table.schema) as writer:
        writer.write_table(table)


def write_wkt(ex, dst):
    array = ga.as_wkt(ex)
    assert array.type.encoding == gat.Encoding.WKT

    table = pa.table({"wkt": ex, "geometry": array})
    with ipc.new_stream(dst, table.schema) as writer:
        writer.write_table(table)


def write_geoparquet(ex, dst):
    array = ga.as_wkb(ex)
    table = pa.table({"wkt": ex, "geometry": array})
    io.write_geoparquet_table(table, dst, write_bbox=True, write_geometry_types=True)


def write_geoparquet_native(ex, dst):
    array = ga.as_geoarrow(ex)
    assert array.type.encoding == gat.Encoding.GEOARROW
    table = pa.table({"wkt": ex, "geometry": array})
    io.write_geoparquet_table(
        table,
        dst,
        write_bbox=True,
        write_geometry_types=True,
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )


def write_file(examples, ex_name, ex_format):
    ex_wkt = examples[ex_name]
    out_file = here / "files" / f"example_{ex_name}{SUFFIXES[ex_format]}"

    if ex_format == "tsv":
        write_tsv(ex_wkt, out_file)
    elif ex_format == "arrows":
        write_native(ex_wkt, gat.CoordType.SEPARATED, out_file)
    elif ex_format == "arrows/interleaved":
        write_native(ex_wkt, gat.CoordType.INTERLEAVED, out_file)
    elif ex_format == "arrows/wkb":
        write_wkb(ex_wkt, out_file)
    elif ex_format == "arrows/wkt":
        write_wkt(ex_wkt, out_file)
    elif ex_format == "geoparquet":
        write_geoparquet(ex_wkt, out_file)
    elif ex_format == "geoparquet/native":
        write_geoparquet_native(ex_wkt, out_file)
    else:
        raise ValueError(f"Unsupported format: {ex_format}")


SUFFIXES = {
    "tsv": ".tsv",
    "arrows": ".arrows",
    "arrows/interleaved": "_interleaved.arrows",
    "arrows/wkb": "_wkb.arrows",
    "arrows/wkt": "_wkt.arrows",
    "geoparquet": "_geo.parquet",
    "geoparquet/native": "_native.parquet",
}


if __name__ == "__main__":
    examples = read_examples()
    manifest = read_manifest()

    formats = manifest["format"]
    files = manifest["files"]

    if (here / "files").exists():
        shutil.rmtree(here / "files")

    (here / "files").mkdir()

    for file in files:
        name = file["name"]
        skip_format = [] if "skip_format" not in file else file["skip_format"]
        for ex_format in formats:
            if ex_format in skip_format:
                continue
            write_file(examples, name, ex_format)
