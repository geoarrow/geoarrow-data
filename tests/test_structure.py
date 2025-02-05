import json
import os
import re
import zipfile

import pyarrow as pa
import pytest
from pyarrow import ipc, parquet

from . import model

# Roughly, these are all the checks we can do on the files without actually invoking
# any spatial interpretation (e.g., file type, metadata, data types)


@pytest.mark.parametrize(
    "file", model.list_files(), ids=[f.path.name for f in model.list_files()]
)
def test_structure(file: model.File):
    assert file.path.exists()
    # We need all of these to be less than 2 GB
    assert os.stat(file.path).st_size <= (2**31 - 1)

    if file.format == "arrows/wkb":
        table = read_format_arrows(file)
        check_wkb(file, table)
    elif file.format == "arrows/wkt":
        table = read_format_arrows(file)
        check_wkt(file, table)
    elif file.format in ("arrows", "arrows/interleaved"):
        table = read_format_arrows(file)
        check_arrows(file, table)
    elif file.format == "arrows/box":
        table = read_format_arrows(file)
        check_box(file, table)
    elif file.format == "tsv":
        read_format_tsv(file)
    elif file.format == "geoparquet":
        table = read_format_geoparquet(file)
        check_wkb(file, table)
    elif file.format == "geoparquet/native":
        table = read_format_geoparquet(file)
        check_arrows(file, table)
    elif file.format == "fgb/zip":
        read_format_fgb_zip(file)
    elif file.format == "fgb":
        read_format_fgb(file)
    else:
        pytest.skip(f"Unimplemented format: {file.format}")


def read_format_arrows(file: model.File):
    with ipc.open_stream(file.path) as reader:
        table = reader.read_all()
        assert "geometry" in table.column_names
        assert table.column_names[-1] == "geometry"

        field = table.schema.field("geometry")
        assert field.metadata[b"ARROW:extension:name"].startswith(b"geoarrow.")
        assert isinstance(json.loads(field.metadata[b"ARROW:extension:metadata"]), dict)

        table.validate(full=True)
        return table


def read_format_geoparquet(file: model.File):
    table = parquet.read_table(file.path)
    assert "geometry" in table.column_names
    assert table.column_names[-1] == "geometry"

    assert b"geo" in table.schema.metadata

    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["primary_column"] == "geometry"
    assert list(geo["columns"].keys()) == ["geometry"]

    col = geo["columns"]["geometry"]
    assert "encoding" in col
    assert "geometry_types" in col
    if "crs" in col:
        assert col["crs"] is None or isinstance(col, dict)

    return table


def read_format_tsv(file: model.File):
    with open(file.path) as f:
        assert f.readline() == "geometry\n"
        count = 0
        for line in f:
            if line.strip():
                assert re.match(r"^[A-Z]+", line) is not None
            count += 1
        assert count > 0


def check_wkb(file: model.File, table: pa.Table):
    assert table["geometry"].type == pa.binary()


def check_wkt(file: model.File, table: pa.Table):
    assert table["geometry"].type == pa.utf8()


def check_box(file: model.File, table: pa.Table):
    assert table["geometry"].type == pa.struct(
        [
            pa.field("xmin", pa.float64(), False),
            pa.field("ymin", pa.float64(), False),
            pa.field("xmax", pa.float64(), False),
            pa.field("ymax", pa.float64(), False),
        ]
    )


def check_arrows(file: model.File, table: pa.Table):
    _check_native_type(table["geometry"].type, file.format)


def _check_native_type(type: pa.DataType, format: str):
    if pa.types.is_list(type):
        return _check_native_type(type.value_type, format)
    elif pa.types.is_union(type):
        # We could validate field names and type ids when implemented
        return

    if format == "arrows/interleaved":
        assert pa.types.is_fixed_size_list(type)
        assert type.value_field.name in ("xy", "xyz", "xym", "xyzm")
        assert pa.types.is_float64(type.value_type)
    elif format in ("arrows", "geoparquet/native"):
        assert pa.types.is_struct(type)
        assert "".join(type.names) in ("xy", "xyz", "xym", "xyzm")
        for field in type.fields:
            assert pa.types.is_float64(field.type)
    else:
        raise ValueError(f"Unexpected format: {format}")


def read_format_fgb_zip(file: model.File):
    with zipfile.ZipFile(file.path) as fzip:
        assert fzip.namelist() == [file.path.name.replace(".zip", "")]
        magic = b"\x66\x67\x62\x03\x66\x67\x62\x00"
        with fzip.open(fzip.namelist()[0]) as f:
            assert f.read(len(magic)) == magic


def read_format_fgb(file: model.File):
    magic = b"\x66\x67\x62\x03\x66\x67\x62\x00"
    with open(file.path, "rb") as f:
        assert f.read(len(magic)) == magic
