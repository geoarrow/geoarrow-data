import json

import pyarrow as pa
import pytest
from pyarrow import ipc

from . import model


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


def check_wkb(file: model.File, table: pa.Table):
    assert table["geometry"].type == pa.binary()


def check_wkt(file: model.File, table: pa.Table):
    assert table["geometry"].type == pa.utf8()


def check_arrows(file: model.File, table: pa.Table):
    _check_native_type(table["geometry"].type, file.format)


def _check_native_type(type: pa.DataType, format: str):
    if pa.types.is_list(type):
        assert type.value_field.name != "item"
        return _check_native_type(type.value_type, format)
    elif pa.types.is_union(type):
        # We could validate field names and type ids when implemented
        return

    if format == "arrows/interleaved":
        assert pa.types.is_fixed_size_list(type)
        assert type.value_field.name in ("xy", "xyz", "xym", "xyzm")
        assert pa.types.is_float64(type.value_type)
    elif format == "arrows":
        assert pa.types.is_struct(type)
        assert "".join(type.names) in ("xy", "xyz", "xym", "xyzm")
        for field in type.fields:
            assert pa.types.is_float64(field.type)
    else:
        raise ValueError(f"Unexpected format: {format}")


@pytest.mark.parametrize(
    "file", model.list_files(), ids=[f.path.name for f in model.list_files()]
)
def test_structure(file: model.File):
    assert file.path.exists()

    if file.format == "arrows/wkb":
        table = read_format_arrows(file)
        check_wkb(file, table)
    elif file.format == "arrows/wkt":
        table = read_format_arrows(file)
        check_wkt(file, table)
    elif file.format in ("arrows", "arrows/interleaved"):
        table = read_format_arrows(file)
        check_arrows(file, table)
    else:
        pytest.skip("Unimplemented format")
