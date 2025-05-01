import geopandas
import pyarrow as pa
import pytest

from . import model

# Skip buildings because it takes too long to load, skip built-in parquet
# because geopandas doesn't support it
GEOPANDAS_PARQUET_FILES = [
    f
    for f in model.list_files()
    if "geoparquet" in f.format and "buildings" not in f.group
]

# Skip buildings because it takes too long to load, skip wkt and box because
# geopandas doesn't support it, skip anything with m values because it's
# not supported by geopandas yet
GEOPANDAS_ARROW_FILES = [
    f
    for f in model.list_files()
    if "arrows" in f.format
    and "buildings" not in f.group
    and not f.name.endswith("m")
    and f.format not in ("arrows/wkt", "arrows/box")
]

# We limit to a million rows on read, so we can handle all .fgb files
GEOPANDAS_FGB_FILES = [f for f in model.list_files() if "fgb" in f.format]


@pytest.mark.parametrize(
    "file",
    GEOPANDAS_PARQUET_FILES,
    ids=[f.path.name for f in GEOPANDAS_PARQUET_FILES],
)
def test_geoparquet(file: model.File):
    df = geopandas.read_parquet(file.path)
    assert isinstance(df, geopandas.GeoDataFrame)
    assert isinstance(df.geometry, geopandas.GeoSeries)


@pytest.mark.parametrize(
    "file",
    GEOPANDAS_ARROW_FILES,
    ids=[f.path.name for f in GEOPANDAS_ARROW_FILES],
)
def test_from_arrow(file: model.File):
    if file.format == "arrows/wkt":
        pytest.skip("geopandas doesn't support geoarrow.wkt")

    with pa.ipc.open_stream(file.path) as reader:
        tab = reader.read_all()
    df = geopandas.GeoDataFrame.from_arrow(tab)
    assert isinstance(df, geopandas.GeoDataFrame)
    assert isinstance(df.geometry, geopandas.GeoSeries)


@pytest.mark.parametrize(
    "file",
    GEOPANDAS_FGB_FILES,
    ids=[f.path.name for f in GEOPANDAS_FGB_FILES],
)
def test_fgb(file: model.File):
    # Check with and without use_arrow
    df = geopandas.read_file(
        file.path, rows=1_000_000, engine="pyogrio", use_arrow=False
    )
    assert isinstance(df, geopandas.GeoDataFrame)
    assert isinstance(df.geometry, geopandas.GeoSeries)

    df = geopandas.read_file(
        file.path, rows=1_000_000, engine="pyogrio", use_arrow=True
    )
    assert isinstance(df, geopandas.GeoDataFrame)
    assert isinstance(df.geometry, geopandas.GeoSeries)
