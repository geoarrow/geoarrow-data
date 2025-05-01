import json
import warnings

import geopandas
import geoarrow.pyarrow as ga
import numpy as np
import pyarrow as pa
from pyarrow import parquet
import pytest

from . import model


def check_parquet_schema(arrow_wkb_schema, parquet_schema):
    field_index = arrow_wkb_schema.get_field_index("geometry")
    field = arrow_wkb_schema.field(field_index)
    col = parquet_schema.column(field_index)
    col_dict = json.loads(col.logical_type.to_json())
    if field.type.edge_type == ga.EdgeType.PLANAR:
        assert col_dict["Type"] == "Geometry"
    else:
        assert col_dict["Type"] == "Geography"

    if field.type.crs is None:
        assert "crs" not in col_dict

    return col_dict["Type"]


def check_xy_stats_geopandas(batch, parquet_stats):
    try:
        # This doesn't handle nulls but is much faster
        geoseries = ga.to_geopandas(batch["geometry"])
    except TypeError:
        # Handles nulls but is much slower
        geoseries = geopandas.GeoSeries.from_wkb(batch["geometry"].to_pylist())

    xmin, ymin, xmax, ymax = geoseries.total_bounds
    assert (
        parquet_stats["xmin"],
        parquet_stats["ymin"],
        parquet_stats["xmax"],
        parquet_stats["ymax"],
    ) == (xmin, ymin, xmax, ymax)

    # There's no good way to get Z or M bounds from geopandas but we can
    # get all the coordinates and compute them ourselves. (M values aren't
    # supported by geopandas yet)
    xyz_coords = geoseries.get_coordinates(include_z=True).to_numpy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        coord_mins = [None if np.isnan(x) else x for x in np.nanmin(xyz_coords, 0)]
        coord_maxes = [None if np.isnan(x) else x for x in np.nanmax(xyz_coords, 0)]

    assert xyz_coords.shape[1] == 3
    xmin, ymin, zmin = coord_mins
    xmax, ymax, zmax = coord_maxes
    assert (
        parquet_stats["xmin"],
        parquet_stats["ymin"],
        parquet_stats["zmin"],
        parquet_stats["xmax"],
        parquet_stats["ymax"],
        parquet_stats["zmax"],
    ) == (xmin, ymin, zmin, xmax, ymax, zmax)


def check_xy_stats_geoarrow_box(batch, parquet_stats):
    box = ga.box_agg(batch["geometry"]).as_py()
    assert (
        parquet_stats["xmin"],
        parquet_stats["ymin"],
        parquet_stats["xmax"],
        parquet_stats["ymax"],
    ) == (
        box["xmin"],
        box["ymin"],
        box["xmax"],
        box["ymax"],
    )


def check_geometry_types_geoarrow(batch, parquet_stats):
    types = ga.unique_geometry_types(batch["geometry"]).to_pylist()
    type_codes = [t["geometry_type"] + ((t["dimensions"] - 1) * 1000) for t in types]
    assert parquet_stats["geospatial_types"] == type_codes


@pytest.mark.parametrize(
    "name_and_item", model.list_items().items(), ids=list(model.list_items().keys())
)
def test_batch_statistics(name_and_item):
    item: model.File = name_and_item[1]

    try:
        parquet_file = parquet.ParquetFile(
            item["parquet"].path, arrow_extensions_enabled=True
        )
    except TypeError:
        pytest.skip("forthcoming pyarrow required for test")

    wkb_path = item["arrows/wkb"].path
    with pa.ipc.open_stream(wkb_path) as reader:
        parquet_type = check_parquet_schema(reader.schema, parquet_file.schema)

    if parquet_type == "Geography":
        return

    parquet_stats = []
    geometry_column_index = parquet_file.schema_arrow.get_field_index("geometry")
    for i in range(parquet_file.num_row_groups):
        col = parquet_file.metadata.row_group(i).column(geometry_column_index)
        parquet_stats.append(col.geo_statistics.to_dict())

    with pa.ipc.open_stream(wkb_path) as reader:
        for i, batch in enumerate(reader):
            check_xy_stats_geopandas(batch, parquet_stats[i])
            check_xy_stats_geoarrow_box(batch, parquet_stats[i])
            check_geometry_types_geoarrow(batch, parquet_stats[i])
