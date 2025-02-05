import urllib.request
from pathlib import Path

import geoarrow.pyarrow as ga
import geoarrow.types as gat
import geopandas
import pyarrow as pa
from geoarrow.pyarrow import io
from geoarrow.rust.io import write_flatgeobuf
from pyarrow import csv, ipc

# Original source:
# https://www.naturalearthdata.com/

# Spherical source:
# https://github.com/paleolimbot/duckdb-geography/tree/97b34f9ba2e076e25fe811e8626c1e9295c37b78/data

here = Path(__file__).parent


def write_cities():
    cities_pd = geopandas.read_file(
        "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_populated_places_simple.zip"
    )

    (
        cities_pd[["nameascii", "geometry"]]
        .rename(columns={"nameascii": "name"})
        .to_parquet(here / "files" / "natural-earth_cities.parquet", compression=None)
    )

    tab = io.read_geoparquet_table(here / "files" / "natural-earth_cities.parquet")
    io.write_geoparquet_table(
        tab,
        here / "files" / "natural-earth_cities_native.parquet",
        compression="none",
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )

    with ipc.new_stream(
        here / "files" / "natural-earth_cities_wkb.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)

    tab_native = tab.set_column(1, "geometry", ga.as_geoarrow(tab["geometry"]))
    with ipc.new_stream(
        here / "files" / "natural-earth_cities.arrows", tab_native.schema
    ) as writer:
        writer.write_table(tab_native)

    tab_native = tab.set_column(
        1,
        "geometry",
        ga.as_geoarrow(tab["geometry"], coord_type=ga.CoordType.INTERLEAVED),
    )
    with ipc.new_stream(
        here / "files" / "natural-earth_cities_interleaved.arrows", tab_native.schema
    ) as writer:
        writer.write_table(tab_native)

    with open(here / "files" / "natural-earth_cities.fgb", "wb") as f:
        write_flatgeobuf(tab_native, f, write_index=False)


def write_countries():
    countries_pd = geopandas.read_file(
        "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    )

    (
        countries_pd[["ADMIN", "CONTINENT", "geometry"]]
        .rename(columns={"ADMIN": "name", "CONTINENT": "continent"})
        .to_parquet(
            here / "files" / "natural-earth_countries.parquet", compression=None
        )
    )

    tab = io.read_geoparquet_table(here / "files" / "natural-earth_countries.parquet")
    io.write_geoparquet_table(
        tab,
        here / "files" / "natural-earth_countries_native.parquet",
        compression="none",
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )

    with ipc.new_stream(
        here / "files" / "natural-earth_countries_wkb.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)

    tab_native = tab.set_column(2, "geometry", ga.as_geoarrow(tab["geometry"]))
    with ipc.new_stream(
        here / "files" / "natural-earth_countries.arrows", tab_native.schema
    ) as writer:
        writer.write_table(tab_native)

    tab_native = tab.set_column(
        2,
        "geometry",
        ga.as_geoarrow(tab["geometry"], coord_type=ga.CoordType.INTERLEAVED),
    )
    with ipc.new_stream(
        here / "files" / "natural-earth_countries_interleaved.arrows", tab_native.schema
    ) as writer:
        writer.write_table(tab_native)

    with open(here / "files" / "natural-earth_countries.fgb", "wb") as f:
        write_flatgeobuf(tab_native, f, write_index=False)


def write_countries_geography():
    url = "https://github.com/paleolimbot/duckdb-geography/raw/refs/heads/main/data/countries.tsv"
    with urllib.request.urlopen(url) as f:
        tab = csv.read_csv(f, parse_options=csv.ParseOptions(delimiter="\t"))

    geom = ga.as_wkb(tab["geog"])
    geom = ga.with_crs(geom, ga.OGC_CRS84)
    geom = ga.with_edge_type(geom, ga.EdgeType.SPHERICAL)
    i = tab.schema.get_field_index("geog")
    tab = tab.set_column(i, "geometry", geom)

    io.write_geoparquet_table(
        tab,
        here / "files" / "natural-earth_countries-geography.parquet",
        compression="none",
    )
    io.write_geoparquet_table(
        tab,
        here / "files" / "natural-earth_countries-geography_native.parquet",
        compression="none",
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )

    with ipc.new_stream(
        here / "files" / "natural-earth_countries-geography_wkb.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)

    tab_native = tab.set_column(2, "geometry", ga.as_geoarrow(tab["geometry"]))
    with ipc.new_stream(
        here / "files" / "natural-earth_countries-geography.arrows", tab_native.schema
    ) as writer:
        writer.write_table(tab_native)

    tab_native = tab.set_column(
        2,
        "geometry",
        ga.as_geoarrow(tab["geometry"], coord_type=ga.CoordType.INTERLEAVED),
    )
    with ipc.new_stream(
        here / "files" / "natural-earth_countries-geography_interleaved.arrows",
        tab_native.schema,
    ) as writer:
        writer.write_table(tab_native)


def write_countries_box():
    tab = io.read_geoparquet_table(here / "files" / "natural-earth_countries.parquet")

    # Manually edit a few of the boxes to have max->min behaviour
    boxes = ga.box(tab["geometry"])
    boxes_wkt = ga.as_wkt(boxes).to_pylist()
    py_boxes = boxes.to_pylist()
    for i, name in enumerate(tab["name"].to_pylist()):
        box = py_boxes[i]

        if name == "Fiji":
            py_boxes[i] = new_box(box, 177.28, -179.79)
            boxes_wkt[i] = new_box_wkt(py_boxes[i])
        elif name == "Russia":
            py_boxes[i] = new_box(box, 19.65, -169.89)
            boxes_wkt[i] = new_box_wkt(py_boxes[i])

    tab = tab.set_column(
        2,
        "geometry",
        boxes.type.wrap_array(pa.array(py_boxes, boxes.type.storage_type)),
    )
    with pa.ipc.new_stream(
        here / "files" / "natural-earth_countries-bounds_box.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)

    tab_wkb = tab.set_column(
        2, "geometry", ga.with_crs(ga.as_wkb(boxes_wkt), ga.OGC_CRS84)
    )
    with ipc.new_stream(
        here / "files" / "natural-earth_countries-bounds_wkb.arrows", tab_wkb.schema
    ) as writer:
        writer.write_table(tab_wkb)

    tab_native = tab.set_column(2, "geometry", ga.as_geoarrow(tab_wkb["geometry"]))
    with ipc.new_stream(
        here / "files" / "natural-earth_countries-bounds.arrows", tab_native.schema
    ) as writer:
        writer.write_table(tab_native)

    tab_native = tab.set_column(
        2,
        "geometry",
        ga.as_geoarrow(tab_wkb["geometry"], coord_type=ga.CoordType.INTERLEAVED),
    )
    with ipc.new_stream(
        here / "files" / "natural-earth_countries-bounds_interleaved.arrows",
        tab_native.schema,
    ) as writer:
        writer.write_table(tab_native)

    io.write_geoparquet_table(
        tab_wkb,
        here / "files" / "natural-earth_countries-bounds.parquet",
        compression="none",
    )
    io.write_geoparquet_table(
        tab_wkb,
        here / "files" / "natural-earth_countries-bounds_native.parquet",
        compression="none",
        geometry_encoding=io.geoparquet_encoding_geoarrow(),
    )

    with open(here / "files" / "natural-earth_countries-bounds.fgb", "wb") as f:
        write_flatgeobuf(tab_native, f, write_index=False)


# Tools to rewrite a box manually specifying the west/east bounds
def new_box(box, xwest, xeast):
    return {"xmin": xwest, "ymin": box["ymin"], "xmax": xeast, "ymax": box["ymax"]}


def new_box_wkt(b):
    box_east = {"xmin": b["xmin"], "ymin": b["ymin"], "xmax": 180.0, "ymax": b["ymax"]}
    box_west = {"xmin": -180.0, "ymin": b["ymin"], "xmax": b["xmax"], "ymax": b["ymax"]}
    box_east_storage = pa.StructArray.from_arrays(
        [pa.array([i]) for i in box_east.values()], names=box_east.keys()
    )
    box_east_array = gat.box().to_pyarrow().wrap_array(box_east_storage)
    box_west_storage = pa.StructArray.from_arrays(
        [pa.array([i]) for i in box_west.values()], names=box_west.keys()
    )
    box_west_array = gat.box().to_pyarrow().wrap_array(box_west_storage)
    box_east_wkt = ga.as_wkt(box_east_array)[0].as_py().replace("POLYGON ", "")
    box_west_wkt = ga.as_wkt(box_west_array)[0].as_py().replace("POLYGON ", "")
    return f"MULTIPOLYGON ({box_east_wkt}, {box_west_wkt})"


if __name__ == "__main__":
    # write_cities()
    # write_countries()
    # write_countries_geography()
    write_countries_box()
