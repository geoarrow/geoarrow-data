from pathlib import Path
import urllib.request

import geopandas
import geoarrow.pyarrow as ga
from geoarrow.pyarrow import io
from pyarrow import ipc
from pyarrow import csv

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
    with ipc.new_stream(
        here / "files" / "natural-earth_cities_wkb.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)


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
    with ipc.new_stream(
        here / "files" / "natural-earth_countries_wkb.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)


def write_countries_geography():
    url = "https://github.com/paleolimbot/duckdb-geography/raw/refs/heads/main/data/countries.tsv"
    with urllib.request.urlopen(url) as f:
        tab = csv.read_csv(f, parse_options=csv.ParseOptions(delimiter="\t"))

    geom = ga.as_wkb(tab["geog"])
    geom = ga.with_crs(geom, ga.OGC_CRS84)
    geom = ga.with_edge_type(geom, ga.EdgeType.SPHERICAL)
    i = tab.schema.get_field_index("geog")
    tab = tab.set_column(i, "geography", geom)

    io.write_geoparquet_table(
        tab, here / "files" / "natural-earth_countries-geography.parquet"
    )

    with ipc.new_stream(
        here / "files" / "natural-earth_countries-geography_wkb.arrows", tab.schema
    ) as writer:
        writer.write_table(tab)


if __name__ == "__main__":
    write_cities()
    write_countries()
    write_countries_geography()
