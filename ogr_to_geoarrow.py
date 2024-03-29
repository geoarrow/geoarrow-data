import pyarrow as pa
import pyarrow.feather as feather
import geoarrow.pyarrow as ga
import pyogrio
import pyproj
import glob
import sys
import os

# e.g. gpkg_to_geoarrow.py separate,interleaved,wkb,wkt "*.gpkg"
formats = sys.argv[1].lower().split(",")
pattern = sys.argv[2]
strip_crs = "--strip-crs" in sys.argv
compression = "zstd" if "--compress" in sys.argv else "uncompressed"

for f in glob.glob(pattern):
    print(f"Reading {f}...")

    info, table = pyogrio.raw.read_arrow(f)
    geometry_name = (
        info["geometry_name"] if info["geometry_name"] != "" else "wkb_geometry"
    )
    geometry = ga.as_geoarrow(table.column(geometry_name))
    prj = pyproj.CRS(info["crs"])
    if not strip_crs:
        # Need to fix ga.with_crs() to work with ChunkedArray
        geometry = pa.chunked_array(
            [ga.with_crs(chunk, prj.to_json()) for chunk in geometry.chunks]
        )

    for i, nm in enumerate(table.column_names):
        if nm == geometry_name:
            table = table.remove_column(i)
            break

    assert geometry.type.coord_type == ga.CoordType.SEPARATE
    _, file_extension = os.path.splitext(f)

    if "separate" in formats:
        out = f.replace(file_extension, ".arrow")
        print(f"Writing {out}...")

        feather.write_feather(
            table.append_column("geometry", geometry),
            out,
            compression=compression,
        )

    if "interleaved" in formats:
        out = f.replace(file_extension, "-interleaved.arrow")
        print(f"Writing {out}...")

        feather.write_feather(
            table.append_column(
                "geometry", ga.with_coord_type(geometry, ga.CoordType.INTERLEAVED)
            ),
            out,
            compression=compression,
        )

    if "wkb" in formats:
        out = f.replace(file_extension, "-wkb.arrow")
        print(f"Writing {out}...")

        feather.write_feather(
            table.append_column("geometry", ga.as_wkb(geometry)),
            out,
            compression=compression,
        )

    if "wkt" in formats:
        out = f.replace(file_extension, "-wkt.arrow")
        print(f"Writing {out}...")

        feather.write_feather(
            table.append_column("geometry", ga.as_wkt(geometry)),
            out,
            compression=compression,
        )
