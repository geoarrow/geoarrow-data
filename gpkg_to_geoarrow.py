import pyarrow as pa
import pyarrow.feather as feather
import geoarrow.pyarrow as ga
import pandas as pd
import pyogrio
import glob
import re
import sys

# e.g. collect_geoarrow.py *.gpkg EPSG:32620
pattern = sys.argv[1]
crs = sys.argv[2] if len(sys.argv) >= 3 else None

print(f"Pattern: '{pattern}'; CRS: '{crs}'")

for f in glob.glob(pattern):
    print(f"Processing {f}...")
    geodf = pyogrio.read_dataframe(f)
    if crs:
        geodf = geodf.to_crs(crs)

    geometry = ga.as_geoarrow(geodf.geometry)
    assert geometry.type.coord_type == ga.CoordType.SEPARATE

    df = pd.DataFrame({k: v for k, v in geodf.items() if k != "geometry"})
    table = pa.table(df)

    feather.write_feather(
        table.append_column("geometry", geometry),
        re.sub(".gpkg", ".arrow", f),
        compression="zstd",
    )

    feather.write_feather(
        table.append_column(
            "geometry", ga.with_coord_type(geometry, ga.CoordType.INTERLEAVED)
        ),
        re.sub(".gpkg", "-interleaved.arrow", f),
        compression="zstd",
    )

    feather.write_feather(
        table.append_column("geometry", ga.as_wkb(geometry)),
        re.sub(".gpkg", "-wkb.arrow", f),
        compression="zstd",
    )
