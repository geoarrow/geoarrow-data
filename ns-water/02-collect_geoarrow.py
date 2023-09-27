import pyarrow as pa
import pyarrow.feather as feather
import geoarrow.pyarrow as ga
import pandas as pd
import pyogrio
import glob
import re

for f in glob.glob("ns-water/*.gpkg"):
    print(f"Processing {f}...")
    geodf = pyogrio.read_dataframe(f).to_crs("EPSG:32620")
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
