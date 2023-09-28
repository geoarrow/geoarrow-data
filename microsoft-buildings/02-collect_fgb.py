import pyogrio
import glob
import os
import pandas as pd

out_fgb = "microsoft-buildings/microsoft-buildings-point.fgb"
if os.path.exists(out_fgb):
    os.unlink(out_fgb)

for i, f in enumerate(sorted(glob.glob("microsoft-buildings/by_state/*.geojson.zip"))):
    print(f"Reading {f}")
    df = pyogrio.read_dataframe(f"/vsizip/{f}", columns=["geometry"])
    df.insert(0, "row_number", pd.Series(range(len(df))))
    df.insert(0, "src_file", i)
    df.geometry = df.geometry.centroid

    print(f"Writing {out_fgb}")
    pyogrio.write_dataframe(
        df,
        out_fgb,
        layer_options={"SPATIAL_INDEX": "NO"},
        append=os.path.exists(out_fgb),
    )
