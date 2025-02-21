from pathlib import Path
import json

# Currently requires a special branch of pyarrow with extra GeoArrow features
# https://github.com/apache/arrow/compare/main...paleolimbot:arrow:parquet-geo-write-files-from-geoarrow
import pyarrow as pa
from pyarrow import parquet
import geoarrow.pyarrow as ga

here = Path(__file__).parent


def list_wkb_files():
    wkb_files = []
    with open(here / "manifest.json") as f:
        manifest = json.load(f)
        for group in manifest["groups"]:
            for file in group["files"]:
                if file["format"] == "arrows/wkb":
                    name = Path(file["url"]).name
                    local_path = here / group["name"] / "files" / name
                    assert local_path.exists()
                    wkb_files.append(local_path)

    return wkb_files


def convert_arrow_wkb_to_parquet(src, dst, compression):
    # Maintain chunking from IPC into Parquet so that the statistics
    # are theoretically the same.
    with (
        pa.ipc.open_stream(src) as reader,
        parquet.ParquetWriter(
            dst,
            reader.schema,
            store_schema=False,
            compression=compression,
            write_geospatial_logical_types=True,
        ) as writer,
    ):
        print(f"Reading {src}")
        for batch in reader:
            writer.write_batch(batch)
        print(f"Wrote {dst}")


def check_parquet_file(src, dst):
    # Read in original table for comparison
    with pa.ipc.open_stream(src) as reader:
        original_table = reader.read_all()

    print(f"Checking {dst}")
    # with parquet.ParquetFile(dst, arrow_extensions_enabled=False) as f:
    #     print(f.schema)
    #     print(f.metadata.metadata)
    with parquet.ParquetFile(dst, arrow_extensions_enabled=True) as f:
        # print(f.schema)
        # print(f.metadata.metadata)
        if f.schema_arrow != original_table.schema:
            print(f"Schema mismatch:\n{f.schema_arrow}\nvs\n{original_table.schema}")
            return False

        reread = f.read()
        if reread != original_table:
            print("Table mismatch")
            return False

    return True


def generate_parquet_testing_files(wkb_files, parquet_testing_path):
    successful_checks = 0
    written_files = 0
    for path in wkb_files:
        # Skip big files + one CRS example that includes a non-PROJJSON value
        # on purpose (allowed in GeoArrow), which is rightly rejected
        # by Parquet
        name = path.name.replace("_wkb.arrows", "")
        if (
            "microsoft-buildings" in name
            or ("ns-water" in name and name != "ns-water_water-point")
            or "wkt2" in name
        ):
            print(f"Skipping {name}")
            continue

        dst = parquet_testing_path / f"{name}.parquet"
        convert_arrow_wkb_to_parquet(path, dst, compression="none")
        written_files += 1
        successful_checks += check_parquet_file(path, dst)

    if successful_checks != written_files:
        raise ValueError("Some checks failed when generating testing files")


def generate_geoarrow_data_parquet_files(wkb_files):
    successful_checks = 0
    written_files = 0
    for path in wkb_files:
        name = path.name.replace("_wkb.arrows", "")
        if "wkt2" in name:
            print(f"Skipping {name}")
            continue
        if name.startswith("ns-water") or name.startswith("microsoft"):
            compression = "zstd"
        else:
            compression = "none"

        dst = path.parent / f"{name}.parquet"
        convert_arrow_wkb_to_parquet(path, dst, compression=compression)
        written_files += 1
        successful_checks += check_parquet_file(path, dst)

    if successful_checks != written_files:
        raise ValueError("Some checks failed when generating testing files")


if __name__ == "__main__":
    parquet_testing_path = here.parent / "parquet-testing" / "data" / "geospatial"
    wkb_files = list_wkb_files()
    generate_parquet_testing_files(wkb_files, parquet_testing_path)
    generate_geoarrow_data_parquet_files(wkb_files)
