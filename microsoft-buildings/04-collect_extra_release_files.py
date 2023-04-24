import pyarrow as pa
import pyarrow.parquet as parquet
import pyarrow.fs as fs
import geoarrow.pyarrow as ga
import glob
import re

local = fs.LocalFileSystem()


def point_to_interleaved(chunked):
    chunks_out = []
    for array in chunked.chunks:
        chunk_out = array.as_geoarrow(
            array.type.with_coord_type(ga.CoordType.INTERLEAVED)
        )
        chunks_out.append(chunk_out)
    return pa.chunked_array(chunks_out)


def wkb_to_geoarrow(chunked):
    chunks_out = []
    for array in chunked.chunks:
        chunk_out = array.as_geoarrow(
            ga.polygon().with_crs(array.type.crs, ga.CrsType.PROJJSON)
        )
        chunks_out.append(chunk_out)
    return pa.chunked_array(chunks_out)


def wkb_to_geoarrow_interleaved(chunked):
    chunks_out = []
    for array in chunked.chunks:
        chunk_out = array.as_geoarrow(
            ga.polygon()
            .with_crs(array.type.crs, ga.CrsType.PROJJSON)
            .with_coord_type(ga.CoordType.INTERLEAVED)
        )
        chunks_out.append(chunk_out)
    return pa.chunked_array(chunks_out)


# Translate points to interleaved format
reader = parquet.ParquetFile(f"microsoft-buildings/microsoft-buildings-point-1.parquet")
schema = reader.schema_arrow
geo_meta = schema.metadata[b"geo"]
reader.close()

geom = reader.schema_arrow.field(2).type
schema_out = schema.set(
    2, pa.field("geometry", geom.with_coord_type(ga.CoordType.INTERLEAVED))
)
schema_out = schema_out.with_metadata({"geo": geo_meta})

for f in glob.glob("microsoft-buildings/microsoft-buildings-point-*.parquet"):
    reader = parquet.ParquetFile(f)

    writer = parquet.ParquetWriter(
        re.sub(
            r"-buildings-point-([0-9]+).parquet",
            r"-buildings-interleaved-point-\1.parquet",
            f,
        ),
        schema_out,
        compression="zstd",
    )

    for j in range(reader.num_row_groups):
        group = reader.read_row_group(j)
        group_out = group.set_column(
            2, "geometry", point_to_interleaved(group.column("geometry"))
        )
        writer.write_table(group_out)

    writer.close()
    reader.close()


# Translate polygons to geoarrow + interleaved format
reader = parquet.ParquetFile(f"microsoft-buildings/microsoft-buildings-wkb-1.parquet")
schema = reader.schema_arrow
geo_meta = schema.metadata[b"geo"]
reader.close()

geom = reader.schema_arrow.field(2).type
schema_out = schema.set(
    2, pa.field("geometry", ga.polygon().with_crs(geom.crs, ga.CrsType.PROJJSON))
)
schema_out_interleaved = schema.set(
    2,
    pa.field(
        "geometry",
        ga.polygon()
        .with_crs(geom.crs, ga.CrsType.PROJJSON)
        .with_coord_type(ga.CoordType.INTERLEAVED),
    ),
)

geo_meta_out = re.sub(
    r'"encoding": "WKB"', r'"encoding": "geoarrow.polygon"', geo_meta.decode("UTF-8")
)
schema_out = schema_out.with_metadata({"geo": geo_meta})
schema_out_interleaved = schema_out_interleaved.with_metadata({"geo": geo_meta_out})

for f in glob.glob("microsoft-buildings/microsoft-buildings-wkb-*.parquet"):
    reader = parquet.ParquetFile(f)

    writer = parquet.ParquetWriter(
        re.sub(r"-buildings-wkb-([0-9]+).parquet", r"-buildings-polygon-\1.parquet", f),
        schema_out,
        compression="zstd",
    )

    writer_interleaved = parquet.ParquetWriter(
        re.sub(
            r"-buildings-wkb-([0-9]+).parquet",
            r"-buildings-interleaved-polygon-\1.parquet",
            f,
        ),
        schema_out_interleaved,
        compression="zstd",
    )

    for j in range(reader.num_row_groups):
        print(f"Reading {f}[{j}]")
        group = reader.read_row_group(j)

        print(f"Writing geoarrow")
        group_out = group.set_column(
            2, "geometry", wkb_to_geoarrow(group.column("geometry"))
        )
        writer.write_table(group_out)

        print(f"Writing interleaved geoarrow")
        group_out = group.set_column(
            2, "geometry", wkb_to_geoarrow_interleaved(group.column("geometry"))
        )
        writer_interleaved.write_table(group_out)

        print(f"Done[{j}]")

    writer.close()
    writer_interleaved.close()
    reader.close()
