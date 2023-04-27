import pyarrow as pa
import pyarrow.parquet as parquet
import geoarrow.pyarrow as ga
import pandas as pd
import pyogrio
import glob
import json
import re

for f in glob.glob('ns-water/*.gpkg'):
    geodf = pyogrio.read_dataframe(f).to_crs('EPSG:32620')
    crs_json = geodf.crs.to_json()
    geom_types = list(sorted(geodf.geometry.geom_type.drop_duplicates()))

    df = pd.DataFrame({k: v for k, v in geodf.items() if k != 'geometry'})
    table = pa.table(df)

    type_out = None
    if 'MultiPoint' in geom_types:
        type_out = ga.multipoint()
    elif 'MultiLineString' in geom_types:
        type_out = ga.multilinestring()
    elif 'MultiPolygon' in geom_types:
        type_out = ga.multipolygon()
    elif 'Point' in geom_types:
        type_out = ga.point()
    elif 'LineString' in geom_types:
        type_out = ga.linestring()
    elif 'Polygon' in geom_types:
        type_out = ga.polygon()
    else:
        raise ValueError('No such geoarrow type')

    type_out = type_out.with_crs(crs_json, ga.CrsType.PROJJSON)
    type_out_interleaved = type_out.with_coord_type(ga.CoordType.INTERLEAVED)
    type_out_wkb = ga.wkb().with_crs(crs_json, ga.CrsType.PROJJSON)

    geom_out_wkb = ga.array(geodf.geometry)
    geom_out = geom_out_wkb.as_geoarrow(type_out)
    geom_out_interleaved = geom_out_wkb.as_geoarrow(type_out_interleaved)

    geo_meta_out_template = """
    {
    "columns": {
        "geometry": {
        "encoding": "ENCODING",
        "crs": CRS_JSON,
        "geometry_types": GEOMETRY_TYPES_JSON
        }
    },
    "primary_column": "geometry",
    "version": "1.0.0-dev"
    }
    """
    geo_meta_out_template = re.sub('CRS_JSON', crs_json, geo_meta_out_template)
    geo_meta_out_template = re.sub('GEOMETRY_TYPES_JSON', json.dumps(geom_types), geo_meta_out_template)

    geo_meta_out = re.sub('ENCODING', type_out.extension_name, geo_meta_out_template)
    geo_meta_out_interleaved = geo_meta_out
    geo_meta_out_wkb = re.sub('ENCODING', 'WKB', geo_meta_out_template)

    schema_out = table.schema.append(
        pa.field('geometry', type_out)
    ).with_metadata({'geo': geo_meta_out})

    schema_out_interleaved = table.schema.append(
        pa.field('geometry', type_out_interleaved)
    ).with_metadata({'geo': geo_meta_out_interleaved})

    schema_out_wkb = table.schema.append(
        pa.field('geometry', type_out_wkb)
    ).with_metadata({'geo': geo_meta_out_wkb})

    table_out = pa.table(
        list(table.columns) + [pa.chunked_array([geom_out])],
        schema=schema_out
    )

    table_out_interleaved = pa.table(
        list(table.columns) + [pa.chunked_array([geom_out_interleaved])],
        schema=schema_out_interleaved
    )

    table_out_wkb = pa.table(
        list(table.columns) + [pa.chunked_array([geom_out_wkb])],
        schema=schema_out_wkb
    )

    parquet.write_table(
        table_out,
        re.sub('.gpkg', '.parquet', f),
        compression='ZSTD'
    )

    parquet.write_table(
        table_out_interleaved,
        re.sub('.gpkg', '-interleaved.parquet', f),
        compression='ZSTD'
    )

    parquet.write_table(
        table_out_wkb,
        re.sub('.gpkg', '-wkb.parquet', f),
        compression='ZSTD'
    )
