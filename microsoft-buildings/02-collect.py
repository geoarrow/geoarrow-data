
import pyarrow as pa
import pyarrow.parquet as parquet
import geoarrow.pyarrow as ga

zip_codes_from_ogr = parquet.read_table('us-zip-codes/us-zip-codes-ogr.parquet')

crs84_json = None
with open('ogc_crs84.json') as f:
    crs84_json = f.read().strip()

geom_type = ga.wkb().with_crs(crs84_json, ga.CrsType.PROJJSON)
geom = pa.chunked_array(
    [geom_type.wrap_array(a) for a in zip_codes_from_ogr.column('geometry').chunks]
)

geo_meta_json = """
{
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Polygon", "MultiPolygon"]
    }
  },
  "primary_column": "geometry",
  "version": "1.0.0-dev"
}
"""

zip_codes_wkb = pa.table(
    [zip_codes_from_ogr.column('NAME20'), geom],
    schema=pa.schema(
        [
            pa.field('zip_code', pa.utf8()),
            pa.field('geometry', geom_type)
        ]
    ).with_metadata({'geo': geo_meta_json})
)

parquet.write_table(
    zip_codes_wkb,
    'us-zip-codes/us-zip-codes-wkb.parquet',
    compression='ZSTD'
)

geom_type_interleaved = ga.multipolygon() \
    .with_coord_type(ga.CoordType.INTERLEAVED) \
    .with_crs(crs84_json, ga.CrsType.PROJJSON)

geom_interleaved = pa.chunked_array(
    [chunk.as_geoarrow(geom_type_interleaved) for chunk in geom.chunks]
)

geo_meta_json = """
{
  "columns": {
    "geometry": {
      "encoding": "geoarrow.multipolygon",
      "geometry_types": ["MultiPolygon"]
    }
  },
  "primary_column": "geometry",
  "version": "1.0.0-dev"
}
"""

zip_codes_interleaved = pa.table(
    [zip_codes_from_ogr.column('NAME20'), geom_interleaved],
    schema=pa.schema(
        [
            pa.field('zip_code', pa.utf8()),
            pa.field('geometry', geom_type_interleaved)
        ]
    ).with_metadata({'geo': geo_meta_json})
)

parquet.write_table(
    zip_codes_interleaved,
    'us-zip-codes/us-zip-codes-interleaved-multipolygon.parquet',
    compression='ZSTD'
)

geom_type_geoarrow = ga.multipolygon() \
    .with_crs(crs84_json, ga.CrsType.PROJJSON)

geom_geoarrow = pa.chunked_array(
    [chunk.as_geoarrow(geom_type_geoarrow) for chunk in geom.chunks]
)

zip_codes_geoarrow = pa.table(
    [zip_codes_from_ogr.column('NAME20'), geom_geoarrow],
    schema=pa.schema(
        [
            pa.field('zip_code', pa.utf8()),
            pa.field('geometry', geom_type_geoarrow)
        ]
    ).with_metadata({'geo': geo_meta_json})
)

parquet.write_table(
    zip_codes_geoarrow,
    'us-zip-codes/us-zip-codes-multipolygon.parquet',
    compression='ZSTD'
)
