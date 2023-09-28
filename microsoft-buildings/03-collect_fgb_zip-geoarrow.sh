
# For now, just generate a few items so that we can spot check our transformations
ogr2ogr microsoft-buildings/microsoft-buildings-interleaved-polygon-6.fgb \
    microsoft-buildings/microsoft-buildings-interleaved-polygon-6.parquet

ogr2ogr microsoft-buildings/microsoft-buildings-interleaved-point-2.fgb \
    microsoft-buildings/microsoft-buildings-interleaved-point-2.parquet
