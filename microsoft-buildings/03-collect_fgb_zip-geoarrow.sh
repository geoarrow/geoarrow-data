
pushd microsoft-buildings
# Make a little smaller to maybe fit under the 2 GB limit
ogr2ogr -select "src_file" microsoft-buildings-point.fgb microsoft-buildings-point-bigger.fgb -lco SPATIAL_INDEX=NO
zip -9 microsoft-buildings-point.fgb.zip microsoft-buildings-point.fgb
popd

python ogr_to_geoarrow.py \
    separate,interleaved,wkb \
    "microsoft-buildings/microsoft-buildings-point.fgb" \
    --compress
