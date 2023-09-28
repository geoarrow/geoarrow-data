
pushd microsoft-buildings
zip microsoft-buildings-point.fgb.zip microsoft-buildings-point.fgb
popd

python ogr_to_geoarrow.py separate,interleaved,wkb "microsoft-buildings/*.fgb" --compress
