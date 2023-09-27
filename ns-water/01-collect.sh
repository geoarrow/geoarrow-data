
for f in $(find ns-water/nshn_v2 -name '*.shp')
do
    f_out=$(echo "${f}" | sed -e 's/.shp/.gpkg/' -e 's/nshn_v2_/ns-water-/' -e 's/-ba_/-basin_/' -e 's/-la_/-land_/' -e 's/-wa_/-water_/')
    ogr2ogr "${f_out}" "${f}" -dim XY -nlt PROMOTE_TO_MULTI -t_srs EPSG:32620
    mv "${f_out}" ns-water
done

python gpkg_to_geoarrow.py separate,interleaved,wkb "ns-water/*.gpkg" --compress
