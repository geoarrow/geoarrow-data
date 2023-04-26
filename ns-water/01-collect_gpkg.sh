
for f in $(find ns-water/nshn_v2 -name '*.shp')
do
    f_out=$(echo "${f}" | sed -e 's/.shp/.gpkg/' -e 's/nshn_v2_/ns-water_/' -e 's/_ba_/_basin_/' -e 's/_la_/_land_/' -e 's/_wa_/_water_/')
    ogr2ogr "${f_out}" "${f}" -nlt PROMOTE_TO_MULTI
    mv "${f_out}" ns-water
done
