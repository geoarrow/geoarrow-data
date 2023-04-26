
for f in $(find example -name '*-wkb.parquet')
do

    f_out=$(echo "${f}" | sed -e 's/-wkb.parquet/.gpkg/')
    ogr2ogr "${f_out}" "${f}"
done
