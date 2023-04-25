
# Landing page:
# https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2020.html#list-tab-1883739534
curl -L \
    https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip \
    -o us-zip-codes/cb_2020_us_zcta520_500k.zip

unzip us-zip-codes/cb_2020_us_zcta520_500k.zip -d us-zip-codes


rm us-zip-codes/us-zip-codes-wkb.parquet
ogr2ogr \
    -t_srs OGC:CRS84 \
    us-zip-codes/us-zip-codes-ogr.parquet \
    us-zip-codes/cb_2020_us_zcta520_500k.shp
