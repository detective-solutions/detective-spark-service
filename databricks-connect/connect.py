# operating url: https://adb-4677518036746641.1.azuredatabricks.net/
# OrgaID: "?o=467751803674664"
# clusterID: "0630-173055-tomes833"
# userToken: "dapi3fa666d3a763bb597a11175431e002e7" - valid till 01.10.2020


"""
docker run -t \
       -e SHARD='https://adb-4677518036746641.1.azuredatabricks.net/' \
                -e CLUSTERID=0630-173055-tomes833 \
                                     -e TOKEN='' \
    dbconnecttest:latest databricks-connect test
"""
