#!/bin/bash

CONTAINER_NAME=pegasus
DBNAME=pegasus

# taxi zone

wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

docker cp taxi+_zone_lookup.csv pegasus:/tmp/taxi+_zone_lookup.csv

docker exec -it $CONTAINER_NAME psql -U postgres -d $DBNAME -c "COPY taxi_zone FROM '/tmp/taxi+_zone_loopup.csv' CSV HEADER DELIMITER ',';"

rm taxi+_zone_lookup.csv

# trip data

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

gunzip green_tripdata_2019-01.csv.gz

docker cp green_tripdata_2019-01.csv pegasus:/tmp/green_tripdata_2019-01.csv

docker exec -it $CONTAINER_NAME psql -U postgres -d $DBNAME -c "COPY green_tripdata FROM '/tmp/green_tripdata_2019-01.csv' CSV HEADER DELIMITER ',';"

rm green_tripdata_2019-01.csv
