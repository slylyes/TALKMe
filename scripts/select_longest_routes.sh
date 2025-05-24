#! /bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <tableName>"
    exit 1
fi

TABLE_NAME="$1"

curl --noproxy localhost --location --request GET 'http://localhost:8080/distributed/filter' \
--header 'Content-Type: application/json' \
--data '{
    "name": "'"$TABLE_NAME"'",
    "columns": ["Trip_Distance", "Start_Lon", "Start_Lat", "End_Lon", "End_Lat"],
    "filters": [],
    "groupBy": [],
    "orderBy": ["Trip_Distance"],
    "orderDirection":"DESC",
    "aggregates": [],
    "limit": 10
    }'

echo ' '