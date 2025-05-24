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
    "columns": ["Start_Lat","Start_Lon"],
     "filters": [],
     "groupBy": ["Start_Lat","Start_Lon"],
     "orderBy": ["count(*)"],
     "orderDirection": "DESC",
     "aggregates": [ {"function": "COUNT", "column": "*"}],
     "limit": 10
 }'


echo ' '
