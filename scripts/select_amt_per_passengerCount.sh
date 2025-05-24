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
    "columns": [
                 "Passenger_Count", "Total_Amt"
             ],
             "filters": [

             ],
             "groupBy": [
                 "Passenger_Count"
             ],
             "aggregates": [
                { "function": "COUNT", "column": "*" },
                { "function": "AVG", "column": "Total_Amt"}
             ],
             "orderBy": ["Passenger_Count"],
             "orderDirection": "ASC"
         }'

echo ' '
