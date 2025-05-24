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
                 "vendor_name",
                 "Total_Amt",
                 "Trip_Distance",
                 "Passenger_Count"
             ],
             "filters": [
                 ["vendor_name", "=", "DDS"]
             ],
             "groupBy": [
                 "vendor_name",
                 "Passenger_Count"
             ],
             "aggregates": [
                 { "function": "SUM", "column": "Total_Amt" },
                 { "function": "COUNT", "column": "Total_Amt" }
             ],
             "orderBy": [
                 "sum_Total_Amt",
                 "count(*)"
             ],
             "orderDirection": "DESC"
         }'

echo ' '


