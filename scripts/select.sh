#! /bin/bash

curl --noproxy localhost --location 'http://localhost:8080/data/filter' \
--header 'Content-Type: application/json' \
--data '{
    "name": "table",
    "columns": [
        "vendor_name",
        "Passenger_Count",
        "Total_Amt",
        "Trip_Distance"
    ],
    "filters": [
        ["vendor_name","=","DDS"],
        ["Passenger_Count","=","4"]
    ],
    "groupBy": [
        "vendor_name",
        "Passenger_Count"
    ],
    "aggregates": [
        { "function": "SUM", "column": "Total_Amt" },
        { "function": "SUM", "column": "Trip_Distance" },
        { "function": "COUNT", "column": "*" },
        { "function": "AVG", "column": "Total_Amt" }

    ]
}'
