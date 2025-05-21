#! /bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <tableName>"
    exit 1
fi

TABLE_NAME="$1"

curl --noproxy localhost --location "http://localhost:8080/distributed/table" \
--header 'Content-Type: application/json' \
--data '{
"name": "'"$TABLE_NAME"'",
    "columns":[
        {
            "name": "surcharge",
            "type": "DOUBLE"
        },
        {
            "name": "Total_Amt",
            "type": "DOUBLE"
        },
        {
            "name": "Trip_Dropoff_DateTime",
            "type": "BINARY"
        },
        {
            "name": "Start_Lon",
            "type": "DOUBLE"
        },
        {
            "name": "Fare_Amt",
            "type": "DOUBLE"
        },
        {
            "name": "Tolls_Amt",
            "type": "DOUBLE"
        },
        {
            "name": "Rate_Code",
            "type": "DOUBLE"
        },
        {
            "name": "vendor_name",
            "type": "BINARY"
        },
        {
            "name": "Tip_Amt",
            "type": "DOUBLE"
        },
        {
            "name": "End_Lat",
            "type": "DOUBLE"
        },
        {
            "name": "Payment_Type",
            "type": "BINARY"
        },
        {
            "name": "Start_Lat",
            "type": "DOUBLE"
        },
        {
            "name": "store_and_forward",
            "type": "DOUBLE"
        },
        {
            "name": "Trip_Distance",
            "type": "DOUBLE"
        },
        {
            "name": "End_Lon",
            "type": "DOUBLE"
        },
        {
            "name": "Passenger_Count",
            "type": "INT64"
        },
        {
            "name": "mta_tax",
            "type": "DOUBLE"
        },
        {
            "name": "Trip_Pickup_DateTime",
            "type": "BINARY"
        }
    ]
}'

echo ' '