#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <tableName> <limit>"
    exit 1
fi

TABLE_NAME="$1"
LIMIT="$2"

# To find the root of the project, then search /data (project's name must be 'TALKMe')
PROJECT_ROOT=$(find ~ -type d -name "TALKMe" -print -quit)

if [ -z "$PROJECT_ROOT" ]; then
    echo "Error : Cannot find TALKMe directory from ~"
    exit 1
fi

# Path to the parquet file from project root
PARQUET_FILE="$PROJECT_ROOT/data/yellow_tripdata_2009-01.parquet"

if [ ! -f "$PARQUET_FILE" ]; then
    echo "Error : Cannot find $PARQUET_FILE "
    exit 1
fi

# URL API
URL="http://localhost:8080/distributed/upload?tableName=$TABLE_NAME&limit=$LIMIT"

# time of upload
echo "Starting upload to '$TABLE_NAME' with limit $LIMIT..."
START=$(date +%s)

curl --noproxy localhost --location "$URL" \
  --header 'Content-Type: application/octet-stream' \
  --data-binary @"$PARQUET_FILE"

END=$(date +%s)
DURATION=$((END - START))
echo -e "\nUpload finished in $DURATION seconds."

