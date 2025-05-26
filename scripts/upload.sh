#!/bin/bash

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: $0 <tableName> [limit]"
    exit 1
fi

TABLE_NAME="$1"
LIMIT="$2"

# Path to the parquet file from project root
PARQUET_FILE="../data/yellow_tripdata_2009-01.parquet"

if [ ! -f "$PARQUET_FILE" ]; then
    echo "Error: Cannot find $PARQUET_FILE"
    exit 1
fi

# URL API
if [ -z "$LIMIT" ]; then
    URL="http://localhost:8080/api/upload?tableName=$TABLE_NAME"
else
    URL="http://localhost:8080/api/upload?tableName=$TABLE_NAME&limit=$LIMIT"
fi

# time of upload
echo "Starting upload to '$TABLE_NAME'${LIMIT:+ with limit $LIMIT}..."
START=$(date +%s)

curl --noproxy localhost --location "$URL" \
  --header 'Content-Type: application/octet-stream' \
  --data-binary @"$PARQUET_FILE"

END=$(date +%s)
DURATION=$((END - START))
echo -e "\nUpload finished in $DURATION seconds."
