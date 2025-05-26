#! /bin/bash


if [ -z "$1" ]; then
    echo "Usage: $0 <tableName>"
    exit 1
fi

TABLE_NAME="$1"

JSON=$(curl --silent --noproxy localhost --location --request GET 'http://localhost:8080/api/filter' \
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
         }')

# Utilise Python pour afficher sous forme de tableau
echo "$JSON" | python3 -c '
import sys, json

data = json.load(sys.stdin)

cols = list(data[0].keys())
col_widths = [max(len(str(row.get(col, ""))) for row in data + [{col: col}]) for col in cols]

header = " | ".join(col.ljust(w) for col, w in zip(cols, col_widths))
sep = "-+-".join("-" * w for w in col_widths)

print(header)
print(sep)
for row in data:
    print(" | ".join(str(row.get(col, "")).ljust(w) for col, w in zip(cols, col_widths)))
'
