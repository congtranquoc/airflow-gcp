#!/bin/bash

DATABASE="tiki-products"
COLLECTION="products"
FILE="/home/quoccong-workspace/airflow_project/data/tiki_export.json"
TEMP_FILE="/home/quoccong-workspace/airflow_project/data/tiki_export_temp.json"

# Export data from MongoDB collection and remove "_id" field and HTML tags
mongoexport --db "$DATABASE" --collection "$COLLECTION" | \
    sed '/"_id":/s/"_id":[^,]*,//' | \
    sed -E 's/<[^>]*>//g' > "$TEMP_FILE"

# Use jq to ensure valid JSON formatting
jq -c '.' "$TEMP_FILE" > "$FILE"

# Clean up temporary file
rm "$TEMP_FILE"

