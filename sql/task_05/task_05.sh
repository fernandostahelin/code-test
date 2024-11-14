#!/bin/bash

source ../../.env

# Create the table
docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d ${POSTGRES_DB_ANALYTICS} -U ${POSTGRES_USER_ANALYTICS} -f /sql/task_05/create_imported_supplier.sql

# Export suppliers from source database to CSV
docker exec -it ${POSTGRES_SOURCE_CONTAINER} psql -d ${POSTGRES_DB_SOURCE} -U ${POSTGRES_USER_SOURCE} -c "\COPY (SELECT * FROM suppliers) TO '/tmp/suppliers.csv' WITH CSV HEADER"

# Copy the CSV file from source container to host
docker cp ${POSTGRES_SOURCE_CONTAINER}:/tmp/suppliers.csv ./suppliers.csv

# Run the import script
uv run ./supplier_import.py

# Cleanup
rm suppliers.csv
docker exec -it ${POSTGRES_SOURCE_CONTAINER} rm /tmp/suppliers.csv