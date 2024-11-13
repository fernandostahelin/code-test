#bin/bash

docker exec -it ${POSTGRES_SOURCE_CONTAINER} psql -d northwind -U fernando -f create_table_long_process_data.sql
docker exec -it ${POSTGRES_SOURCE_CONTAINER} psql -d northwind -U fernando -f procedure_01.sql