#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d target_db -U db_analytics -f /sql/task_02/create_audit_table.sql
docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d target_db -U db_analytics -f /sql/task_02/create_target_products.sql
docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d target_db -U db_analytics -f /sql/task_02/create_target_employees.sql

uv sync
uv run task_02.py