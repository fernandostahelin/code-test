#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d ${POSTGRES_DB_ANALYTICS} -U ${POSTGRES_USER_ANALYTICS} -f /sql/task_02/create_audit_table.sql
docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d ${POSTGRES_DB_ANALYTICS} -U ${POSTGRES_USER_ANALYTICS} -f /sql/task_02/create_target_products.sql
docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d ${POSTGRES_DB_ANALYTICS} -U ${POSTGRES_USER_ANALYTICS} -f /sql/task_02/create_target_employees.sql

uv sync
uv run task_02.py