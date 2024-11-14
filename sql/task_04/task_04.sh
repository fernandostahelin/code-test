#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d target_db -U db_analytics -f /sql/task_04/create_supplier_dim.sql

uv run ./supplier_sync.py
