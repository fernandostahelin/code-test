#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d target_db -U db_analytics -f /sql/task_03/task_03_schema_ddl.sql