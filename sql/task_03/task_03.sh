#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_ANALYTICS_CONTAINER} psql -d ${POSTGRES_DB_ANALYTICS} -U ${POSTGRES_USER_ANALYTICS} -f /sql/task_03/task_03_schema_ddl.sql
