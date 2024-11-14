#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_SOURCE_CONTAINER} psql -d ${POSTGRES_DB_SOURCE} -U ${POSTGRES_USER_SOURCE} -f /sql/task_01/procedure_01.sql