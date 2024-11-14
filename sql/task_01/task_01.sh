#!/bin/bash

source ../../.env


docker exec -it ${POSTGRES_SOURCE_CONTAINER} psql -d northwind -U fernando -f /sql/task_01/procedure_01.sql