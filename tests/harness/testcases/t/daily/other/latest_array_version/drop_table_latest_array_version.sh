#!/bin/bash
psql -e -d ${DB_NAME} -U ${DB_USER} -p ${DB_PORT} -h ${DB_HOST} -c "drop table latest_array_version"
