#!/bin/bash
psql -e -d ${DB_NAME} -U ${DB_USER} -p ${DB_PORT} -h ${DB_HOST} -c "delete from latest_array_version where namespace_name='$1' and array_name='$2'"
