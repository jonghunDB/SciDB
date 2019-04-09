#!/bin/bash
psql -e -d ${DB_NAME} -U ${DB_USER} -p ${DB_PORT} -h ${DB_HOST} -c "update latest_array_version set $1='$2' where namespace_name='$3' and array_name='$4'"
