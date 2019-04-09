#!/bin/bash
observed=$(psql -d ${DB_NAME} -U ${DB_USER} -p ${DB_PORT} -h ${DB_HOST} -c \
                "select namespace_name, array_name as arr_name, array_id as max_arr_id from latest_array_version order by namespace_name, arr_name")
expected=$(psql -d ${DB_NAME} -U ${DB_USER} -p ${DB_PORT} -h ${DB_HOST} -c \
                "select ARR.namespace_name, substring(ARR.array_name,'([^@]+).*') as arr_name, max(ARR.array_id) as max_arr_id from namespace_arrays as ARR group by namespace_name, arr_name order by namespace_name, arr_name")
if [ "$expected" = "$observed" ]; then
    echo "latest_array_version has expected content"
    exit 0
else
    echo "Unexpected result!"
    echo "Expected in latest_array_version (derived from namespace_arrays table):"
    echo "$expected"
    echo "Observed in latest_array_version:"
    echo "$observed"
    exit 1
fi
