#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015-2018 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#
#
# Test script for parallel loading of binary data.  This
# script attempts to load data into a bounded array that is
# too small for the size of the data.

source ${TESTDIR}/pload_common_funcs.sh

trap "run_cleanup" 0 # Set up the cleanup callback.

# Set up some size info for the 2D array.
d0_size=${DIM_SZ}
d1_size=${d0_size}
num_cells=$[ ( ${d0_size} * ${d1_size} )]

dummy_chunk_size=$[ ( ${num_cells} / ${NUM_INSTANCES} ) ]


# Setup attributes and binary format strings (ATTRS and ATTR_BIN_TYPES).
setup_array_info set_nulls

# Schema of the array where to load the data.
SCHEMA="<
${ATTRS}
>
[
d_0=0:*,501,0,
d_1=0:*,503,1
]"

# Schema for the "flat" array where the dimensions appear as attributes.
# fewer_cells must be '*' to work with the current version of load() operator
# this changed this test from a failing (negative) test to a positive test
# if and when load starts to use a round-robin distribution for which a fixed size schema
# can work, then we can change fewer_cells back to num_cells-2 and this file can
# again handle the negative case
fewer_cells='*'  #  was $[ ( ${num_cells} - 2 )]
FLAT_SCHEMA="<
d_0:int64,
d_1:int64,
${ATTRS}
>
[
dummy=0:${fewer_cells},${dummy_chunk_size},0
]"

echo "Creating array:"
${IQUERY_CMD} -aq "create array dummy_01_A ${FLAT_SCHEMA}"

# Generate binary data and split it into chunks (files).
${DATA_GEN} --dims-sizes ${d0_size},${d1_size} --seed 1234 --format binary -w 3 --split=${NUM_INSTANCES} --base-name /tmp/${MY_PID}_input "${SCHEMA}"

# Upload split up data files to each instance.
for i in $(seq 0 $((${NUM_INSTANCES} - 1))) ;
do
    # Copy the file to the instance.
    copy_file_to_host ${SERVER_IPS[i]} /tmp/${MY_PID}_input${i} ${DATA_DIRS[i]}/dummy_01_A_input
done

echo "Loading data into array:"

if [ "${DATA_FORMAT}" == "binary" ] ;
then
    load_output=$(${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A,'dummy_01_A_input',-1,'(int64,int64,${ATTR_BIN_TYPES})')" 2>&1 )
else
    load_output=$(${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A,'dummy_01_A_input',-1,'${DATA_FORMAT}')" 2>&1 )
fi

import_failed_message=$( echo $load_output | grep "Error id: scidb::SCIDB_SE_IMPORT_ERROR::SCIDB_LE_FILE_IMPORT_FAILED")

if [ ! -z "${import_failed_message}" ] ;
then
    echo "Success: SCIDB_LE_FILE_IMPORT_FAILED detected"
else
    echo "Failure: load_output = ${load_output}"
fi

echo "Done."
