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
# script loads binary data where one of the data files is
# contains only nulls.
#

source ${TESTDIR}/pload_common_funcs.sh

trap "run_cleanup" 0 # Set up the cleanup callback.

# Set up some size info for the 2D array.
d0_size=${DIM_SZ}
d1_size=${d0_size}
array_size=$[ ( ${d0_size} * ${d1_size} )]
dummy_chunk=$[ ( ${array_size} / ${NUM_INSTANCES} ) ]

# Setup attributes and binary format strings (ATTRS and ATTR_BIN_TYPES).
setup_array_info all

# Schema of the array where to load the data.
SCHEMA="<
${ATTRS}
>
[
d_0=0:*,501,0,
d_1=0:*,503,1
]"

# Schema for the "flat" array where the dimensions appear as attributes.
FLAT_SCHEMA="<
d_0:int64,
d_1:int64,
${ATTRS}
>
[
dummy=0:${DIM_HI},${dummy_chunk},0
]"

echo "Creating array:"
${IQUERY_CMD} -aq "create array dummy_01_A ${FLAT_SCHEMA}"

# Divide up the first dimension into the number of instances.  This is done
# so that we can generate the data file for each instance separately.
sz0=$[ ( ${d0_size} / ${NUM_INSTANCES} ) ]

# Pick the file index that will be all nulls.
null_index=$[ ( $RANDOM % ${NUM_INSTANCES} ) ]

# Generate the binary data files.
for i in $(seq 0 $[ ( ${NUM_INSTANCES} - 1 ) ] ) ;
do
    # Count the first dimension index offset for each instance.
    offset0=$[ ( ${i} * ${sz0} ) ]
    # Generate fully non-null data.
    null_rate="0"
    if [ "${i}" -eq "${null_index}" ] ;
    then
        # For one of the files generate fully-null data.
        null_rate="1.0"
    fi
    ${DATA_GEN} --dim-offsets ${offset0},0 --dims-sizes ${sz0},${d1_size} --seed 4321 --format binary -w 3 --null-rate ${null_rate} "${SCHEMA}" > /tmp/${MY_PID}_input${i}
done

# Upload split up data files to each instance.
for i in $(seq 0 $((${NUM_INSTANCES} - 1))) ;
do
    # Copy the file to the instance.
    copy_file_to_host ${SERVER_IPS[i]} /tmp/${MY_PID}_input${i} ${DATA_DIRS[i]}/dummy_01_A_input
done

echo "Loading data into array:"
if [ "${DATA_FORMAT}" == "binary" ] ;
then
    ${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A,'dummy_01_A_input',-1,'(int64,int64,${ATTR_BIN_TYPES})')"
else
    ${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A,'dummy_01_A_input',-1,'${DATA_FORMAT}')"
fi

echo "Count non-empty d_0 attributes in loaded array:"
${IQUERY_CMD} -ocsv:l -aq "aggregate(dummy_01_A,count(d_0))"

echo "Count non-empty d_1 attributes in loaded array:"
${IQUERY_CMD} -ocsv:l -aq "aggregate(dummy_01_A,count(d_1))"

echo "Count non-empty a_1 attributes in array after redimension:"
n_a_1=$(${IQUERY_CMD} -ocsv:l -aq "aggregate(redimension(dummy_01_A,${SCHEMA}),count(a_1))" | sed 1d)
expected_n_a_1=$[ ( ${array_size} - ${dummy_chunk} ) ]
if [ "${n_a_1}" -ne "${expected_n_a_1}" ] ;
then
# This test will always fail on an unbounded first dimension because the files will fail to load at all making all the
# counts zero.  This test - and other negative - tests should be rewritten to test for counts other than zero.
    echo "FAIL: count(a_1) = ${n_a_1}, not the expected value!"
else
    echo "PASS: count(a_1) = expected!"
fi

echo "Done."
