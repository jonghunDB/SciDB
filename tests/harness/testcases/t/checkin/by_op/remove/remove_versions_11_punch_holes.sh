#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2018 SciDB, Inc.
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

#set -x to debug

#
# Same as remove_versions_01.sh, but with various chunk sizes to attempt to get code coverage
# of the binary buddy, free-list management, and fallocPunchHole functionality
#

IQUERY="iquery -c $IQUERY_HOST -p $IQUERY_PORT"

# remove arrays and exit with status provided
cleanup()
{
    status=$1
    location=$2
    $IQUERY -naq "remove(foo)"
    if [ $status != 0 ]; then
        #echo "debug: Error occured: " $status "at location: " $location 1>&2
        echo "Error occured: " $status "at location: " $location
    else
        #echo "debug Success" 1>&2
        echo "Success"
    fi
    exit $status
}

# find BASE_PATH
BASE_PATH=$($IQUERY -o csv -aq "project(filter(list('instances'), instance_id=0), instance_path)" | head -n1  | sed -e "s/'\(.*\)'/\1/")
#echo debug: BASE_PATH is $BASE_PATH 1>&2

CHUNK_SIZE=4096 # prefer st.st_blksize
FULL="64*$CHUNK_SIZE"

# create and fill array in a manner that is non-RLE-compressible
$IQUERY -naq "store(build(<a:int8>[I=1:${FULL},${CHUNK_SIZE},0],I%512),foo)"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi

# get uaid for datastore lookups
uaid=`$IQUERY -ocsv -aq "project(filter(list('arrays'),name='foo'),uaid)"`
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi

#
# capture file_bytes and file_free_bytes into bash arrays on per-instance basis
#
# 1. find num instances
LAST_INSTANCE=$($IQUERY -o csv+ -aq "list('instances')"  | tail -n1 | cut -f1 -d,)

# collect bytes used and bytes in file but not used
for INST in $(seq 0 $LAST_INSTANCE) ; do
    ALLOC_BYTES_PRE[$INST]=$($IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=$INST), file_bytes)")
    if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
    FREE_BYTES_PRE[$INST]=$($IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=$INST), file_free_bytes)")
    if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
    #echo "debug: ALLOC_BYTES_PRE[$INST] ${ALLOC_BYTES_PRE[${INST}]}" 1>&2
    #echo "debug: FREE_BYTES_PRE[$INST] ${FREE_BYTES_PRE[${INST}]}" 1>&2
    #echo "debug: NET_BYTES_PRE[$INST] ${NET_BYTES_PRE[${INST}]}" 1>&2
done

for MAXI in "48*4096" "40*4096" "16*4096" "3*4096" ; do   # must be monotonically decreasing

    # next version: delete all cells at MAXI and up
    $IQUERY -naq "delete(foo, I >= ${MAXI})"
    if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi

    # remove_versions to eliminate data from datastore
    $IQUERY -naq "remove_versions(foo, keep: 1)"
    if [[ $? != 0 ]]; then cleanup 1 $LINENO; fi

    # collect bytes used and bytes in file but not used
    for INST in $(seq 0 $LAST_INSTANCE) ; do
        ALLOC_BYTES[$INST]=$($IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=$INST), file_bytes)")
        FREE_BYTES[$INST]=$($IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=$INST), file_free_bytes)")
        #echo "debug: ALLOC_BYTES[$INST] ${ALLOC_BYTES[${INST}]}" 1>&2
        #echo "debug: FREE_BYTES[$INST] ${FREE_BYTES[${INST}]}" 1>&2
        #echo "debug: NET_BYTES[$INST] ${NET_BYTES[${INST}]}" 1>&2
    done

    # get file_bytes via du for instance 0 and compare to ALLOC_BYTES[0]
    DU0=$(du -B1 $BASE_PATH/datastores/1_persistent/*.data | cut -f1 )
    #echo "debug: ALLOC_BYTES[0] is ${ALLOC_BYTES[0]} vs DU0 is ${DU0}" 1>&2
    if [[ ${ALLOC_BYTES[0]} -ne $DU0 ]] ; then cleanup 1 $LINENO; fi

    # calculate changes
    for INST in $(seq 0 ${LAST_INSTANCE}) ; do
        # ALLOC_BYTES_DELTA drops (or is zero) due to remove versions
        ((ALLOC_BYTES_DELTA[INST] = ALLOC_BYTES[INST] - ALLOC_BYTES_PRE[INST]))
        #echo "debug: ALLOC_BYTES[${INST}] ${ALLOC_BYTES[$INST]}" 1>&2
        #echo "debug: ALLOC_BYTES_PRE[${INST}] ${ALLOC_BYTES_PRE[$INST]}" 1>&2
        #echo "debug: ALLOC_BYTES_DELTA[${INST}] ${ALLOC_BYTES_DELTA[$INST]}" 1>&2
        if [[ ${ALLOC_BYTES_DELTA[$INST]} -gt 0 ]] ; then
            cleanup 2 $LINENO; # error
        fi

        ((FREE_BYTES_DELTA[INST] = FREE_BYTES[INST] - FREE_BYTES_PRE[INST]))
        #echo "debug: FREE_BYTES[${INST}] ${FREE_BYTES[$INST]}" 1>&2
        #echo "debug: FREE_BYTES_PRE[${INST}] ${FREE_BYTES_PRE[$INST]}" 1>&2
        #echo "debug: FREE_BYTES_DELTA[${INST}] ${FREE_BYTES_DELTA[$INST]}" 1>&2
    done

    # ALLOC_BYTES is net bytes (gross bytes is ALLOC_BYTES + amount returned to FFILE_BYTES
    for INST in $(seq 0 ${LAST_INSTANCE}) ; do
        #echo "debug: ALLOC_BYTES_DELTA[${INST}] ${ALLOC_BYTES_DELTA[$INST]}" 1>&2
        if [[ ${ALLOC_BYTES_DELTA[$INST]} -gt 0 ]] ; then cleanup 1 $LINENO; fi
    done

    ((ALLOC_BYTES_PRE=ALLOC_BYTES))  # for next trip in the loop
    #echo "debug: reaching end of loop" 1>&2
done

cleanup 0

