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

#
# Same as remove_versions_01.sh, but use the keep: option.
#

IQUERY="iquery -c $IQUERY_HOST -p $IQUERY_PORT"

# remove arrays and exit with status provided
cleanup()
{
    status=$1
    location=$2
    $IQUERY -naq "remove(foo)"
    if [ $status != 0 ]; then
        echo "Error occured: " $status "at location: " $location
    else
        echo "Success"
    fi
    exit $status
}


# setup the array with four versions and record the UAID
$IQUERY -naq "create array foo <a:int32> [I=0:50000,1000,0]"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
$IQUERY -naq "store(build(foo,1),foo)"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
$IQUERY -naq "store(build(foo,2),foo)"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
$IQUERY -naq "store(build(foo,3),foo)"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
$IQUERY -naq "store(build(foo,4),foo)"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi

uaid=`$IQUERY -ocsv -aq "project(filter(list('arrays'),name='foo'),uaid)"`

# calculate the free and used bytes for instance 0
free=`$IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=0), file_free_bytes)"`
used=`$IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=0), file_bytes)"`
((net = used - free))

# run remove_versions and verify that the correct versions are removed
$IQUERY -naq "remove_versions(foo, keep:2)"
if [[ $? != 0 ]]; then cleanup 1 $LINENO; fi

count1=`$IQUERY -ocsv -aq "aggregate(filter(foo, a = 4), count(a))"`
count2=`$IQUERY -ocsv -aq "aggregate(filter(foo@3, a = 3), count(a))"`
if [[ $count1 != 50001 || $count2 != 50001 ]]; then cleanup 1 $LINENO; fi

$IQUERY -aq "aggregate(filter(foo@2, a = 2), count(a))" 2>&1 |
    grep ARRAY_VERSION_DOESNT_EXIST
if [[ $? != 0 ]]; then cleanup 1 $LINENO; fi
$IQUERY -aq "aggregate(filter(foo@1, a = 1), count(a))" 2>&1 |
    grep ARRAY_VERSION_DOESNT_EXIST
if [[ $? != 0 ]]; then cleanup 1 $LINENO; fi

# verify that the free space increased and the used bytes decreased
free1=`$IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=0), file_free_bytes)"`
used1=`$IQUERY -ocsv -aq "project(filter(list('datastores'), uaid=$uaid and inst=0), file_bytes)"`
((net1 = used1 - free1))

if [[ $net -le $net1 ]]; then echo net $net net1 $net1; cleanup 1 $LINENO; fi

# success
cleanup 0
