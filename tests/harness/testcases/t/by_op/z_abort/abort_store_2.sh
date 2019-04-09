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

die ()
{
    local SCRIPT=$(basename $0)
    echo 2>&1 "${SCRIPT}: $@"
    exit 1
}

[ "$IQUERY_HOST" != "" ] || die "IQUERY_HOST not defined"
[ "$IQUERY_PORT" != "" ] || die "IQUERY_PORT not defined"

IQUERY="iquery -c $IQUERY_HOST -p $IQUERY_PORT"

# remove arrays and exit with status provided
cleanup()
{
    status=$1
    location=$2
    $IQUERY -naq "remove(fooas2)"
    if [ $status != 0 ]; then
        echo "Error occured: " $status "at line: " $location
    else
        echo "Success"
    fi
    exit $status
}


# create the test array
$IQUERY -naq "create array fooas2 <v:int64> [I=0:2000,100,0]"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi

uaid=`$IQUERY -otsv -aq "project(filter(list('arrays'),name='fooas2'),uaid)"`

# case 1 --- abort the store of the first version of an array.
# Verify that no datastore is created.
${TEST_UTILS_DIR}/killquery.sh -afl 2  2 'store (build (fooas2, I), fooas2)'
if [[ $? != 0 ]]; then cleanup 1 $LINENO; fi

$IQUERY -aq "rename(fooas2, fooas2a)"
$IQUERY -aq "rename(fooas2a, fooas2)"

lines=`$IQUERY -aq "filter(list('datastores'), uaid=$uaid)" | wc -l`
if [[ $lines != 1 ]]; then echo lines = $lines; cleanup 1 $LINENO; fi

# case 2 --- abort the store of the second version of an array.
# Verify that the contents did not change and that the used size of the
# array did not increase
$IQUERY -naq "store (build (fooas2, I), fooas2)"
if [[ $? != 0 ]] ; then cleanup 1 $LINENO; fi
size=$($IQUERY -aq "project(filter(list('datastores'), uaid=$uaid), file_bytes)" |
       awk '{ sum += $2 } END { print sum }')
$IQUERY -aq "project(filter(list('datastores'), uaid=$uaid), file_bytes)"
free=$($IQUERY -aq "project(filter(list('datastores'), uaid=$uaid), file_free_bytes)" |
       awk '{ sum += $2 } END { print sum }')
$IQUERY -aq "project(filter(list('datastores'), uaid=$uaid), file_free_bytes)"
((used = size - free))

${TEST_UTILS_DIR}/killquery.sh -afl 2  2 'store (build (fooas2, I+1), fooas2)'
if [[ $? != 0 ]]; then cleanup 1 $LINENO; fi

lines=`$IQUERY -aq "filter (fooas2, v = I)" | wc -l`
if [[ $lines != 2002 ]]; then echo lines = $lines; cleanup 1 $LINENO; fi

$IQUERY -aq "rename(fooas2, fooas2a)"
$IQUERY -aq "rename(fooas2a, fooas2)"
size1=$($IQUERY -aq "project(filter(list('datastores'), uaid=$uaid), file_bytes)" |
      awk '{ sum += $2 } END { print sum }')
free1=$($IQUERY -aq "project(filter(list('datastores'), uaid=$uaid), file_free_bytes)" |
      awk '{ sum += $2 } END { print sum }')
((used1 = size1 - free1))
if [ $used != $used1 ]; then echo "Uh oh, $used != $used1"; cleanup 1 $LINENO; fi

# success
cleanup 0 $LINENO
