#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2018 SciDB, Inc.
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
#/bin/bash
set -e

. $TESTDIR/common.sh

INSTANCES_ONLINE_AT_START=`iquery -aq "list('instances')"`
iquery -aq "create array bcd <v:uint64 compression 'bzlib'> [i=0:1048576:0:1024]"
iquery -naq "store(build(bcd, null), bcd)"
iquery -naq "limit(scan(bcd), 5)"
iquery -naq "insert(build(<v:uint64 compression 'bzlib'> [i=0:20480:0:1024], i), bcd)"
iquery -naq "show(bcd)"
iquery -naq "versions(bcd)"
iquery -naq "consume(bcd)"
iquery -naq "consume(bcd@1)"
INSTANCES_ONLINE_AT_END=`iquery -aq "list('instances')"`
if [ "$INSTANCES_ONLINE_AT_START" != "$INSTANCES_ONLINE_AT_END" ]; then
    exit 1
fi

restart_scidb

INSTANCES_ONLINE_AT_START=`iquery -aq "list('instances')"`
iquery -naq "consume(bcd)"
iquery -naq "consume(bcd@1)"
iquery -naq "list()"
iquery -naq "insert(build(<v:uint64 compression 'bzlib'> [i=0:20480:0:1024], i), bcd)"
iquery -naq "versions(bcd)"
iquery -naq "insert(build(<v:uint64 compression 'bzlib'> [i=0:20480:0:512], i), bcd)"
iquery -naq "versions(bcd)"
iquery -naq "insert(build(<v:uint64 compression 'bzlib'> [i=0:20480:0:1024], i), bcd)"
iquery -naq "versions(bcd)"
iquery -naq "consume(bcd)"
iquery -naq "consume(bcd)"
iquery -naq "consume(bcd@1)"
INSTANCES_ONLINE_AT_END=`iquery -aq "list('instances')"`
if [ "$INSTANCES_ONLINE_AT_START" != "$INSTANCES_ONLINE_AT_END" ]; then
    exit 1
fi

restart_scidb

INSTANCES_ONLINE_AT_START=`iquery -aq "list('instances')"`
iquery -naq "consume(bcd)"
iquery -naq "consume(bcd@1)"
iquery -naq "consume(bcd@5)"
iquery -naq "versions(bcd)"
iquery -naq "redimension(build(bcd, i), <v:uint64>[i=0:*:0:1])"
iquery -naq "insert(redimension(build(bcd, i), <v:uint64>[i=0:*:0:1024]), bcd)"
iquery -naq "consume(bcd)"
iquery -naq "versions(bcd)"
INSTANCES_ONLINE_AT_END=`iquery -aq "list('instances')"`
if [ "$INSTANCES_ONLINE_AT_START" != "$INSTANCES_ONLINE_AT_END" ]; then
    exit 1
fi

restart_scidb

INSTANCES_ONLINE_AT_START=`iquery -aq "list('instances')"`
iquery -aq "insert(redimension(filter(bcd, i=15000), bcd), bcd)" 2>&1 > /dev/null
iquery -naq "consume(bcd)"
iquery -naq "versions(bcd)"
# the following consume is typically where this test would cause cluster death
iquery -aq "consume(bcd@1)"
INSTANCES_ONLINE_AT_END=`iquery -aq "list('instances')"`
if [ "$INSTANCES_ONLINE_AT_START" != "$INSTANCES_ONLINE_AT_END" ]; then
    exit 1
fi

exit 0
