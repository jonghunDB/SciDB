#!/bin/bash
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
COUNTER=1
LIMIT=51
INSTANCES_ONLINE_AT_START=`iquery -aq "list('instances')"`
while [ $COUNTER -lt $LIMIT ]; do
    echo
    echo "Iteration #$COUNTER"
    iquery -aq "create array sdb_5989_test_array<v:int64 compression 'bzlib',inj:int64>[i=0:1023,1,0]"
    iquery -aq "store(apply(build(<v : int64> [I=0:1023,1,0], I), inj, injectError(0,23)), sdb_5989_test_array)"
    iquery -aq "remove(sdb_5989_test_array)"
    let COUNTER+=1
done
INSTANCES_ONLINE_AT_END=`iquery -aq "list('instances')"`
if [ "$INSTANCES_ONLINE_AT_START" = "$INSTANCES_ONLINE_AT_END" ]; then
    exit 0
else
    exit 1
fi
