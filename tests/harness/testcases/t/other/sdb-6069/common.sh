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

restart_scidb () {
    scidb.py stopall $SCIDB_CLUSTER_NAME
    sleep 1
    scidb.py startall $SCIDB_CLUSTER_NAME
    sleep 13
}

list_arrays () {
    for arr in `iquery -aq "list()" | grep -v "{No}" | cut -d" " -f2 | cut -d"," -f1 | cut -d"'" -f2`; do
        echo $arr
    done
}

clear_arrays () {
    for arr in $(list_arrays); do
        iquery -aq "remove($arr)"
    done
}
