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
set -e

scratch_path=$TESTDIR/$HPID

consume_cnt=1; while [ $consume_cnt -lt 10 ]; do iquery -aq "consume(gh_secure_COPYNUMBER_MAT)"; let consume_cnt+=1; done &
consume_pid=$!

save_cnt=1; while [ $save_cnt -lt 10 ]; do rm -rf $scratch_path/*; scidb_backup.py --force --save opaque --parallel $scratch_path/backup; let save_cnt+=1; done &
save_pid=$!

# don't need to play nice in the shutdown
wait $consume_pid $save_pid
exit 0
