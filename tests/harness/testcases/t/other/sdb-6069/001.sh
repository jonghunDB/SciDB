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

. $TESTDIR/common.sh

COUNTER=0
# while debugging, this failed by about iteration 5,
# run 10 iterations as a verification
while [ $COUNTER -lt 10 ]; do
  let COUNTER+=1
  echo "iteration $COUNTER"
  $TESTDIR/001-queries.sh
  clear_arrays
  restart_scidb
done
