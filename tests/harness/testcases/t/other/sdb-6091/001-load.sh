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

array_file_name="public_gh_secure_COPYNUMBER_MAT.tar.gz"
array_data_path="/public/data/tickets/sdb6088/$array_file_name"
scratch_path=$TESTDIR/$HPID

# create the scratch area for this script, copy the sample data
# into the scratch path, then load the data
rm -rf $scratch_path
mkdir -p $scratch_path
pushd $scratch_path 2>&1 > /dev/null
cp -r $array_data_path .
tar xzf $array_file_name
scidb_backup.py -r opaque $scratch_path/gh_secure_COPYNUMBER_MAT
popd 2>&1 > /dev/null

# demonstrate that the array was loaded successfully
iquery -aq "show(gh_secure_COPYNUMBER_MAT)"
echo "Data loaded successfully"

exit 0
